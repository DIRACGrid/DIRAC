""" GGUSTicketsCommand

  The GGUSTickets_Command class is a command class to know about
  the number of active present opened tickets.

"""
from urllib.error import URLError

from DIRAC import S_ERROR, S_OK
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getGOCSiteName, getGOCSites
from DIRAC.Core.LCG.GGUSTicketsClient import GGUSTicketsClient
from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient
from DIRAC.ResourceStatusSystem.Command.Command import Command


class GGUSTicketsCommand(Command):
    """
    GGUSTickets "master" Command
    """

    def __init__(self, args=None, clients=None):

        super().__init__(args, clients)

        if "GGUSTicketsClient" in self.apis:
            self.gClient = self.apis["GGUSTicketsClient"]
        else:
            self.gClient = GGUSTicketsClient()

        if "ResourceManagementClient" in self.apis:
            self.rmClient = self.apis["ResourceManagementClient"]
        else:
            self.rmClient = ResourceManagementClient()

    def _storeCommand(self, result):
        """
        Stores the results of doNew method on the database.
        """

        for ggus in result:
            resQuery = self.rmClient.addOrModifyGGUSTicketsCache(
                ggus["GocSite"], ggus["Link"], ggus["OpenTickets"], ggus["Tickets"]
            )
            if not resQuery["OK"]:
                return resQuery
        return S_OK()

    def _prepareCommand(self):
        """
        GGUSTicketsCommand requires one arguments:
        - elementName : <str>

        GGUSTickets are associated with gocDB names, so we have to transform the
        diracSiteName into a gocSiteName.
        """

        if "name" not in self.args:
            return S_ERROR('"name" not found in self.args')
        name = self.args["name"]

        return getGOCSiteName(name)

    def doNew(self, masterParams=None):
        """
        Gets the parameters to run, either from the master method or from its
        own arguments.

        For every elementName ( cannot process bulk queries.. ) contacts the
        ggus client. The server is not very stable, so we protect against crashes.

        If there are ggus tickets, are recorded and then returned.
        """

        if masterParams is not None:
            gocName = masterParams
            gocNames = [gocName]

        else:
            gocName = self._prepareCommand()
            if not gocName["OK"]:
                return gocName
            gocName = gocName["Value"]
            gocNames = [gocName]

        try:
            results = self.gClient.getTicketsList(gocName)
        except URLError as e:
            return S_ERROR(f"{gocName} {e}")

        if not results["OK"]:
            return results
        results = results["Value"]

        uniformResult = []

        for gocSite, ggusResult in results.items():

            if gocSite not in gocNames:
                continue

            ggusDict = {}
            ggusDict["GocSite"] = gocSite
            ggusDict["Link"] = ggusResult["URL"]

            del ggusResult["URL"]

            openTickets = 0
            for priorityDict in ggusResult.values():
                openTickets += len(priorityDict)

            ggusDict["Tickets"] = ggusResult
            ggusDict["OpenTickets"] = openTickets

            uniformResult.append(ggusDict)

        storeRes = self._storeCommand(uniformResult)
        if not storeRes["OK"]:
            return storeRes

        return S_OK(uniformResult)

    def doCache(self):
        """
        Method that reads the cache table and tries to read from it. It will
        return a list of dictionaries if there are results.
        """

        gocName = self._prepareCommand()
        if not gocName["OK"]:
            return gocName
        gocName = gocName["Value"]

        result = self.rmClient.selectGGUSTicketsCache(gocSite=gocName)
        if result["OK"]:
            result = S_OK([dict(zip(result["Columns"], res)) for res in result["Value"]])

        return result

    def doMaster(self):
        """
        Master method, which looks little bit spaguetti code, sorry !
        - It gets all gocSites.

        As there is no bulk query, it compares with what we have on the database.
        It queries a portion of them.
        """

        gocSites = getGOCSites()
        if not gocSites["OK"]:
            return gocSites
        gocSites = gocSites["Value"]

        #    resQuery = self.rmClient.selectGGUSTicketsCache( meta = { 'columns' : [ 'GocSite' ] } )
        #    if not resQuery[ 'OK' ]:
        #      return resQuery
        #    resQuery = [ element[0] for element in resQuery[ 'Value' ] ]
        #
        #    gocNamesToQuery = set( gocSites ).difference( set( resQuery ) )

        self.log.info("Processing %s" % ", ".join(gocSites))

        for gocNameToQuery in gocSites:

            #    if gocNameToQuery is None:
            #      self.metrics[ 'failed' ].append( 'None result' )
            #      continue

            result = self.doNew(gocNameToQuery)

            if not result["OK"]:
                self.metrics["failed"].append(result)

        return S_OK(self.metrics)
