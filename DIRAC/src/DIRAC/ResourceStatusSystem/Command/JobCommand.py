""" JobCommand

  The JobCommand class is a command class to know about present jobs efficiency

"""
from DIRAC import S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getSites
from DIRAC.ResourceStatusSystem.Command.Command import Command
from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient
from DIRAC.WorkloadManagementSystem.Client.WMSAdministratorClient import WMSAdministratorClient


class JobCommand(Command):
    """
    Job "master" Command.
    """

    def __init__(self, args=None, clients=None):
        super().__init__(args, clients)

        if "WMSAdministrator" in self.apis:
            self.wmsAdmin = self.apis["WMSAdministrator"]
        else:
            self.wmsAdmin = WMSAdministratorClient()

        if "ResourceManagementClient" in self.apis:
            self.rmClient = self.apis["ResourceManagementClient"]
        else:
            self.rmClient = ResourceManagementClient()

    def _storeCommand(self, result):
        """
        Stores the results of doNew method on the database.
        """

        for jobDict in result:
            resQuery = self.rmClient.addOrModifyJobCache(
                jobDict["Site"], jobDict["MaskStatus"], jobDict["Efficiency"], jobDict["Status"]
            )
            if not resQuery["OK"]:
                return resQuery
        return S_OK()

    def _prepareCommand(self):
        """
        JobCommand requires one arguments:
        - name : <str>
        """

        if "name" not in self.args:
            return S_ERROR('"name" not found in self.args')
        name = self.args["name"]

        return S_OK(name)

    def doNew(self, masterParams=None):
        """
        Gets the parameters to run, either from the master method or from its
        own arguments.

        It contacts the WMSAdministrator with a list of site names, or a single
        site.

        If there are jobs, are recorded and then returned.
        """

        if masterParams is not None:
            name = masterParams
        else:
            params = self._prepareCommand()
            if not params["OK"]:
                return params
            name = params["Value"]

        # selectDict, sortList, startItem, maxItems
        # Returns statistics of Last day !
        results = self.wmsAdmin.getSiteSummaryWeb({"Site": name}, [], 0, 0)
        if not results["OK"]:
            return results
        results = results["Value"]

        if "ParameterNames" not in results:
            return S_ERROR('Wrong result dictionary, missing "ParameterNames"')
        params = results["ParameterNames"]

        if "Records" not in results:
            return S_ERROR('Wrong formed result dictionary, missing "Records"')
        records = results["Records"]

        uniformResult = []

        for record in records:
            # This returns a dictionary with the following keys
            # 'Site', 'GridType', 'Country', 'Tier', 'MaskStatus', 'Received',
            # 'Checking', 'Staging', 'Waiting', 'Matched', 'Running', 'Stalled',
            # 'Done', 'Completed', 'Failed', 'Efficiency', 'Status'
            jobDict = dict(zip(params, record))

            # We cast efficiency to a float
            jobDict["Efficiency"] = float(jobDict["Efficiency"])

            uniformResult.append(jobDict)

        storeRes = self._storeCommand(uniformResult)
        if not storeRes["OK"]:
            return storeRes

        return S_OK(uniformResult)

    def doCache(self):
        """
        Method that reads the cache table and tries to read from it. It will
        return a list of dictionaries if there are results.
        """

        params = self._prepareCommand()
        if not params["OK"]:
            return params
        name = params["Value"]

        result = self.rmClient.selectJobCache(name)
        if result["OK"]:
            result = S_OK([dict(zip(result["Columns"], res)) for res in result["Value"]])

        return result

    def doMaster(self):
        """
        Master method.

        Gets all sites and calls doNew method.
        """

        siteNames = getSites()
        if not siteNames["OK"]:
            return siteNames
        siteNames = siteNames["Value"]

        jobsResults = self.doNew(siteNames)
        if not jobsResults["OK"]:
            self.metrics["failed"].append(jobsResults["Message"])

        return S_OK(self.metrics)


class JobsWMSCommand(Command):
    def __init__(self, args=None, clients=None):
        super().__init__(args, clients)

        if "WMSAdministrator" in self.apis:
            self.wmsAdmin = self.apis["WMSAdministrator"]
        else:
            self.wmsAdmin = WMSAdministratorClient()

    def doCommand(self):
        """
        Returns simple jobs efficiency

        :param args:
           - args[0]: string: should be a ValidElement

           - args[1]: string should be the name of the ValidElement

        :returns: { 'Result': 'Good'|'Fair'|'Poor'|'Idle'|'Bad' }
        """

        if "siteName" not in self.args:
            return self.returnERROR(S_ERROR("siteName is missing"))
        siteName = self.args["siteName"]

        # If siteName is None, we take all sites
        if siteName is None:
            siteName = getSites()
            if not siteName["OK"]:
                return self.returnERROR(siteName)
            siteName = siteName["Value"]

        results = self.wmsAdmin.getSiteSummaryWeb({"Site": siteName}, [], 0, 500)

        if not results["OK"]:
            return self.returnERROR(results)
        results = results["Value"]

        if "ParameterNames" not in results:
            return self.returnERROR(S_ERROR("Malformed result dictionary"))
        params = results["ParameterNames"]

        if "Records" not in results:
            return self.returnERROR(S_ERROR("Malformed result dictionary"))
        records = results["Records"]

        jobResults = []

        for record in records:
            jobDict = dict(zip(params, record))
            try:
                jobDict["Efficiency"] = float(jobDict["Efficiency"])
            except KeyError as e:
                return self.returnERROR(S_ERROR(e))
            except ValueError as e:
                return self.returnERROR(S_ERROR(e))

            jobResults.append(jobDict)

        return S_OK(jobResults)
