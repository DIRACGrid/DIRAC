""" PilotCommand

  The PilotCommand class is a command class to know about present pilots
  efficiency.

"""
from DIRAC import S_ERROR, S_OK
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getCESiteMapping, getSites
from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient
from DIRAC.ResourceStatusSystem.Command.Command import Command
from DIRAC.WorkloadManagementSystem.Client.PilotManagerClient import PilotManagerClient


class PilotCommand(Command):
    """
    Pilot "master" Command.
    """

    def __init__(self, args=None, clients=None):
        super().__init__(args, clients)

        if "Pilots" in self.apis:
            self.pilots = self.apis["Pilots"]
        else:
            self.pilots = PilotManagerClient()

        if "ResourceManagementClient" in self.apis:
            self.rmClient = self.apis["ResourceManagementClient"]
        else:
            self.rmClient = ResourceManagementClient()

    def _storeCommand(self, result):
        """
        Stores the results of doNew method on the database.
        """

        for pilotDict in result:
            resQuery = self.rmClient.addOrModifyPilotCache(
                site=pilotDict["Site"],
                cE=pilotDict["CE"],
                vO=pilotDict.get("OwnerGroup", None),
                pilotsPerJob=pilotDict["PilotsPerJob"],
                pilotJobEff=pilotDict["PilotJobEff"],
                status=pilotDict["Status"],
            )
            if not resQuery["OK"]:
                return resQuery

        return S_OK()

    def _prepareCommand(self):
        """
        JobCommand requires one arguments:
        - name : <str>
        """

        self.log.debug("_prepareCommand: args:", self.args)
        if "name" not in self.args:
            return S_ERROR('"name" not found in self.args')
        name = self.args["name"]

        if "element" not in self.args:
            return S_ERROR("element is missing")
        element = self.args["element"]

        if "vO" not in self.args:
            return S_ERROR('_prepareCommand: "vO" not found in self.args')
        vo = self.args["vO"]

        if element not in ["Site", "Resource"]:
            return S_ERROR(f'"{element}" is not Site nor Resource')

        return S_OK((element, name, vo))

    def doNew(self, masterParams=None):
        self.log.debug("PilotCommand doNew")
        if masterParams is not None:
            element, name = masterParams
        else:
            params = self._prepareCommand()
            if not params["OK"]:
                return params
            element, name = params["Value"]

        wmsDict = {}

        if element == "Site":
            wmsDict = {"GridSite": name}
        elif element == "Resource":
            wmsDict = {"ExpandSite": name}
        else:
            # You should never see this error
            return S_ERROR(f'"{element}" is not  Site nor Resource')

        if element == "Resource":
            pilotsResultPivot = self.pilots.getGroupedPilotSummary(["GridSite", "DestinationSite", "VO"])
        elif element == "Site":
            pilotsResultPivot = self.pilots.getGroupedPilotSummary(["GridSite", "VO"])
        else:
            # You should never see this error
            return S_ERROR(f'"{element}" is not  Site nor Resource')

        if not pilotsResultPivot["OK"]:
            return pilotsResultPivot
        pilotsResults = pilotsResultPivot["Value"]

        if "ParameterNames" not in pilotsResults:
            return S_ERROR('Wrong result dictionary, missing "ParameterNames"')
        params = pilotsResults["ParameterNames"]

        if "Records" not in pilotsResults:
            return S_ERROR('Wrong formed result dictionary, missing "Records"')
        records = pilotsResults["Records"]

        uniformResult = []

        for record in records:
            # This returns a dictionary with the following keys:
            # 'Site', 'CE', 'Submitted', 'Ready', 'Scheduled', 'Waiting', 'Running',
            # 'Done', 'Aborted', 'Done_Empty', 'Aborted_Hour', 'Total', 'PilotsPerJob',
            # 'PilotJobEff', 'Status', 'InMask'
            pilotDict = dict(zip(params, record))

            pilotDict["PilotsPerJob"] = float(pilotDict["PilotsPerJob"])
            pilotDict["PilotJobEff"] = float(pilotDict["PilotJobEff"])

            uniformResult.append(pilotDict)

        storeRes = self._storeCommand(uniformResult)
        if not storeRes["OK"]:
            return storeRes

        return S_OK(uniformResult)

    def doCache(self):
        self.log.debug("PilotCommand doCache")
        params = self._prepareCommand()
        if not params["OK"]:
            return params
        element, name, vo = params["Value"]

        if element == "Site":
            # WMS returns Site entries with CE = 'Multiple'
            site, ce = name, "Multiple"
        elif element == "Resource":
            site, ce = None, name
        else:
            # You should never see this error
            return S_ERROR(f'"{element}" is not  Site nor Resource')

        result = self.rmClient.selectPilotCache(site=site, cE=ce)
        if result["OK"]:
            result = S_OK([dict(zip(result["Columns"], res)) for res in result["Value"]])
        self.log.debug("PilotCommand doCache result: ", result)
        return result

    def doMaster(self):
        self.log.debug("PilotCommand doMaster")
        siteNames = getSites()
        if not siteNames["OK"]:
            return siteNames
        siteNames = siteNames["Value"]

        res = getCESiteMapping()
        if not res["OK"]:
            return res
        ces = list(res["Value"])

        pilotResults = self.doNew(("Site", siteNames))
        if not pilotResults["OK"]:
            self.metrics["failed"].append(pilotResults["Message"])

        pilotResults = self.doNew(("Resource", ces))
        if not pilotResults["OK"]:
            self.metrics["failed"].append(pilotResults["Message"])

        return S_OK(self.metrics)
