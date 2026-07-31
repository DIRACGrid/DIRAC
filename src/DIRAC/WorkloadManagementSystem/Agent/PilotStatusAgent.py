"""The Pilot Status Agent updates the status of the pilot jobs in the
     PilotAgents database.

.. literalinclude:: ../ConfigTemplate.cfg
  :start-after: ##BEGIN PilotStatusAgent
  :end-before: ##END
  :dedent: 2
  :caption: PilotStatusAgent options
"""


from DIRAC import S_OK
from DIRAC.AccountingSystem.Client.DataStoreClient import gDataStoreClient
from DIRAC.AccountingSystem.Client.Types.Pilot import Pilot as PilotAccounting
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getCESiteMapping
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Utilities import TimeUtilities
from DIRAC.Core.Utilities.TimeUtilities import DiracTime
from DIRAC.WorkloadManagementSystem.Client import PilotStatus
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
from DIRAC.WorkloadManagementSystem.DB.PilotAgentsDB import PilotAgentsDB
from DIRAC.WorkloadManagementSystem.Service.WMSUtilities import killPilotsInQueues
from DIRAC.WorkloadManagementSystem.Utilities.QueueUtilities import QueueCECache


class PilotStatusAgent(AgentModule):
    """
    The specific agents must provide the following methods:
      - initialize() for initial settings
      - beginExecution()
      - execute() - the main method called in the agent cycle
      - endExecution()
      - finalize() - the graceful exit of the method, this one is usually used
                 for the agent restart
    """

    def __init__(self, *args, **kwargs):
        """c'tor"""
        super().__init__(*args, **kwargs)

        self.jobDB = None
        self.pilotDB = None
        # Cache of ComputingElement instances keyed by queue.
        self.ceCache = None

    #############################################################################
    def initialize(self):
        """Sets defaults"""

        self.pilotDB = PilotAgentsDB()
        self.ceCache = QueueCECache()
        self.jobDB = JobDB()
        self.clearPilotsDelay = self.am_getOption("ClearPilotsDelay", 30)
        self.clearAbortedDelay = self.am_getOption("ClearAbortedPilotsDelay", 7)

        return S_OK()

    #############################################################################
    def execute(self):
        """The PilotAgent execution method."""

        self.pilotStalledDays = self.am_getOption("PilotStalledDays", 3)

        result = self.pilotDB._getConnection()
        if not result["OK"]:
            return result
        connection = result["Value"]

        # Now handle pilots not updated in the last N days and declare them Deleted.
        result = self.handleOldPilots(connection)

        result = self.pilotDB.clearPilots(self.clearPilotsDelay, self.clearAbortedDelay)
        if not result["OK"]:
            self.log.warn("Failed to clear old pilots in the PilotAgentsDB")

        return S_OK()

    def handleOldPilots(self, connection):
        """
        select all pilots that have not been updated in the last N days and declared them
        Deleted, accounting for them.
        """
        pilotsToAccount = {}
        timeLimitToConsider = TimeUtilities.toString(DiracTime.utcnow() - TimeUtilities.day * self.pilotStalledDays)
        result = self.pilotDB.selectPilots(
            {"Status": PilotStatus.PILOT_TRANSIENT_STATES}, older=timeLimitToConsider, timeStamp="LastUpdateTime"
        )
        if not result["OK"]:
            self.log.error("Failed to get the Pilot Agents")
            return result
        if not result["Value"]:
            return S_OK()

        refList = result["Value"]
        result = self.pilotDB.getPilotInfo(refList)
        if not result["OK"]:
            self.log.error("Failed to get Info for Pilot Agents")
            return result

        pilotsDict = result["Value"]

        for pRef in pilotsDict:
            if pilotsDict[pRef].get("Jobs") and self._checkJobLastUpdateTime(
                pilotsDict[pRef]["Jobs"], self.pilotStalledDays
            ):
                self.log.debug(
                    "%s should not be deleted since one job of %s is running."
                    % (str(pRef), str(pilotsDict[pRef]["Jobs"]))
                )
                continue
            deletedJobDict = pilotsDict[pRef]
            deletedJobDict["Status"] = PilotStatus.DELETED
            deletedJobDict["StatusDate"] = DiracTime.utcnow()
            pilotsToAccount[pRef] = deletedJobDict
            if len(pilotsToAccount) > 100:
                self.accountPilots(pilotsToAccount, connection)
                self._killPilots(pilotsToAccount)
                pilotsToAccount = {}

        self.accountPilots(pilotsToAccount, connection)
        self._killPilots(pilotsToAccount)

        return S_OK()

    def accountPilots(self, pilotsToAccount, connection):
        """account for pilots"""
        accountingFlag = False
        pae = self.am_getOption("PilotAccountingEnabled", "yes")
        if pae.lower() == "yes":
            accountingFlag = True

        if not pilotsToAccount:
            self.log.info("No pilots to Account")
            return S_OK()

        accountingSent = False
        if accountingFlag:
            retVal = self.pilotDB.getPilotInfo(list(pilotsToAccount), conn=connection)
            if not retVal["OK"]:
                self.log.error("Fail to retrieve Info for pilots", retVal["Message"])
                return retVal
            dbData = retVal["Value"]
            for pref in dbData:
                if pref in pilotsToAccount:
                    if dbData[pref]["Status"] not in PilotStatus.PILOT_FINAL_STATES:
                        dbData[pref]["Status"] = pilotsToAccount[pref]["Status"]
                        dbData[pref]["DestinationSite"] = pilotsToAccount[pref]["DestinationSite"]
                        dbData[pref]["LastUpdateTime"] = pilotsToAccount[pref]["StatusDate"]

            retVal = self._addPilotsAccountingReport(dbData)
            if not retVal["OK"]:
                self.log.error("Fail to retrieve Info for pilots", retVal["Message"])
                return retVal

            self.log.info("Sending accounting records...")
            retVal = gDataStoreClient.commit()
            if not retVal["OK"]:
                self.log.error("Can't send accounting reports", retVal["Message"])
            else:
                self.log.info(f"Accounting sent for {len(pilotsToAccount)} pilots")
                accountingSent = True

        if not accountingFlag or accountingSent:
            for pRef in pilotsToAccount:
                pDict = pilotsToAccount[pRef]
                self.log.verbose(f"Setting Status for {pRef} to {pDict['Status']}")
                self.pilotDB.setPilotStatus(
                    pRef, pDict["Status"], pDict["DestinationSite"], pDict["StatusDate"], conn=connection
                )

        return S_OK()

    def _addPilotsAccountingReport(self, pilotsData):
        """fill accounting data"""
        for pRef in pilotsData:
            pData = pilotsData[pRef]
            pA = PilotAccounting()
            pA.setEndTime(pData["LastUpdateTime"])
            pA.setStartTime(pData["SubmissionTime"])
            pA.setValueByKey("User", "unknown")
            pA.setValueByKey("UserGroup", pData.get("VO", pData.get("OwnerGroup")))
            result = getCESiteMapping(pData["DestinationSite"])
            if result["OK"] and pData["DestinationSite"] in result["Value"]:
                pA.setValueByKey("Site", result["Value"][pData["DestinationSite"]].strip())
            else:
                pA.setValueByKey("Site", "Unknown")
            pA.setValueByKey("GridCE", pData["DestinationSite"])
            pA.setValueByKey("GridMiddleware", pData["GridType"])
            pA.setValueByKey("GridResourceBroker", "DIRAC")
            pA.setValueByKey("GridStatus", pData["Status"])
            if "Jobs" not in pData:
                pA.setValueByKey("Jobs", 0)
            else:
                pA.setValueByKey("Jobs", len(pData["Jobs"]))
            self.log.verbose(f"Added accounting record for pilot {pData['PilotID']}")
            retVal = gDataStoreClient.addRegister(pA)
            if not retVal["OK"]:
                return retVal
        return S_OK()

    def _killPilots(self, acc):
        """Declare the given pilots killed on their CEs.

        Pilots are grouped per queue and killed with a single call per queue,
        reusing a cached CE/connection per queue across cycles (see
        :func:`~DIRAC.WorkloadManagementSystem.Service.WMSUtilities.killPilotsInQueues`).
        """
        # Group the pilots to kill per queue
        pilotsByQueue = {}
        for pRef in acc:
            pilotDict = acc[pRef]
            queueFields = [pilotDict["VO"], pilotDict["GridSite"], pilotDict["DestinationSite"], pilotDict["Queue"]]
            # A pilot with an incomplete queue definition cannot be located on a
            # CE; skip it rather than letting it abort the whole batch.
            if not all(queueFields):
                self.log.warn("Cannot determine queue for pilot, skipping kill", f"{pRef} : {queueFields}")
                continue
            queueKey = "@@@".join(queueFields)
            queueData = pilotsByQueue.setdefault(queueKey, {"GridType": pilotDict["GridType"], "PilotList": []})
            queueData["PilotList"].append(pRef)

        if not pilotsByQueue:
            return

        # The CEs (and their connections) are reused across cycles via self.ceCache.
        result = killPilotsInQueues(pilotsByQueue, ceCache=self.ceCache)
        if not result["OK"]:
            self.log.error("Failed to kill some pilots", result["Message"])

    def _checkJobLastUpdateTime(self, joblist, StalledDays):
        timeLimitToConsider = DiracTime.utcnow() - TimeUtilities.day * StalledDays
        ret = False
        for jobID in joblist:
            result = self.jobDB.getJobAttributes(int(jobID))
            if result["OK"]:
                if "LastUpdateTime" in result["Value"]:
                    lastUpdateTime = result["Value"]["LastUpdateTime"]
                    if TimeUtilities.fromString(lastUpdateTime) > timeLimitToConsider:
                        ret = True
                        self.log.debug(
                            "Since %s updates LastUpdateTime on %s this does not to need to be deleted."
                            % (str(jobID), str(lastUpdateTime))
                        )
                        break
            else:
                self.log.error("Error taking job info from DB", result["Message"])
        return ret
