""" PilotsHistoryAgent sends the number of pilots retrieved from PilotAgentsDB
    every 15 min to the Monitoring system to create historical plots.
"""

from DIRAC import S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Utilities import Time
from DIRAC.MonitoringSystem.Client.MonitoringReporter import MonitoringReporter
from DIRAC.WorkloadManagementSystem.DB.PilotAgentsDB import PilotAgentsDB


class PilotsHistoryAgent(AgentModule):
    """Agent that every 15 minutes will report a snapshot of
    the PilotAgentsDB to the Monitoring DB (ElasticSearch).
    """

    __summaryKeyFieldsMapping = ["TaskQueueID", "GridSite", "GridType", "Status"]
    __summaryValueFieldsMapping = ["Pilots"]

    def initialize(self):
        # Loop every 15m
        self.am_setOption("PollingTime", 900)
        self.pilotReporter = MonitoringReporter(monitoringType="PilotsHistory")

        return S_OK()

    def execute(self):
        # Retrieve the snapshot of the number of pilots
        result = PilotAgentsDB.getSummarySnapshot()
        now = Time.dateTime()
        if not result["OK"]:
            self.log.error("Can't get the PilotAgentsDB summary", "%s: won't commit at this cycle" % result["Message"])
            return S_ERROR()

        values = result["Value"][1]
        for record in values:
            record = record[1:]
            rD = {}
            for iP in range(len(self.__summaryKeyFieldsMapping)):
                rD[self.__summaryKeyFieldsMapping[iP]] = record[iP]
            record = record[len(self.__summaryKeyFieldsMapping) :]
            for iP in range(len(self.__summaryValueFieldsMapping)):
                rD[self.__summaryValueFieldsMapping[iP]] = int(record[iP])
            rD["timestamp"] = int(Time.toEpoch(now))
            self.log.verbose("Adding following record to Reporter: \n", rD)
            self.pilotReporter.addRecord(rD)

        self.log.info("Committing to Monitoring")
        result = self.pilotReporter.commit()
        if not result["OK"]:
            self.log.error("Could not commit pilots history to Monitoring")
            return S_ERROR()
        self.log.verbose("Done committing to Monitoring")

        return S_OK()
