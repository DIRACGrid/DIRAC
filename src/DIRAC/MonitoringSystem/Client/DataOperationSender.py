from DIRAC import S_OK, S_ERROR, gLogger

from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.AccountingSystem.Client.DataStoreClient import gDataStoreClient
from DIRAC.AccountingSystem.Client.Types.DataOperation import DataOperation
from DIRAC.MonitoringSystem.Client.MonitoringReporter import MonitoringReporter


class DataOperationMonitoringSender(object):
    """
    .. class:: DataOperationMonitoringSender

    This class is used, when called in an instance, to decide whether to send its input
    to Monitoring or Accounting. It then sends it to the decided target.

    """

    def __init__(self, monitoringType="", failoverQueueName="dirac.monitoring"):
        self.monitoringOption = (
            Operations().getValue("DataManagement/MonitoringBackends", "Accounting").replace(" ", "").split(",")
        )

    def sendData(self, baseDict):
        """
        Sends the input to Monitoring or Acconting based on the monitoringOption
        :param dict baseDict: contains a key/value pair
        """
        if "Monitoring" in self.monitoringOption:
            dataOperationReporter = MonitoringReporter(monitoringType="DataOperation")
            dataOperationReporter.addRecord(baseDict)
            result = dataOperationReporter.commit()

            gLogger.verbose("Committing data operation to monitoring")
            if not result["OK"]:
                gLogger.error("Couldn't commit data operation to monitoring", result["Message"])
                return S_ERROR()
            gLogger.verbose("Done committing to monitoring")
            return S_OK()

        if "Accounting" in self.monitoringOption:
            dataOp = DataOperation()
            dataOp.setStartTime()
            dataOp.setValuesFromDict(baseDict)
            dataOp.setEndTime()
            gDataStoreClient.addRegister(dataOp)
            result = gDataStoreClient.commit()
            gLogger.verbose("Committing data operation to accounting")
            if not result["OK"]:
                gLogger.error("Couldn't commit data operation to accounting", result["Message"])
                return S_ERROR()
            gLogger.verbose("Done committing to accounting")
            return S_OK()
