import DIRAC
from DIRAC import S_OK, S_ERROR, gLogger

from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.AccountingSystem.Client.DataStoreClient import gDataStoreClient
from DIRAC.AccountingSystem.Client.Types.DataOperation import DataOperation
from DIRAC.MonitoringSystem.Client.MonitoringReporter import MonitoringReporter


class DataOperationSender:

    # Initialize the object so that the Reporters are created only once
    def __init__(self):
        self.monitoringOption = Operations().getValue("DataManagement/MonitoringBackends", ["Accounting"])
        if "Monitoring" in self.monitoringOption:
            self.dataOperationReporter = MonitoringReporter(monitoringType="DataOperation")
        if "Accounting" in self.monitoringOption:
            self.dataOp = DataOperation()

    def sendData(self, baseDict, commitFlag=False, delayedCommit=False, startTime=False, endTime=False):
        """
        Sends the input to Monitoring or Acconting based on the monitoringOption

        :param dict baseDict: contains a key/value pair
        :param bool commitFlag: decides whether to commit the record or not.
        :param bool delayedCommit: decides whether to commit the record with delay (only for sending to Accounting)
        """
        if "Monitoring" in self.monitoringOption:
            baseDict["ExecutionSite"] = DIRAC.siteName()
            self.dataOperationReporter.addRecord(baseDict)
            if commitFlag:
                result = self.dataOperationReporter.commit()
                gLogger.verbose("Committing data operation to monitoring")
                if not result["OK"]:
                    gLogger.error("Couldn't commit data operation to monitoring", result["Message"])
                    return result
                gLogger.verbose("Done committing to monitoring")

        if "Accounting" in self.monitoringOption:
            self.dataOp.setValuesFromDict(baseDict)
            if startTime:
                self.dataOp.setStartTime(startTime)
                self.dataOp.setEndTime(endTime)
            else:
                self.dataOp.setStartTime()
                self.dataOp.setEndTime()
            # Adding only to register
            if not commitFlag and not delayedCommit:
                result = gDataStoreClient.addRegister(self.dataOp)
                return result
            # Adding to register and committing
            if commitFlag and not delayedCommit:
                gDataStoreClient.addRegister(self.dataOp)
                result = gDataStoreClient.commit()
                gLogger.verbose("Committing data operation to accounting")
                if not result["OK"]:
                    gLogger.error("Couldn't commit data operation to accounting", result["Message"])
                    return result
                gLogger.verbose("Done committing to accounting")
            # Only late committing
            else:
                result = self.dataOp.delayedCommit()
                if not result["OK"]:
                    gLogger.error("Couldn't delay-commit data operation to accounting")
                    return result

        return S_OK()

    # Call this method in order to commit all records added but not yet committed to Accounting
    def concludeSending(self):
        if "Accounting" in self.monitoringOption:
            result = gDataStoreClient.commit()
            gLogger.verbose("Concluding the sending and committing data operation to accounting")
            if not result["OK"]:
                gLogger.error("Couldn't commit data operation to accounting", result["Message"])
                return result
        gLogger.verbose("Done committing to accounting")
        return S_OK()
