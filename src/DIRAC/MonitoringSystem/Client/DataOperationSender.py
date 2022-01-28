import DIRAC
from DIRAC import S_OK, S_ERROR, gLogger

from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.AccountingSystem.Client.DataStoreClient import gDataStoreClient
from DIRAC.AccountingSystem.Client.Types.DataOperation import DataOperation
from DIRAC.MonitoringSystem.Client.MonitoringReporter import MonitoringReporter


def sendData(baseDict, commitFlag=False, delayedCommit=False, startTime=False, endTime=False):
    """
    Sends the input to Monitoring or Acconting based on the monitoringOption
    :param dict baseDict: contains a key/value pair
    :param bool commitFlag: decides whether to commit the record or not.
    :param bool delayedCommit: decides whether to commit the record with delay (only for sending to Accounting)
    """

    monitoringOption = Operations().getValue("DataManagement/MonitoringBackends", ["Accounting"])
    if "Monitoring" in monitoringOption:
        baseDict["ExecutionSite"] = DIRAC.siteName()
        dataOperationReporter = MonitoringReporter(monitoringType="DataOperation")
        dataOperationReporter.addRecord(baseDict)
        if commitFlag:
            result = dataOperationReporter.commit()
            gLogger.verbose("Committing data operation to monitoring")
            if not result["OK"]:
                gLogger.error("Couldn't commit data operation to monitoring", result["Message"])
                return result
            gLogger.verbose("Done committing to monitoring")

    if "Accounting" in monitoringOption:
        dataOp = DataOperation()
        dataOp.setValuesFromDict(baseDict)
        if startTime:
            dataOp.setStartTime(startTime)
            dataOp.setEndTime(endTime)
        else:
            dataOp.setStartTime()
            dataOp.setEndTime()
        # Adding only to register
        if not commitFlag and not delayedCommit:
            result = gDataStoreClient.addRegister(dataOp)
            return result
        # Adding to register and committing
        if commitFlag and not delayedCommit:
            gDataStoreClient.addRegister(dataOp)
            result = gDataStoreClient.commit()
            gLogger.verbose("Committing data operation to accounting")
            if not result["OK"]:
                gLogger.error("Couldn't commit data operation to accounting", result["Message"])
                return result
            gLogger.verbose("Done committing to accounting")
        # Only late committing
        else:
            result = dataOp.delayedCommit()
            if not result["OK"]:
                gLogger.error("Couldn't delay-commit data operation to accounting")
                return result

    return S_OK()
