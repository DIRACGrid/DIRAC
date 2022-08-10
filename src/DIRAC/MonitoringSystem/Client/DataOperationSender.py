"""
This class is being called whenever there is need to send data operation to Accounting or Monitoring, or both.
Created as replacement, or rather semplification, of the MonitoringReporter/gDataStoreClient usage for data operation to handle both cases.

"""

import copy
import DIRAC
from DIRAC import S_OK, gLogger

from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Utilities.ReturnValues import convertToReturnValue, returnValueOrRaise
from DIRAC.Core.Utilities.TimeUtilities import toEpochMilliSeconds
from DIRAC.AccountingSystem.Client.DataStoreClient import gDataStoreClient
from DIRAC.AccountingSystem.Client.Types.DataOperation import DataOperation
from DIRAC.MonitoringSystem.Client.MonitoringReporter import MonitoringReporter

sLog = gLogger.getSubLogger(__name__)


class DataOperationSender:
    """
    class:: DataOperationSender
    It reads the MonitoringBackends option to decide whether send and commit data operation to either Accounting or Monitoring.
    """

    # Initialize the object so that the Reporters are created only once
    def __init__(self):
        monitoringType = "DataOperation"
        # Will use the `MonitoringBackends/Default` value
        # as monitoring backend unless a flag for `MonitoringBackends/DataOperation` is set.
        self.monitoringOptions = Operations().getMonitoringBackends(monitoringType)
        if "Monitoring" in self.monitoringOptions:
            self.dataOperationReporter = MonitoringReporter(monitoringType)
        if "Accounting" in self.monitoringOptions:
            self.dataOp = DataOperation()

        self._sendDataMethods = []
        self._commitMethods = []
        for backend in self.monitoringOptions:
            self._sendDataMethods.append(getattr(self, f"_sendData{backend}"))
            self._commitMethods.append(getattr(self, f"_commit{backend}"))

    def _sendDataMonitoring(self, baseDict, commitFlag=False, delayedCommit=False, startTime=False, endTime=False):
        """Send the data to the monitoring system"""
        # Some fields added here are not known to the Accounting, so we have to make a copy
        monitoringDict = copy.deepcopy(baseDict)

        monitoringDict["Channel"] = monitoringDict["Source"] + "->" + monitoringDict["Destination"]
        # Add timestamp if not already added
        if "timestamp" not in monitoringDict:
            monitoringDict["timestamp"] = int(toEpochMilliSeconds())
        self.dataOperationReporter.addRecord(monitoringDict)
        if commitFlag:
            result = self.dataOperationReporter.commit()
            sLog.debug("Committing data operation to monitoring")
            if not result["OK"]:
                sLog.error("Could not commit data operation to monitoring", result["Message"])
            else:
                sLog.debug("Done committing to monitoring")
            return result
        return S_OK()

    @convertToReturnValue
    def _sendDataAccounting(self, baseDict, commitFlag=False, delayedCommit=False, startTime=False, endTime=False):
        """Send the data to the accounting system"""
        returnValueOrRaise(self.dataOp.setValuesFromDict(baseDict))

        if startTime:
            self.dataOp.setStartTime(startTime)
            self.dataOp.setEndTime(endTime)
        else:
            self.dataOp.setStartTime()
            self.dataOp.setEndTime()
        # Adding only to register
        if not commitFlag and not delayedCommit:
            return gDataStoreClient.addRegister(self.dataOp)

        # Adding to register and committing
        if commitFlag and not delayedCommit:
            gDataStoreClient.addRegister(self.dataOp)
            sLog.debug("Committing data operation to accounting")
            result = gDataStoreClient.commit()

            if not result["OK"]:
                sLog.error("Could not commit data operation to accounting", result["Message"])
                return result
            sLog.debug("Done committing to accounting")
        # Only late committing
        else:
            result = self.dataOp.delayedCommit()
            if not result["OK"]:
                sLog.error("Could not delay-commit data operation to accounting")
        return result

    def sendData(self, baseDict, commitFlag=False, delayedCommit=False, startTime=False, endTime=False):
        """
        Sends the input to Monitoring or Accounting based on the monitoringOptions

        :param dict baseDict: contains a key/value pair
        :param bool commitFlag: decides whether to commit the record or not.
        :param bool delayedCommit: decides whether to commit the record with delay (only for sending to Accounting)
        :param int startTime: epoch time, start time of the plot
        :param int endTime: epoch time, end time of the plot
        """

        baseDict["ExecutionSite"] = DIRAC.siteName()

        # Send data and commit prioritizing the first monitoring option in the list
        for methId, _sendDataMeth in enumerate(self._sendDataMethods):
            res = _sendDataMeth(
                baseDict, commitFlag=commitFlag, delayedCommit=delayedCommit, startTime=startTime, endTime=endTime
            )
            if not res["OK"]:
                sLog.error("DataOperationSender.sendData: could not send data", f"{res}")
                # If this is the first backend, we stop
                if methId == 0:
                    sLog.error(
                        "DataOperationSender.sendData: failure of the master accounting system, not trying the others"
                    )
                    return res

        return S_OK()

    def _commitAccounting(self):
        result = gDataStoreClient.commit()
        sLog.debug("Concluding the sending and committing data operation to accounting")
        if not result["OK"]:
            sLog.error("Could not commit data operation to accounting", result["Message"])
        sLog.debug("Committing to accounting concluded")
        return result

    def _commitMonitoring(self):
        result = self.dataOperationReporter.commit()
        sLog.debug("Committing data operation to monitoring")
        if not result["OK"]:
            sLog.error("Could not commit data operation to monitoring", result["Message"])
        sLog.debug("Committing to monitoring concluded")
        return result

    # Call this method in order to commit all records added but not yet committed to Accounting and Monitoring
    def concludeSending(self):
        """Flush to the services what is still queued"""
        for methId, _commitMeth in enumerate(self._commitMethods):
            res = _commitMeth()
            if not res["OK"]:
                sLog.error("DataOperationSender.concludeSending: could not commit data", f"{res}")
                # If this is the first backend, we stop
                if methId == 0:
                    sLog.error(
                        "DataOperationSender.sendData: failure of the master accounting system, not trying the others"
                    )
                    return res
        return S_OK()
