""" Within this module is defined the class from which all other accounting types are defined
"""

import datetime

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Base.Client import Client
from DIRAC.AccountingSystem.Client.DataStoreClient import gDataStoreClient


class BaseAccountingType:
    def __init__(self):
        self.keyFieldsList = []
        self.valueFieldsList = []
        self.valuesList = []
        self.fieldsList = []
        self.startTime = 0
        self.endTime = 0
        self.dataTimespan = 0
        self.bucketsLength = [
            (86400 * 8, 3600),  # <1w+1 = 1h
            (15552000, 86400),  # >1w+1d <6m = 1d
            (31104000, 604800),  # >6m = 1w
        ]
        self.definitionKeyFields = []
        self.definitionAccountingFields = []

    def checkType(self):
        """
        Check that everything is defined
        """
        if not self.definitionKeyFields:
            raise Exception("definitionKeyFields has to be filled prior to utilization")
        if not self.definitionAccountingFields:
            raise Exception("definitionAccountingFields has to be filled prior to utilization")
        for key in self.definitionKeyFields:
            self.keyFieldsList.append(key[0])
        for value in self.definitionAccountingFields:
            self.valueFieldsList.append(value[0])
        self.fieldsList = []
        self.fieldsList.extend(self.keyFieldsList)
        self.fieldsList.extend(self.valueFieldsList)
        if len(self.valuesList) != len(self.fieldsList):
            self.valuesList = [None] * len(self.fieldsList)

    def getDataTimespan(self):
        """
        Get the data timespan for the time. Data older than dataTimespan will be deleted
        """
        return self.dataTimespan

    def setStartTime(self, startTime=False):
        """
        Give a start time for the report
        By default use now
        """
        if not startTime:
            self.startTime = datetime.datetime.utcnow()
        else:
            self.startTime = startTime

    def setEndTime(self, endTime=False):
        """
        Give a end time for the report
        By default use now
        """
        if not endTime:
            self.endTime = datetime.datetime.utcnow()
        else:
            self.endTime = endTime

    def setNowAsStartAndEndTime(self):
        """
        Set current time as start and end time of the report
        """
        self.startTime = datetime.datetime.utcnow()
        self.endTime = self.startTime

    def setValueByKey(self, key, value):
        """
        Add value for key
        """
        if key not in self.fieldsList:
            return S_ERROR(f"Key {key} is not defined")
        keyPos = self.fieldsList.index(key)
        self.valuesList[keyPos] = value
        return S_OK()

    def setValuesFromDict(self, dataDict):
        """
        Set values from key-value dictionary
        """
        errKeys = []
        for key in dataDict:
            if key not in self.fieldsList:
                errKeys.append(key)
        if errKeys:
            return S_ERROR(f"Key(s) {', '.join(errKeys)} are not valid")
        for key in dataDict:
            self.setValueByKey(key, dataDict[key])
        return S_OK()

    def getValue(self, key):
        try:
            return S_OK(self.valuesList[self.fieldsList.index(key)])
        except IndexError:
            return S_ERROR(f"{key} does not have a value")
        except ValueError:
            return S_ERROR(f"{key} is not a valid key")

    def checkValues(self):
        """
        Check that all values are defined and valid
        """
        errorList = []
        for i in range(len(self.valuesList)):
            key = self.fieldsList[i]
            if self.valuesList[i] is None:
                errorList.append(f"no value for {key}")
            if key in self.valueFieldsList and not isinstance(self.valuesList[i], (int, float)):
                errorList.append(f"value for key {key} is not numerical type")
        if errorList:
            return S_ERROR(f"Invalid values: {', '.join(errorList)}")
        if not self.startTime:
            return S_ERROR("Start time has not been defined")
        if not isinstance(self.startTime, datetime.datetime):
            return S_ERROR("Start time is not a datetime object")
        if not self.endTime:
            return S_ERROR("End time has not been defined")
        if not isinstance(self.endTime, datetime.datetime):
            return S_ERROR("End time is not a datetime object")
        return self.checkRecord()

    def checkRecord(self):
        """To be overwritten by child class"""
        return S_OK()

    def getDefinition(self):
        """
        Get a tuple containing type definition
        """
        return (self.__class__.__name__, self.definitionKeyFields, self.definitionAccountingFields, self.bucketsLength)

    def getValues(self):
        """
        Get a tuple containing report values
        """
        return (self.__class__.__name__, self.startTime, self.endTime, self.valuesList)

    def getContents(self):
        """
        Get the contents
        """
        cD = {}
        if self.startTime:
            cD["starTime"] = self.startTime
        if self.endTime:
            cD["endTime"] = self.endTime
        for iPos in range(len(self.fieldsList)):
            if self.valuesList[iPos]:
                cD[self.fieldsList[iPos]] = self.valuesList[iPos]
        return cD

    def commit(self):
        """
        Commit register to server
        """
        retVal = gDataStoreClient.addRegister(self)
        if not retVal["OK"]:
            return retVal
        return gDataStoreClient.commit()

    def delayedCommit(self):
        """
        Commit register to the server. Delayed commit allows to speed up
        the operation as more registers will be sent at once.
        """

        retVal = gDataStoreClient.addRegister(self)
        if not retVal["OK"]:
            return retVal
        return gDataStoreClient.delayedCommit()

    def remove(self):
        """
        Remove a register from server
        """
        return gDataStoreClient.remove(self)
