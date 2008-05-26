""" DIRAC Dataset DB
"""
__RCSID__ = "$Id: "

import re,time,types,threading

from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.DB import DB
from DIRAC.Core.Utilities.List import stringListToString, intListToString
from DIRAC.Core.DISET.RPCClient import RPCClient

class DatasetDB(DB):

  def __init__(self, maxQueueSize=10 ):
    """ The standard constructor takes the database name (dbname) and the name of the
        configuration section (dbconfig)
    """
    DB.__init__(self,'DatasetDB','DataManagement/DatasetDB', maxQueueSize)
    self.lock = threading.Lock()


  #############################################################################
  #
  # These are the methods to use the Datasets table
  #

  def publishDataset(self,handle,description,longDescription,authorDN,authorGroup,type):
    self.lock.acquire()
    req = "INSERT INTO Datasets (DatasetHandle,Description,LongDescription,CreationDate,AuthorDN,AuthorGroup,Type,Status) VALUES \
    ('%s','%s','%s',UTC_TIMESTAMP(),'%s','%s','%s','New');" % (handle, description, longDescription,authorDN, authorGroup, type)
    res = self._update(req)
    if not res['OK']:
      self.lock.release()
      return res
    req = "SELECT LAST_INSERT_ID();"
    res = self._query(req)
    self.lock.release()
    if not res['OK']:
      return res
    datasetID = int(res['Value'][0][0])
    return S_OK(datasetID)


  def setDatasetStatus(self,handle,status):
    res = self.__getDatasetID(handle)
    if not res['OK']:
      return res
    elif not res['Value']:
      errStr = "DatasetDB.setDatasetStatus: Supplied dataset handle doesnt exist."
      gLogger.error(errStr,handle)
      return S_ERROR(errStr)
    else:
      datasetID = res['Value']
      req = "UPDATE Datasets SET Status='%s' WHERE DatasetID=%s;" % (status,datasetID)
      res = self._update(req)
      return res

  def __getDatasetID(self,handle):
    """ Retrieves the ID for the dataset
    """
    req = "SELECT DatasetID from Datasets WHERE DatasetHandle = '%s';" % handle
    res = self._query(req)
    if not res['OK']:
      gLogger.error("DatasetDB.__getDatasetID: Failed to obtain DatasetID.",res['Message'])
      return res
    elif res['Value'] == ():
      gLogger.info("DatasetDB.__getDatasetID: Dataset %s doesnt exists." % handle)
      return S_OK()
    else:
      return S_OK(res['Value'][0][0])

  def datasetExists(self,handle):
    res = self.__getDatasetID(handle)
    if not res['OK']:
      return res
    elif not res['Value']:
      return S_OK(False)
    else:
      return S_OK(True)

  def removeDataset(self,handle):
    res = self.__getDatasetID(handle)
    if not res['OK']:
      return res
    elif not res['Value']:
      errStr = "DatasetDB.removeDataset: Supplied dataset handle doesnt exist."
      gLogger.error(errStr,handle)
      return S_ERROR(errStr)
    else:
      datasetID = res['Value']
      req = "DELETE FROM DatasetLog where DatasetID = %s;" % datasetID
      res = self._update(req)
      if not res['OK']:
        return res
      req = "DELETE FROM DatasetParameters where DatasetID = %s;" % datasetID
      res = self._update(req)
      if not res['OK']:
        return res
      req = "DELETE FROM Datasets where DatasetID = %s;" % datasetID
      res = self._update(req)
      return res

  def getAllDatasets(self):
    datasetList = []
    req = "SELECT DatasetID,DatasetHandle,Description,LongDescription,CreationDate,AuthorDN,AuthorGroup,Type,Status FROM Datasets;"
    res = self._query(req)
    if not res['OK']:
      return res
    for datasetID,handle,description,longDesc,createDate,authorDN,authorGroup,type,status in res['Value']:
      datasetDict = {}
      datasetDict['DatasetID'] = datasetID
      datasetDict['Handle'] = handle
      datasetDict['Description'] = description
      datasetDict['LongDescription'] = longDesc
      datasetDict['CreationDate'] = createDate
      datasetDict['AuthorDN'] = authorDN
      datasetDict['AuthorGroup'] = authorGroup
      datasetDict['Type'] = type
      datasetDict['Status'] = status
      res = self.__getDatasetParameters(datasetID)
      if not res['OK']:
        return res
      if res['Value']:
        datasetDict['Additional'] = res['Value']
      datasetList.append(datasetDict)
    return S_OK(datasetList)

  #############################################################################
  #
  # These are the methods to use the DatasetParameters table
  #

  def addDatasetParameter(self,handle,paramName,paramValue):
    """ Add a parameter for the supplied dataset
    """
    res = self.__getDatasetID(handle)
    if not res['OK']:
      return res
    elif not res['Value']:
      errStr = "DatasetDB.addDatasetParameter: Supplied dataset handle doesnt exist."
      gLogger.error(errStr,handle)
      return S_ERROR(errStr)
    else:
      datasetID = res['Value']
      return self.__addDatasetParameter(datasetID,paramName,paramValue)

  def addDatasetParameters(self,handle,paramDict):
    """ Add all parameters for the supplied dataset
    """
    res = self.__getDatasetID(handle)
    if not res['OK']:
      return res
    elif not res['Value']:
      errStr = "DatasetDB.addDatasetParameters: Supplied dataset handle doesnt exist."
      gLogger.error(errStr,handle)
      return S_ERROR(errStr)
    else:
      datasetID = res['Value']
      for paramName,paramValue in paramDict.items():
        res = self.__addDatasetParameter(datasetID,paramName,paramValue)
        if not res['OK']:
          return res
      return res

  def __addDatasetParameter(self,datasetID,paramName,paramValue):
    req = "INSERT INTO DatasetParameters (DatasetID,ParameterName,ParameterValue) VALUES (%s,'%s','%s');" % (datasetID,paramName,paramValue)
    res = self._update(req)
    return res

  def getDatasetParameters(self,handle):
    res = self.__getDatasetID(handle)
    if not res['OK']:
      return res
    elif not res['Value']:
      errStr = "DatasetDB.getDatasetParameters: Supplied dataset handle doesnt exist."
      gLogger.error(errStr,handle)
      return S_ERROR(errStr)
    else:
      datasetID = res['Value']
      return self.__getDatasetParameters(datasetID)

  def __getDatasetParameters(self,datasetID):
    req = "SELECT DatasetID, ParameterName, ParameterValue FROM DatasetParameters WHERE DatasetID=%s;" % datasetID
    res = self._query(req)
    if not res['OK']:
      return res
    parameters = {}
    for datasetID, parameterName,parameterValue in res['Value']:
      parameters[parameterName] = parameterValue
    return S_OK(parameters)

  #############################################################################
  #
  # These are the methods to use the DatasetLog table
  #

  def getDatasetHistory(self,handle):
    res = self.__getDatasetID(handle)
    if not res['OK']:
      return res
    elif not res['Value']:
      errStr = "DatasetDB.getDatasetHistory: Supplied dataset handle doesnt exist."
      gLogger.error(errStr,handle)
      return S_ERROR(errStr)
    else:
      datasetID = res['Value']
      req = "SELECT DatasetID, Message, Author, MessageDate FROM DatasetLog WHERE DatasetID=%s ORDER BY MessageDate;" % (datasetID)
      res = self._query(req)
      if not res['OK']:
        return res
      historyList = []
      for datasetID, message, authorDN, messageDate in res['Value']:
        datasetDict = {}
        datasetDict['Message'] = message
        datasetDict['AuthorDN'] = authorDN
        datasetDict['MessageDate'] = messageDate
        historyList.append(datasetDict)
    return S_OK(historyList)

  def updateDatasetLogging(self,handle,message,authorDN):
    res = self.__getDatasetID(handle)
    if not res['OK']:
      return res
    elif not res['Value']:
      errStr = "DatasetDB.updateDatasetLogging: Supplied dataset handle doesnt exist."
      gLogger.error(errStr,handle)
      return S_ERROR(errStr)
    else:
      datasetID = res['Value']
      req = "INSERT INTO DatasetLog (DatasetID,Message,Author,MessageDate) VALUES (%s,'%s','%s',UTC_TIMESTAMP());" % (datasetID,message,authorDN)
      res = self._update(req)
      return res
