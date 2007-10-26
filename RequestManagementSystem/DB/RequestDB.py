""" RequestDB is a front end to the Request Database.
"""

from DIRAC.Core.Base.DB import DB
from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Utilities.List import intListToString
from DIRAC.RequestManagementSystem.Client.DataManagementRequest import DataManagementRequest

import os
import threading
from types import *

gLogger.initialize('RMS','/Databases/RequestDB/Test')

class RequestDB:

  def __init__(self, backend):
    if backend == 'mysql':
      self.requestDB = RequestDBMySQL()
    elif backend == 'file':
      self.requestDB = RequestDBFile()

  def setRequest(self,requestType,requestName,requestStatus,requestString):
    # Can probably get rid of (requestType,requestStatus) from this request, it is incoded in the request string.
    result = self.requestDB._setRequest(requestType,requestName,requestStatus,requestString)
    return result

  def setRequestStatus(self,requestType,requestName,status):
    result = self.requestDB._setRequestStatus(requestType,requestName,status)
    return result

  def getRequest(self,requestType):
    result = self.requestDB._getRequest(requestType)
    return result

  def deleteRequest(self,requestName):
    result = self.requestDB._deleteRequest(requestName)
    return result

  def updateRequest(self,requestName,requestString):
    result = self.requestDB._updateRequest(requestName,requestString)
    return result

  def serveRequest(self):
    result = self.requestDB._serveRequest()
    return result

  """
  def serveRequest(self,requestType,status):
    result = self.requestDB._getRequest(requestType,status)
    if result['Value']:
      requestDict = result['Value']
      removalResult = self.requestDB._deleteRequest(requestType,requestDict['requestName'])
      if removalResult['OK']:
        return result
        else:
        return S_ERROR('Failed to remove request')
    return result
  """

  def getDBSummary(self):
    result = self.requestDB._getDBSummary()
    return result

class RequestDBFile:
  def __init__(self, systemInstance ='Default'):
    self.root = self.__getRequestDBPath()
    self.lastRequest = {}

  def __getRequestDBPath(self):
    root = gConfig.getValue('/Systems/RequestManagement/Development/Services/RequestHandler/Path')
    if not root:
      diracRoot = gConfig.getValue('/LocalSite/Root')
      root = diracRoot+'/requestdb'
    if not os.path.exists(root):
      os.makedirs(root)
    return root

  def _deleteRequest(self,requestType,requestName):
    reqDir = '%s/%s' % (self.root,requestType)
    if os.path.isdir(reqdir):
      statusList = os.listdir(reqdir)
      for status in statusList:
        statusDir = '%s/%s' % (reqDir,status)
        requestPath = '%s/%s' % (statusDir,requestName)
        if os.path.isdir(statusDir) and os.path.exists(requestPath):
          os.remove(requestPath)
          return S_OK()
    return S_ERROR()

  def __getRequestStatus(self,requestType,requestName):
    reqDir = '%s/%s' % (self.root,requestType)
    if os.path.isdir(reqdir):
      statusList = os.listdir(reqDir)
      for status in statusList:
        statusDir = '%s/%s' % (reqDir,status)
        requestPath = '%s/%s' % (statusDir,requestName)
        if os.path.isdir(statusDir) and os.path.exists(requestPath):
          result = S_OK()
          result['Value'] = status
          return result
    return S_ERROR('Request does not exist')

  def __getRequestString(self,requestType,requestName,status=None):
    if not status:
      result = status = self.__getRequestStatus(requestType, requestName)
      if not result['OK']:
        return result
      else:
        status = result['Value']
    requestPath = '%s/%s/%s/%s' % (self.root,requestType,status,requestName)
    reqfile = open(requestPath,'r')
    requestString = reqfile.read()
    result = S_OK()
    result['Value'] = requestString
    return result

  def _setRequestStatus(self,requestType,requestName,status='Done'):
    result = self.__getRequestStatus(requestType, request)
    if result['OK']:
      oldStatus = result['Value']
      reqDir = '%s/%s' % (self.root,requestType)
      oldReqPath = '%s/%s/%s' % (reqDir,oldStatus,requestName)
      newReqDir = '%s/%s' % (reqDir,status)
      if not os.path.exists(newReqDir):
        os.makedirs(newReqDir)
      newReqPath = '%s/%s/%s' % (reqDir,status,requestName)
      os.rename(oldReqPath,newReqPath)
      return S_OK()
    return S_ERROR('Unable to locate request')

  def _getDBSummary(self):
    summaryDict = {}
    requestTypeList = os.listdir(self.root)
    for requestType in requestTypeList:
      summaryDict[requestType] = {}
      reqTypeDir = '%s/%s' % (self.root,requestType)
      statusList = os.listdir(reqTypeDir)
      for status in statusList:
        reqTypeStatusDir = '%s/%s' % (reqTypeDir,status)
        requests = os.listdir(reqTypeStatusDir)
        summaryDict[requestType][status] = len(requests)
    result = S_OK()
    result['Value'] = summaryDict
    return result

  def _setRequest(self,requestType,requestName,requestStatus,requestString):
    result = self.__getRequestStatus(requestType,requestName)
    if result['OK']:
      self.__deleteRequest(requestType,requestName)
    reqDir = '%s/%s/%s' % (self.root,requestType,requestStatus)
    if not os.path.exists(reqDir):
      os.makedirs(reqDir)
    requestPath = '%s/%s' % (reqDir,requestName)
    requestFile = open(requestPath,'w')
    requestFile.write(requestString)
    requestFile.close()
    return S_OK()

  def _getRequest(self,requestType,requestStatus):
    requests = []
    reqDir = '%s/%s/%s' % (self.root,requestType,requestStatus)
    if os.path.exists(reqDir):
      requestList = os.listdir(reqDir)
      for request in requestList:
        if (re.search(r'.xml',req) or re.search(r'.jdl',req)):
          requests.append(request)
    result = S_OK()
    if len(requests) > 0:
      if not self.lastRequest.has_key(requestType):
        self.lastRequest[requestType] = {requestStatus:''}
      if not self.lastRequest[requestType].has_key(requestStatus):
        self.lastRequest[requestType][requestStatus] = ''
      lastRequest = self.lastRequest[requestType][requestStatus]
      requestName = self.__selectRequestCursor(requests,lastRequest)
      reqStr = self.__getRequestString(requestType,requestName,requestStatus)
      if reqStr['OK']:
        requestString = reqStr['Value']
        self.lastRequest[requestType][requestStatus] = requestName
        resDict = {'requestString':requestString,'requestName':requestName}
        result['Value'] = resDict
      else:
        result = S_ERROR('Failed to get request string')
    else:
      result['Value'] = ''
    return result

  def __selectRequestCursor(self,requests,lastRequest):
    if lastRequest in requests:
      numberOfRequests = len(requests)
      lastIndex = requests.index(lastRequest)
      newIndex = lastIndex+1
      if newIndex >= numberOfRequests:
        newIndex = 0
    else:
      newIndex = 0
    newRequest = requests[newIndex]
    return newRequest

class RequestDBMySQL(DB):

  def __init__(self, systemInstance ='Default', maxQueueSize=10 ):
    DB.__init__(self,'RequestDB','RequestManagement/RequestDB',maxQueueSize)
    self.getIdLock = threading.Lock()

  def _setRequestStatus(self,requestType,requestName,status):
    attrName = 'RequestID'
    res = self._getRequestAttribute(attrName,requestName=requestName)
    if not res['OK']:
      return res
    requestID = res['Value']
    attrName = 'Status'
    attrValue = status
    res = self._setRequestAttribute(requestID,attrName,attrValue)
    return res

  def _serveRequest(self):
    self.getIdLock.acquire()
    req = "SELECT MAX(RequestID) FROM Requests;"
    res = self._query(req)
    if not res['OK']:
      err = 'RequestDB._serveRequest: Failed to retrieve max RequestID'
      self.getIdLock.release()
      return S_ERROR('%s\n%s' % (err,res['Message']))
    requestID = res['Value'][0][0]
    #_getRequest(
    #_removeRequest(
    req = "SELECT * from SubRequests WHERE RequestID=%s" % requestID
    res = self._query(req)
    if not res['OK']:
      err = 'RequestDB._serveRequest: Failed to retrieve SubRequest IDs for RequestID %s' % requestID
      self.getIdLock.release()
      return S_ERROR('%s\n%s' % (err,res['Message']))
    subRequestIDs = []
    for subRequestID in res['Value']:
      subRequestIDs.append(subRequestID[0])
    print subRequestIDs
    req = "SELECT * from SubRequests WHERE RequestID IN =%s" % requestID
    self.getIdLock.release()
    #THIS IS WHERE I AM IN THIS METHOD
    # STILL TO DO: compile the request string
    # STILL TO DO: remove the request completely from the db
    # STILL TO DO: return the request string

  def _getRequest(self,requestType):
    dmRequest = DataManagementRequest()
    self.getIdLock.acquire()
    req = "SELECT RequestID,SubRequestID FROM SubRequests WHERE Status = 'Waiting' AND RequestType = '%s' ORDER BY RequestID LIMIT 1;" % requestType
    res = self._query(req)
    if not res['OK']:
      err = 'RequestDB._getRequest: Failed to retrieve max RequestID'
      self.getIdLock.release()
      return S_ERROR('%s\n%s' % (err,res['Message']))
    if not res['Value']:
      self.getIdLock.release()
      return S_OK()
    requestID = res['Value'][0][0]
    dmRequest.setRequestID(requestID)
    subRequestIDs = []
    req = "SELECT SubRequestID,Operation,SourceSE,TargetSE,SpaceToken,Catalogue \
    from SubRequests WHERE RequestID=%s AND RequestType='%s' AND Status='%s'" % (requestID,requestType,'Waiting')
    res = self._query(req)
    if not res['OK']:
      err = 'RequestDB._getRequest: Failed to retrieve SubRequests for RequestID %s' % requestID
      self.getIdLock.release()
      return S_ERROR('%s\n%s' % (err,res['Message']))
    for subRequestID,operation,sourceSE,targetSE,spaceToken,catalogue in res['Value']:
      subRequestIDs.append(subRequestID)
      res = dmRequest.initiateSubRequest(requestType)
      ind = res['Value']
      subRequestDict = {'Operation':operation,'SourceSE':sourceSE,'TargetSE':targetSE,'Catalogue':catalogue,'SpaceToken':spaceToken,'Status':'Waiting','SubRequestID':subRequestID}
      res = dmRequest.setSubRequestAttributes(ind,requestType,subRequestDict)
      if not res['OK']:
        err = 'RequestDB._getRequest: Failed to set subRequest attributes for RequestID %s' % requestID
        self.getIdLock.release()
        return S_ERROR('%s\n%s' % (err,res['Message']))

      req = "SELECT FileID,LFN,Size,PFN,GUID,Md5,Addler,Attempt,Status \
      from Files WHERE SubRequestID = %s;" % subRequestID
      res = self._query(req)
      if not res['OK']:
        err = 'RequestDB._getRequest: Failed to get File attributes for RequestID %s.%s' % (requestID,subRequestID)
        self.getIdLock.release()
        return S_ERROR('%s\n%s' % (err,res['Message']))
      files = []
      for fileID,lfn,size,pfn,guid,md5,addler,attempt,status in res['Value']:
        fileDict = {'FileID':fileID,'LFN':lfn,'Size':size,'PFN':pfn,'GUID':guid,'Md5':md5,'Addler':addler,'Attempt':attempt,'Status':status}
        files.append(fileDict)
      res = dmRequest.setSubRequestFiles(ind,requestType,files)
      if not res['OK']:
        err = 'RequestDB._getRequest: Failed to set files into Request for RequestID %s.%s' % (requestID,subRequestID)
        self.getIdLock.release()
        return S_ERROR('%s\n%s' % (err,res['Message']))

      req = "SELECT Dataset,Status FROM Datasets WHERE SubRequestID = %s;" % subRequestID
      res = self._query(req)
      if not res['OK']:
        err = 'RequestDB._getRequest: Failed to get Datasets for RequestID %s.%s' % (requestID,subRequestID)
        self.getIdLock.release()
        return S_ERROR('%s\n%s' % (err,res['Message']))
      datasets = []
      for dataset,status in res['Value']:
        datasets.append(dataset)
      res = dmRequest.setSubRequestDatasets(ind,requestType,datasets)
      if not res['OK']:
        err = 'RequestDB._getRequest: Failed to set datasets into Request for RequestID %s.%s' % (requestID,subRequestID)
        self.getIdLock.release()
        return S_ERROR('%s\n%s' % (err,res['Message']))

    req = "SELECT RequestName,JobID,OwnerDN,DIRACInstance,CreationTime from Requests WHERE RequestID = %s;" % requestID
    res = self._query(req)
    if not res['OK']:
      err = 'RequestDB._getRequest: Failed to retrieve max RequestID'
      self.getIdLock.release()
      return S_ERROR('%s\n%s' % (err,res['Message']))
    requestName,jobID,ownerDN,diracInstance,creationTime = res['Value'][0]
    dmRequest.setRequestName(requestName)
    dmRequest.setJobID(jobID)
    dmRequest.setOwnerDN(ownerDN)
    dmRequest.setDiracInstance(diracInstance)
    dmRequest.setCreationTime(creationTime)

    res = dmRequest.toXML()
    if not res['OK']:
      err = 'RequestDB._getRequest: Failed to create XML for RequestID %s' % (requestID)
      self.getIdLock.release()
      return S_ERROR('%s\n%s' % (err,res['Message']))
    requestString = res['Value']

    failed = False
    for subRequestID in subRequestIDs:
      res = self._setSubRequestAttribute(subRequestID,'Status','Assigned')
      if not res['OK']:
        failed = True
    if failed:
      for subRequestID in subRequestIDs:
        res = self._setSubRequestAttribute(subRequestID,'Status','Waiting')
      self.getIdLock.release()
      err = 'RequestDB._getRequest: Failed to set sub request status to assigned'
      return S_ERROR(err)
    self.getIdLock.release()

    #still have to manage the status of the dataset properly
    resultDict = {}
    resultDict['RequestName'] = requestName
    resultDict['RequestString'] = requestString
    return S_OK(resultDict)


  def _setRequest(self,requestType,requestName,requestStatus,requestString):
    request = DataManagementRequest(request=requestString)
    requestTypes = ['transfer','register','removal','stage']
    failed = False
    res = self._getRequestID(requestName)
    if not res['OK']:
      return res
    requestID = res['Value']
    subRequestIDs = []
    res = self.__setRequestAttributes(requestID,request)
    if res['OK']:
      for requestType in requestTypes:
        res = request.getNumSubRequests(requestType)
        if res['OK']:
          numRequests = res['Value']
          for ind in range(numRequests):
            res = self._getSubRequestID(requestID,requestType)
            if res['OK']:
              subRequestID = res['Value']
              subRequestIDs.append(subRequestID)
              res = self.__setSubRequestAttributes(ind,requestType,subRequestID,request)
              if res['OK']:
                res = self.__setSubRequestFiles(ind,requestType,subRequestID,request)
                if res['OK']:
                  res = self.__setSubRequestDatasets(ind,requestType,subRequestID,request)
                  if not res['OK']:
                    failed = True
                else:
                  failed = True
              else:
                failed = True
            else:
              failed = True
        else:
          failed = True
    else:
      failed = True
    for subRequestID in subRequestIDs:
      res = self._setSubRequestAttribute(subRequestID,'Status','Waiting')
      if not res['OK']:
        failed = True
    res = self._setRequestAttribute(self,requestID,'Status','Waiting')
    if not res['OK']:
      failed = True
    if failed:
      res = self._deleteRequest(requestName)
      return S_ERROR('Failed to set request')
    else:
      return S_OK(requestID)

  def _updateRequest(self,requestName,requestString):
    request = DataManagementRequest(request=requestString)
    requestTypes = ['transfer','register','removal','stage']
    failed = False
    requestID = request.getRequestID()
    failed = False
    for requestType in requestTypes:
      res = request.getNumSubRequests(requestType)
      if res['OK']:
        numRequests = res['Value']
        for ind in range(numRequests):
          res = request.getSubRequestAttributes(ind,requestType)
          if res['OK']:
            if res['Value'].has_key('SubRequestID'):
              subRequestID = res['Value']['SubRequestID']
              res = self.__updateSubRequestFiles(ind,requestType,subRequestID,request)
              if not res['OK']:
                failed = True
            else:
              failed = True
          else:
            failed = True
      else:
        failed = True
    if failed:
      errStr = 'Failed to update request %s status' % requestID
      return S_ERROR(errStr)
    return S_OK()

  def _deleteRequest(self,requestName):
    #This method needs extended to truely remove everything that is being removed i.e.fts job entries etc.
    failed = False
    req = "SELECT RequestID from Requests WHERE RequestName='%s';" % requestName
    res = self._query(req)
    if res['OK']:
      if res['Value']:
        requestID = res['Value'][0]
        req = "SELECT SubRequestID from SubRequests where RequestID = %s;" % requestID
        res = self._query(req)
        if res['OK']:
          subRequestIDs = []
          for reqID in res['Value']:
            subRequestIDs.append(reqID[0])
          idString = intListToString(subRequestIDs)
          req = "DELETE FROM Files WHERE SubRequestID IN (%s);" % idString['Value']
          res = self._update(req)
          if not res['OK']:
            failed = True
          req = "DELETE FROM Datasets WHERE SubRequestID IN (%s);" % idString['Value']
          res = self._update(req)
          if not res['OK']:
            failed = True
          req = "DELETE FROM SubRequests WHERE RequestID = %s;" % requestID
          res = self._update(req)
          if not res['OK']:
            failed = True
          req = "DELETE from Requests where RequestID = %s;" % requestID
          res = self._update(req)
          if not res['OK']:
            failed = True
          if failed:
            errStr = 'RequestDB._deleteRequest: Failed to fully remove Request %s' % requestID
            return S_ERROR(errStr)
          else:
            return S_OK()
        else:
          errStr = 'RequestDB._deleteRequest: Unable to retrieve SubRequestIDs for Request %s' % requestID
          return S_ERROR(errStr)
      else:
        errStr = 'RequestDB._deleteRequest: No RequestID found for %s' % requestName
        return S_ERROR(errStr)
    else:
      errStr = "RequestDB._deleteRequest: Failed to retrieve RequestID for %s" % requestName
      return S_ERROR(errStr)

  def __setSubRequestDatasets(self,ind,requestType,subRequestID,request):
    res = request.getSubRequestDatasets(ind,requestType)
    if not res['OK']:
      return S_ERROR('Failed to get request datasets')
    datasets = res['Value']
    for dataset in datasets:
      res = self._setDataset(subRequestID,dataset)
      if not res['OK']:
        return S_ERROR('Failed to set dataset in DB')
    return res

  def __updateSubRequestFiles(self,ind,requestType,subRequestID,request):
    res = request.getSubRequestFiles(ind,requestType)
    if not res['OK']:
      return S_ERROR('Failed to get request files')
    files = res['Value']
    for file in files:
      if not file.has_key('FileID'):
        return S_ERROR('No FileID associated to file')
      fileID = file['FileID']
      for fileAttribute,attributeValue in file.items():
        if not fileAttribute == 'FileID':
          res = self._setFileAttribute(subRequestID,fileID,fileAttribute,attributeValue)
          if not res['OK']:
            return S_ERROR('Failed to set file attribute in DB')
    return res

  def __setSubRequestFiles(self,ind,requestType,subRequestID,request):
    res = request.getSubRequestFiles(ind,requestType)
    if not res['OK']:
      return S_ERROR('Failed to get request files')
    files = res['Value']
    for file in files:
      res = self._getFileID(subRequestID)
      if not res['OK']:
        return S_ERROR('Failed to get FileID')
      fileID = res['Value']
      for fileAttribute,attributeValue in file.items():
        if not fileAttribute == 'FileID':
          res = self._setFileAttribute(subRequestID,fileID,fileAttribute,attributeValue)
          if not res['OK']:
            return S_ERROR('Failed to set file attribute in DB')
    return res

  def __setSubRequestAttributes(self,ind,requestType,subRequestID,request):
    res = request.getSubRequestAttributes(ind,requestType)
    if not res['OK']:
      return S_ERROR('Failed to get sub request attributes')
    requestAttributes = res['Value']
    for requestAttribute,attributeValue in requestAttributes.items():
      if not requestAttribute == 'SubRequestID':
        res = self._setSubRequestAttribute(subRequestID,requestAttribute,attributeValue)
        if not res['OK']:
          return S_ERROR('Failed to set sub request in DB')
    return res

  def __setRequestAttributes(self,requestID,request):
    """ Insert into the DB the request attributes
    """
    res = self._setRequestAttribute(requestID,'CreationTime', request.getCurrentDate())
    if not res['OK']:
      return res
    res = self._setRequestAttribute(requestID,'JobID',request.getJobID())
    if not res['OK']:
      return res
    res = self._setRequestAttribute(requestID,'OwnerDN',request.getOwnerDN())
    if not res['OK']:
      return res
    res = self._setRequestAttribute(requestID,'DIRACInstance',request.getDiracInstance())
    return res

  def _setRequestAttribute(self,requestID, attrName, attrValue):
    req = "UPDATE Requests SET %s='%s' WHERE RequestID='%s';" % (attrName,attrValue,requestID)
    res = self._update(req)
    if res['OK']:
      return res
    else:
      return S_ERROR('RequestDB.setRequestAttribute: failed to set attribute')

  def _getRequestAttribute(self,attrName,requestID=None,requestName=None):
    if requestID:
      req = "SELECT %s from Requests WHERE RequestID=%s;" % (attrName,requestID)
    elif requestName:
      req = "SELECT %s from Requests WHERE RequestName='%s';" % (attrName,requestName)
    else:
      return S_ERROR('RequestID or RequestName must be supplied')
    res = self._query(req)
    if not res['OK']:
      return res
    if res['Value']:
      attrValue = res['Value'][0][0]
      return S_OK(attrValue)
    else:
      errStr = 'Failed to retreive %s for Request %s%s' % (attrName,requestID,requestName)
      return S_ERROR(errStr)

  def _setSubRequestAttribute(self,subRequestID, attrName, attrValue):
    req = "UPDATE SubRequests SET %s='%s' WHERE SubRequestID='%s';" % (attrName,attrValue,subRequestID)
    res = self._update(req)
    if res['OK']:
      return res
    else:
      return S_ERROR('RequestDB.setRequestAttribute: failed to set attribute')

  def _setFileAttribute(self,subRequestID, fileID, attrName, attrValue):
    req = "UPDATE Files SET %s='%s' WHERE SubRequestID='%s' AND FileID='%s';" % (attrName,attrValue,subRequestID,fileID)
    res = self._update(req)
    if res['OK']:
      return res
    else:
      return S_ERROR('RequestDB.setFileAttribute: failed to set attribute')

  def _setDataset(self,subRequestID,dataset):
    req = "INSERT INTO Datasets (Dataset,SubRequestID) VALUES ('%s',%s);" % (dataset,subRequestID)
    res = self._update(req)
    if res['OK']:
      return res
    else:
      return S_ERROR('RequestDB.setFileAttribute: failed to set attribute')

  def _getFileID(self,subRequestID):
    self.getIdLock.acquire()
    req = "INSERT INTO Files (Status,SubRequestID) VALUES ('%s','%s');" % ('New',subRequestID)
    res = self._update(req)
    if not res['OK']:
      self.getIdLock.release()
      return S_ERROR( '%s\n%s' % (err, res['Message'] ) )
    req = 'SELECT MAX(FileID) FROM Files WHERE SubRequestID=%s' % subRequestID
    res = self._query(req)
    if not res['OK']:
      self.getIdLock.release()
      return S_ERROR( '%s\n%s' % (err, res['Message'] ) )
    self.getIdLock.release()
    try:
      fileID = int(res['Value'][0][0])
      self.log.info( 'RequestDB: New FileID served "%s"' % fileID )
    except Exception, x:
      return S_ERROR( '%s\n%s' % (err, str(x) ) )
    return S_OK(fileID)

  def _getRequestID(self,requestName):
    self.getIdLock.acquire()
    req = "SELECT RequestID from Requests WHERE RequestName='%s';" % requestName
    res = self._query(req)
    if not res['OK']:
      err = 'RequestDB._getRequestID: Failed to get RequestID from RequestName'
      return S_ERROR( '%s\n%s' % (err, res['Message'] ) )
    if not len(res['Value']) == 0:
      err = 'RequestDB._getRequestID: Duplicate entry for RequestName'
      return S_ERROR(err)
    req = 'INSERT INTO Requests (RequestName,SubmissionTime) VALUES ("%s",NOW());' % requestName
    err = 'RequestDB._getRequestID: Failed to retrieve RequestID'
    res = self._update(req)
    if not res['OK']:
      self.getIdLock.release()
      return S_ERROR( '%s\n%s' % (err, res['Message'] ) )
    req = 'SELECT MAX(RequestID) FROM Requests'
    res = self._query(req)
    if not res['OK']:
      self.getIdLock.release()
      return S_ERROR( '%s\n%s' % (err, res['Message'] ) )
    try:
      RequestID = int(res['Value'][0][0])
      self.log.info( 'RequestDB: New RequestID served "%s"' % RequestID )
    except Exception, x:
      self.getIdLock.release()
      return S_ERROR( '%s\n%s' % (err, str(x) ) )
    self.getIdLock.release()
    return S_OK(RequestID)

  def _getSubRequestID(self,requestID,requestType):
    self.getIdLock.acquire()
    req = 'INSERT INTO SubRequests (RequestID,RequestType,SubmissionTime) VALUES (%s,"%s",NOW())' % (requestID,requestType)
    err = 'RequestDB._getSubRequestID: Failed to retrieve SubRequestID'
    res = self._update(req)
    if not res['OK']:
      self.getIdLock.release()
      return S_ERROR( '%s\n%s' % (err, res['Message'] ) )
    req = 'SELECT MAX(SubRequestID) FROM SubRequests;'
    res = self._query(req)
    if not res['OK']:
      self.getIdLock.release()
      return S_ERROR( '%s\n%s' % (err, res['Message'] ) )
    try:
      subRequestID = int(res['Value'][0][0])
      self.log.info( 'RequestDB: New SubRequestID served "%s"' % subRequestID )
    except Exception, x:
      self.getIdLock.release()
      return S_ERROR( '%s\n%s' % (err, str(x) ) )
    self.getIdLock.release()
    return S_OK(subRequestID)
