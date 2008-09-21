# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/RequestManagementSystem/DB/RequestDBMySQL.py,v 1.33 2008/09/21 19:03:40 atsareg Exp $

""" RequestDBMySQL is the MySQL plug in for the request DB
"""

__RCSID__ = "$Id: RequestDBMySQL.py,v 1.33 2008/09/21 19:03:40 atsareg Exp $"

from DIRAC.Core.Base.DB import DB
from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Utilities.List import intListToString
from DIRAC.RequestManagementSystem.Client.RequestContainer import RequestContainer

import os
import threading
import types

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

  def _getSubRequests(self,requestID):
    """ Get subrequest IDs for the given request
    """
    subRequestList = []
    req = "SELECT SubRequestID FROM SubRequests WHERE RequestID=%d" % requestID
    result = self._query(req)
    if not result['OK']:
      return result

    if result['Value']:
      subRequestList = [ int(x[0]) for x in result['Value']]

    return S_OK(subRequestList)

  def setRequestStatus(self,requestName,requestStatus,subRequest_flag=True):
    """ Set request status and optionally subrequest status
    """

    res = self._getRequestAttribute('RequestID',requestName=requestName)
    if not res['OK']:
      return res
    requestID = res['Value']
    res = self._setRequestAttribute(requestID,'Status',requestStatus)
    if not res['OK']:
      return res

    if not subRequest_flag:
      return S_OK()

    result = self._getSubRequests(requestID)
    if result['OK']:
      for subRequestID in result['Value']:
        res = self._setSubRequestAttribute(requestID,subRequestID,'Status',requestStatus)

    return S_OK()

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

  def getDBSummary(self):
    """ Get the summary of the Request DB contents
    """

    summaryDict = {}
    req = "SELECT DISTINCT(RequestType) FROM SubRequests"
    result = self._query(req)
    if not result['OK']:
      return S_ERROR('RequestDBMySQL.getDBSummary: Failed to retrieve request info')
    typeList = []
    for row in result['Value']:
      typeList.append(row[0])

    req = "SELECT DISTINCT(Status) FROM SubRequests"
    result = self._query(req)
    if not result['OK']:
      return S_ERROR('RequestDBMySQL.getDBSummary: Failed to retrieve request info')
    statusList = []
    for row in result['Value']:
      statusList.append(row[0])

    if not typeList:
      return S_OK(summaryDict)

    for rtype in typeList:
      summaryDict[rtype] = {}
      for status in statusList:
        req = "SELECT COUNT(*) FROM SubRequests WHERE RequestType='%s' AND Status='%s'" % (rtype,status)
        result = self._query(req)
        if not result['OK']:
          summaryDict[rtype][status] = 0
        elif not result['Value']:
          summaryDict[rtype][status] = 0
        else:
          summaryDict[rtype][status] = int(result['Value'][0][0])

    return S_OK(summaryDict)

  def getRequest(self,requestType=''):
    dmRequest = RequestContainer(init=False)
    self.getIdLock.acquire()
    req = "SELECT RequestID,SubRequestID FROM SubRequests WHERE Status = 'Waiting' AND RequestType = '%s' ORDER BY LastUpdate ASC LIMIT 1;" % requestType
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
    if requestType:
      req = "SELECT SubRequestID,Operation,Arguments,ExecutionOrder,SourceSE,TargetSE,Catalogue,CreationTime,SubmissionTime,LastUpdate \
      from SubRequests WHERE RequestID=%s AND RequestType='%s' AND Status='%s'" % (requestID,requestType,'Waiting')
    else:
      req = "SELECT SubRequestID,Operation,Arguments,ExecutionOrder,SourceSE,TargetSE,Catalogue,CreationTime,SubmissionTime,LastUpdate \
      from SubRequests WHERE RequestID=%s" % requestID
    res = self._query(req)
    if not res['OK']:
      err = 'RequestDB._getRequest: Failed to retrieve SubRequests for RequestID %s' % requestID
      self.getIdLock.release()
      return S_ERROR('%s\n%s' % (err,res['Message']))

    for tuple in res['Value']:
      self._setSubRequestAttribute(requestID,tuple[0],'Status','Assigned')
    self.getIdLock.release()

    for subRequestID,operation,arguments,executionOrder,sourceSE,targetSE,catalogue,creationTime,submissionTime,lastUpdate in res['Value']:
      subRequestIDs.append(subRequestID)
      res = dmRequest.initiateSubRequest(requestType)
      ind = res['Value']
      subRequestDict = {
                        'Status'        : 'Waiting',
                        'SubRequestID'  : subRequestID,
                        'Operation'     : operation,
                        'Arguments'     : arguments,
                        'ExecutionOrder': int(executionOrder),
                        'SourceSE'      : sourceSE,
                        'TargetSE'      : targetSE,
                        'Catalogue'     : catalogue,
                        'CreationTime'  : creationTime,
                        'SubmissionTime': submissionTime,
                        'LastUpdate'    : lastUpdate
                       }
      res = dmRequest.setSubRequestAttributes(ind,requestType,subRequestDict)
      if not res['OK']:
        err = 'RequestDB._getRequest: Failed to set subRequest attributes for RequestID %s' % requestID
        self.__releaseSubRequests(requestID,subRequestIDs)
        return S_ERROR('%s\n%s' % (err,res['Message']))

      req = "SELECT FileID,LFN,Size,PFN,GUID,Md5,Addler,Attempt,Status \
      from Files WHERE SubRequestID = %s ORDER BY FileID;" % subRequestID
      res = self._query(req)
      if not res['OK']:
        err = 'RequestDB._getRequest: Failed to get File attributes for RequestID %s.%s' % (requestID,subRequestID)
        self.__releaseSubRequests(requestID,subRequestIDs)
        return S_ERROR('%s\n%s' % (err,res['Message']))
      files = []
      for fileID,lfn,size,pfn,guid,md5,addler,attempt,status in res['Value']:
        fileDict = {'FileID':fileID,'LFN':lfn,'Size':size,'PFN':pfn,'GUID':guid,'Md5':md5,'Addler':addler,'Attempt':attempt,'Status':status}
        files.append(fileDict)
      res = dmRequest.setSubRequestFiles(ind,requestType,files)
      if not res['OK']:
        err = 'RequestDB._getRequest: Failed to set files into Request for RequestID %s.%s' % (requestID,subRequestID)
        self.__releaseSubRequests(requestID,subRequestIDs)
        return S_ERROR('%s\n%s' % (err,res['Message']))

      req = "SELECT Dataset,Status FROM Datasets WHERE SubRequestID = %s;" % subRequestID
      res = self._query(req)
      if not res['OK']:
        err = 'RequestDB._getRequest: Failed to get Datasets for RequestID %s.%s' % (requestID,subRequestID)
        self.__releaseSubRequests(requestID,subRequestIDs)
        return S_ERROR('%s\n%s' % (err,res['Message']))
      datasets = []
      for dataset,status in res['Value']:
        datasets.append(dataset)
      res = dmRequest.setSubRequestDatasets(ind,requestType,datasets)
      if not res['OK']:
        err = 'RequestDB._getRequest: Failed to set datasets into Request for RequestID %s.%s' % (requestID,subRequestID)
        self.__releaseSubRequests(requestID,subRequestIDs)
        return S_ERROR('%s\n%s' % (err,res['Message']))

    req = "SELECT RequestName,JobID,OwnerDN,OwnerGroup,DIRACSetup,SourceComponent,CreationTime,SubmissionTime,LastUpdate from Requests WHERE RequestID = %s;" % requestID
    res = self._query(req)
    if not res['OK']:
      err = 'RequestDB._getRequest: Failed to retrieve max RequestID'
      self.__releaseSubRequests(requestID,subRequestIDs)
      return S_ERROR('%s\n%s' % (err,res['Message']))
    requestName,jobID,ownerDN,ownerGroup,diracSetup,sourceComponent,creationTime,submissionTime,lastUpdate = res['Value'][0]
    dmRequest.setRequestName(requestName)
    dmRequest.setJobID(jobID)
    dmRequest.setOwnerDN(ownerDN)
    dmRequest.setOwnerGroup(ownerGroup)
    dmRequest.setDIRACSetup(diracSetup)
    dmRequest.setSourceComponent(sourceComponent)
    dmRequest.setCreationTime(str(creationTime))
    dmRequest.setLastUpdate(str(lastUpdate))
    res = dmRequest.toXML()
    if not res['OK']:
      err = 'RequestDB._getRequest: Failed to create XML for RequestID %s' % (requestID)
      self.__releaseSubRequests(requestID,subRequestIDs)
      return S_ERROR('%s\n%s' % (err,res['Message']))
    requestString = res['Value']
    #still have to manage the status of the dataset properly
    resultDict = {}
    resultDict['RequestName'] = requestName
    resultDict['RequestString'] = requestString
    resultDict['JobID'] = jobID
    return S_OK(resultDict)

  def __releaseSubRequests(self,requestID,subRequestIDs):
    for subRequestID in subRequestIDs:
      res = self._setSubRequestAttribute(requestID,subRequestID,'Status','Waiting')

  def setRequest(self,requestName,requestString):
    request = RequestContainer(init=True,request=requestString)
    print request.toXML()['Value']
    requestTypes = request.getSubRequestTypes()['Value']
    failed = False
    res = self._getRequestID(requestName)
    if not res['OK']:
      # we have a special case here: if request already exists, we override it if it
      # comes from a DIRAC job. This is identified by having a meaningful JobID in
      # the request
      if res['Message'].find('Duplicate') != -1:
        # Duplicate request
        jobID = request.getJobID()['Value']
        if jobID == "Unknown":
          return res
        try:
          jobID = int(jobID)
        except:
          return res
        if jobID > 0:
          # Remove the existing request
          result = self._deleteRequest(requestName)
          if not result['OK']:
            return S_ERROR('Failed to set request: '+message+' can not override')
          res = self._getRequestID(requestName)
          if not res['OK']:
            return res
      else:
        return res
    requestID = res['Value']
    subRequestIDs = {}
    res = self.__setRequestAttributes(requestID,request)
    if res['OK']:
      for requestType in requestTypes:
        res = request.getNumSubRequests(requestType)
        numRequests = res['Value']
        for ind in range(numRequests):
          res = self._getSubRequestID(requestID,requestType)
          if res['OK']:
            subRequestID = res['Value']
            res = self.__setSubRequestAttributes(requestID,ind,requestType,subRequestID,request)
            if res['OK']:
              subRequestIDs[subRequestID] = res['Value']
              res = self.__setSubRequestFiles(ind,requestType,subRequestID,request)
              if res['OK']:
                res = self.__setSubRequestDatasets(ind,requestType,subRequestID,request)
                if not res['OK']:
                  failed = True
                  message = res['Message']
              else:
                failed = True
                message = res['Message']
            else:
              failed = True
              message = res['Message']
          else:
            failed = True
            message = res['Message']
    else:
      failed = True
      message = res['Message']
    for subRequestID,status in subRequestIDs.items():
      res = self._setSubRequestAttribute(requestID,subRequestID,'Status',status)
      if not res['OK']:
        failed = True
        message = res['Message']
    res = self._setRequestAttribute(requestID,'Status','Waiting')
    if not res['OK']:
      failed = True
      message = res['Message']
    if failed:
      res = self._deleteRequest(requestName)
      return S_ERROR('Failed to set request: '+message)
    else:
      return S_OK(requestID)

  def updateRequest(self,requestName,requestString):
    request = RequestContainer(request=requestString)
    requestTypes = ['transfer','register','removal','stage','diset','logupload']
    requestID = request.getRequestID()['Value']
    updateRequestFailed = False
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
              if res['OK']:
                if request.isSubRequestEmpty(ind,requestType)['Value']:
                  res = self._setSubRequestAttribute(requestID,subRequestID,'Status','Done')
                else:
                  res = self._setSubRequestAttribute(requestID,subRequestID,'Status','Waiting')
                if not res['OK']:
                  print 1
                  updateRequestFailed = True
              else:
                print 2
                updateRequestFailed = True
            else:
              print 3
              updateRequestFailed = True
          else:
            print 4
            updateRequestFailed = True
      else:
        print 5
        updateRequestFailed = True
    if updateRequestFailed:
      errStr = 'Failed to update request %s.' % requestID
      return S_ERROR(errStr)
    else:
      if request.isRequestEmpty()['Value']:
        res = self._setRequestAttribute(requestID,'Status','Done')
        if not res['OK']:
          errStr = 'Failed to update request status of %s to Done.' % requestID
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
          req = "DELETE FROM Files WHERE SubRequestID IN (%s);" % idString
          res = self._update(req)
          if not res['OK']:
            failed = True
          req = "DELETE FROM Datasets WHERE SubRequestID IN (%s);" % idString
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
    res = S_OK()
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
    for fileDict in files:
      if not fileDict.has_key('FileID'):
        return S_ERROR('No FileID associated to file')
      fileID = fileDict['FileID']
      req = "UPDATE Files SET"
      for fileAttribute,attributeValue in fileDict.items():
        if not fileAttribute == 'FileID':
          if attributeValue:
            req = "%s %s='%s'," %  (req,fileAttribute,attributeValue)
      req = req.rstrip(',')
      req = "%s WHERE SubRequestID = %s AND FileID = %s;" % (req,subRequestID,fileID)
      res = self._update(req)
      if not res['OK']:
        return S_ERROR('Failed to update file in db')
    return S_OK()

  def __setSubRequestFiles(self,ind,requestType,subRequestID,request):
    """ This is the new method for updating the File table
    """
    res = request.getSubRequestFiles(ind,requestType)
    if not res['OK']:
      return S_ERROR('Failed to get request files')
    files = res['Value']
    for fileDict in files:
      fileAttributes = ['SubRequestID']
      attributeValues = [subRequestID]
      for fileAttribute,attributeValue in fileDict.items():
        if not fileAttribute == 'FileID':
          if attributeValue:
            fileAttributes.append(fileAttribute)
            attributeValues.append(attributeValue)
      if not 'Status' in fileAttributes:
        fileAttributes.append('Status')
        attributeValues.append('Waiting')
      res = self._insert('Files',fileAttributes,attributeValues)
      if not res['OK']:
        return S_ERROR('Failed to insert file into db')
    return S_OK()

  def __setSubRequestAttributes(self,requestID,ind,requestType,subRequestID,request):
    res = request.getSubRequestAttributes(ind,requestType)
    if not res['OK']:
      return S_ERROR('Failed to get sub request attributes')
    requestAttributes = res['Value']
    status = 'Waiting'
    for requestAttribute,attributeValue in requestAttributes.items():
      if requestAttribute == 'Status':
        status = attributeValue
      elif not requestAttribute == 'SubRequestID':
        res = self._setSubRequestAttribute(requestID,subRequestID,requestAttribute,attributeValue)
        if not res['OK']:
          return S_ERROR('Failed to set sub request in DB')
    return S_OK(status)

  def __setRequestAttributes(self,requestID,request):
    """ Insert into the DB the request attributes
    """
    res = self._setRequestAttribute(requestID,'CreationTime', request.getCreationTime()['Value'])
    if not res['OK']:
      return res
    jobID = request.getJobID()['Value']
    if jobID and not jobID == 'Unknown':
      res = self._setRequestAttribute(requestID,'JobID',int(jobID))
      if not res['OK']:
        return res
    res = self._setRequestAttribute(requestID,'OwnerDN',request.getOwnerDN()['Value'])
    if not res['OK']:
      return res
    res = self._setRequestAttribute(requestID,'OwnerGroup',request.getOwnerGroup()['Value'])
    if not res['OK']:
      return res
    res = self._setRequestAttribute(requestID,'DIRACSetup',request.getDIRACSetup()['Value'])
    return res

  def _setRequestAttribute(self,requestID, attrName, attrValue):
    req = "UPDATE Requests SET %s='%s', LastUpdate = UTC_TIMESTAMP() WHERE RequestID='%s';" % (attrName,attrValue,requestID)
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

  def _setSubRequestAttribute(self,requestID, subRequestID, attrName, attrValue):
    req = "UPDATE SubRequests SET %s='%s', LastUpdate=UTC_TIMESTAMP() WHERE RequestID=%s AND SubRequestID=%s;" % (attrName,attrValue,requestID,subRequestID)
    res = self._update(req)
    if res['OK']:
      return res
    else:
      return S_ERROR('RequestDB.setRequestAttribute: failed to set attribute')

  def _setSubRequestLastUpdate(self,subRequestID):
    req = "UPDATE SubRequests SET LastUpdate=UTC_TIMESTAMP() WHERE  RequestID=%s AND SubRequestID='%s';" % (requestID,subRequestID)
    res = self._update(req)
    if res['OK']:
      return res
    else:
      return S_ERROR('RequestDB.setSubRequestLastUpdate: failed to set LastUpdate')

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
      self.getIdLock.release()
      err = 'RequestDB._getRequestID: Failed to get RequestID from RequestName'
      return S_ERROR( '%s\n%s' % (err, res['Message'] ) )
    if not len(res['Value']) == 0:
      self.getIdLock.release()
      err = 'RequestDB._getRequestID: Duplicate entry for RequestName'
      return S_ERROR(err)
    req = 'INSERT INTO Requests (RequestName,SubmissionTime) VALUES ("%s",UTC_TIMESTAMP());' % requestName
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
    req = 'INSERT INTO SubRequests (RequestID,RequestType,SubmissionTime) VALUES (%s,"%s",UTC_TIMESTAMP())' % (requestID,requestType)
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

  def getDigest(self,requestID):
    """ Get digest of the given request specified by its requestID
    """
    digest = ''
    digestStrings = []

    req = "SELECT RequestType,Operation,Status,ExecutionOrder,TargetSE,Catalogue,SubRequestID from SubRequests \
           WHERE RequestID=%d" % int(requestID)
    result = self._query(req)
    if not result['OK']:
      return result

    if not result['Value']:
      return S_OK('')

    for row in result['Value']:
      digestList = []
      digestList.append(row[0])
      digestList.append(row[1])
      digestList.append(row[2])
      digestList.append(str(row[3]))
      if row[0] == "transfer" or row[0] == "register":
        digestList.append(row[4])
      if row[0] == "register":
        digestList.append(row[5])
      subRequestID = int(row[6])
      req = "SELECT LFN from Files WHERE SubRequestID = %s ORDER BY FileID;" % subRequestID
      resFile = self._query(req)
      if resFile['OK']:
        if resFile['Value']:
          lfn = resFile['Value'][0][0]
          digestList.append(os.path.basename(lfn))

      digestStrings.append(":".join(digestList))

    digest = '\n'.join(digestStrings)
    return S_OK(digest)

  def getRequestStatus(self,requestID):
    """ Get status of the request and its subrequests
    """

    req = "SELECT Status from Requests WHERE RequestID=%d" % int(requestID)
    result = self._query(req)
    if not result['OK']:
      return result
    requestStatus = result['Value'][0][0]
    req = "SELECT Status from SubRequests WHERE RequestID=%d" % int(requestID)
    result = self._query(req)
    if not result['OK']:
      return result

    if not result['Value']:
      subrequestStatus = "Empty"
    else:
      subrequestStatus = "Done"
      for row in result['Value']:
        if row[0] == "Waiting":
          subrequestStatus = "Waiting"
        elif row[0] == "Failed" and subrequestStatus != "Waiting":
          subrequestStatus = "Failed"

    resDict = {}
    resDict['RequestStatus'] = requestStatus
    resDict['SubRequestStatus'] = subrequestStatus
    return S_OK(resDict)

  def getCurrentExecutionOrder(self,requestID):
    """ Get the current subrequest execution order for the given request
    """

    req = "SELECT Status,ExecutionOrder from SubRequests WHERE RequestID=%d" % int(requestID)
    result = self._query(req)
    if not result['OK']:
      return result

    if not result['Value']:
      return S_ERROR('No SubRequests found')

    current_order = 999
    for row in result['Value']:
      status,order = row
      if status == "Waiting" and order < current_order:
        current_order = order

    return S_OK(current_order)

  def __buildCondition(self, condDict, older=None, newer=None ):
    """ build SQL condition statement from provided condDict
        and other extra conditions
    """
    condition = ''
    conjunction = "WHERE"

    if condDict != None:
      for attrName, attrValue in condDict.items():
        if type(attrValue) == types.ListType:
          multiValue = ','.join(['"'+x.strip()+'"' for x in attrValue])
          condition = ' %s %s %s in (%s)' % ( condition,
                                             conjunction,
                                             str(attrName),
                                             multiValue  )
        else:
          condition = ' %s %s %s=\'%s\'' % ( condition,
                                             conjunction,
                                             str(attrName),
                                             str(attrValue)  )
        conjunction = "AND"

    if older:
      condition = ' %s %s LastUpdateTime < \'%s\'' % ( condition,
                                                 conjunction,
                                                 str(older) )
      conjunction = "AND"

    if newer:
      condition = ' %s %s LastUpdateTime >= \'%s\'' % ( condition,
                                                 conjunction,
                                                 str(newer) )

    return condition

  def getRequestSummaryWeb(self,selectDict, sortList, startItem, maxItems):
    """ Get summary of the requests in the database
    """

    resultDict = {}
    rparameterList = ['RequestID','RequestName','JobID','OwnerDN','OwnerGroup']
    sparameterList = ['RequestType','Status','Operation']
    parameterList = rparameterList + sparameterList

    req = "SELECT R.RequestID, R.RequestName, R.JobID, R.OwnerDN, R.OwnerGroup,"
    req += "S.RequestType, S.Status, S.Operation FROM Requests as R, SubRequests as S "

    new_selectDict = {}
    for key,value in selectDict.items():
      if key in rparameterList:
        new_selectDict['R.'+key] = value
      elif key in sparameterList:
        new_selectDict['S.'+key] = value

    condition = ''
    if new_selectDict:
      condition = self.__buildCondition(new_selectDict)
      req += condition

    if condition:
      req += " AND R.RequestID=S.RequestID"
    else:
      req += " WHERE R.RequestID=S.RequestID"

    if sortList:
      req += " ORDER BY %s %s" % (sortList[0][0],sortList[0][1])

    result = self._query(req)
    if not result['OK']:
      return result

    if not result['Value']:
      resultDict['ParameterNames'] = parameterList
      resultDict['Records'] = []
      return S_OK(resultDict)

    nRequests = len(result['Value'])

    if startItem <= len(result['Value']):
      firstIndex = startItem
    else:
      return S_ERROR('Requested index out of range')

    if (startItem + maxItems) <= len(result['Value']):
      secondIndex = startItem + maxItems
    else:
      secondIndex = len(result['Value'])

    records = []
    columnWidth = [ 0 for x in range(len(parameterList)) ]
    for i in range(firstIndex,secondIndex):
      row = result['Value'][i]
      records.append([ str(x) for x in row])
      for ind in range(len(row)):
        if len(str(row[ind])) > columnWidth[ind]:
          columnWidth[ind] = len(str(row[ind]))

    resultDict['ParameterNames'] = parameterList
    resultDict['ColumnWidths'] = columnWidth
    resultDict['Records'] = records
    resultDict['TotalRecords'] = nRequests

    return S_OK(resultDict)
