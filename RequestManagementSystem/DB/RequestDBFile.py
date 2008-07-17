""" RequestDBFile is the plug in for the file backend.
"""

from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.RequestManagementSystem.Client.RequestContainer import RequestContainer
from DIRAC.ConfigurationSystem.Client import PathFinder

import os
import threading,random
from types import *

class RequestDBFile:

  def __init__(self, systemInstance ='Default'):
    self.root = self.__getRequestDBPath()
    self.lastRequest = {}
    self.getIdLock = threading.Lock()

  def __getRequestDBPath(self):
    """ Obtain the root of the requestDB from the configuration
    """
    csSection = csSection = PathFinder.getServiceSection( "RequestManagement/RequestManager" )
    root = gConfig.getValue('%s/Path' % csSection)
    if not root:
      diracRoot = gConfig.getValue('/LocalSite/Root')
      root = diracRoot+'/requestdb'
    if not os.path.exists(root):
      os.makedirs(root)
    return root

  #######################################################################################
  #
  # These are the methods that expose the common functionality
  #

  def getDBSummary(self):
    """ Obtain a summary of the contents of the requestDB
    """
    gLogger.info("RequestDBFile._getDBSummary: Attempting to get database summary.")
    requestTypes = os.listdir(self.root)
    try:
      summaryDict = {}
      for requestType in requestTypes:
        summaryDict[requestType] = {}
        reqTypeDir = '%s/%s' % (self.root,requestType)
        if os.path.isdir(reqTypeDir):
          statusList = os.listdir(reqTypeDir)
          for status in statusList:
            reqTypeStatusDir = '%s/%s' % (reqTypeDir,status)
            requests = os.listdir(reqTypeStatusDir)
            summaryDict[requestType][status] = len(requests)
      gLogger.info("RequestDBFile._getDBSummary: Successfully obtained database summary.")
      return S_OK(summaryDict)
    except Exception,x:
      errStr = "RequestDBFile._getDBSummary: Exception while getting DB summary."
      gLogger.exception(errStr,str(x))
      return S_ERROR(errStr)

  def setRequest(self,requestName,requestString,desiredStatus=None):
    """ Set request to the database (including all sub-requests)
    """
    gLogger.info("RequestDBFile._setRequest: Attempting to set %s." % requestName)
    request = RequestContainer(requestString)
    requestTypes = request.getSubRequestTypes()['Value']
    try:
      for requestType in requestTypes:
        subRequestString = request.toXML(desiredType=requestType)['Value']
        if subRequestString:
          if desiredStatus:
            status = desiredStatus
          elif not request.isRequestTypeEmpty(requestType)['Value']:
            status = 'ToDo'
          else:
            status = 'Done'
          subRequestDir = '%s/%s/%s' % (self.root,requestType,status)
          if not os.path.exists(subRequestDir):
            os.makedirs(subRequestDir)
          subRequestPath = '%s/%s' % (subRequestDir,requestName)
          subRequestFile = open(subRequestPath,'w')
          subRequestFile.write(subRequestString)
          subRequestFile.close()
      gLogger.info("RequestDBFile._setRequest: Successfully set %s." % requestName)
      return S_OK()
    except Exception, x:
      errStr = "RequestDBFile._setRequest: Exception while setting request."
      gLogger.exception(errStr,"%s %s" % (requestName,str(x)))
      self.deleteRequest(requestName)
      return S_ERROR(errStr)

  def deleteRequest(self,requestName):
    """ Delete all sub requests associated to a request
    """
    gLogger.info("RequestDBFile._deleteRequest: Attempting to delete %s." % requestName)
    res = self.__locateRequest(requestName,assigned=True)
    if not res['OK']:
      gLogger.info("RequestDBFile._deleteRequest: Failed to delete %s." % requestName)
      return res
    subRequests = res['Value']
    try:
      for subRequest in subRequests:
        os.remove(subRequest)
      gLogger.info("RequestDBFile._deleteRequest: Successfully deleted %s." % requestName)
      return S_OK()
    except Exception, x:
      errStr = "RequestDBFile._deleteRequest: Exception while deleting request."
      gLogger.exception(errStr,"%s %s" % (requestName,str(x)))
      return S_ERROR(errStr)

  def getRequest(self,requestType):
    """ Obtain a request from the database of a certain type
    """
    gLogger.info("RequestDBFile._getRequest: Attempting to get %s type request." % requestType)
    try:
      # Determine the request name to be obtained
      candidateRequests = []
      reqDir = '%s/%s/ToDo' % (self.root,requestType)
      self.getIdLock.acquire()
      if os.path.exists(reqDir):
        requestNames = os.listdir(reqDir)
        for requestName in requestNames:
          requestPath = "%s/%s" % (reqDir,requestName)
          if os.path.isfile(requestPath):
            candidateRequests.append(requestName)
      if not len(candidateRequests) > 0:
        self.getIdLock.release()
        gLogger.info("RequestDBFile._getRequest: No request of type %s found." % requestType)
        return S_OK()

      # Select a request
      if not self.lastRequest.has_key(requestType):
        self.lastRequest[requestType] = ('',0)
      lastRequest,lastRequestIndex = self.lastRequest[requestType]
      res = self.__selectRequestCursor(candidateRequests,lastRequest,lastRequestIndex)
      if not res['OK']:
        self.getIdLock.release()
        errStr = "RequestDBFile._getRequest: Failed to get request cursor."
        gLogger.error(errStr,res['Message'])
        return S_ERROR(errStr)
      selectedRequestName,selectedRequestIndex = res['Value']

      # Obtain the string for the selected request
      res = self.__getRequestString(selectedRequestName)
      if not res['OK']:
        self.getIdLock.release()
        errStr = "RequestDBFile._getRequest: Failed to get request string for %s." % selectedRequestName
        gLogger.error(errStr,res['Message'])
        return S_ERROR(errStr)
      selectedRequestString = res['Value']

      # Set the request status to assigned
      res = self.setRequestStatus(selectedRequestName,'Assigned')
      if not res['OK']:
        self.getIdLock.release()
        errStr = "RequestDBFile._getRequest: Failed to set %s status to 'Assigned'." % selectedRequestName
        gLogger.error(errStr,res['Message'])
        return S_ERROR(errStr)

      # Update the request cursor and return the selected request
      self.lastRequest[requestType] = (selectedRequestName,selectedRequestIndex)
      self.getIdLock.release()
      gLogger.info("RequestDBFile._getRequest: Successfully obtained %s request." % selectedRequestName)
      resDict = {'RequestString':selectedRequestString,'RequestName':selectedRequestName}
      return S_OK(resDict)
    except Exception, x:
      errStr = "RequestDBFile._getRequest: Exception while getting request."
      gLogger.exception(errStr,"%s %s" % (requestType,str(x)))
      return S_ERROR(errStr)

  def setRequestStatus(self,requestName,requestStatus):
    """ Set the request status
    """
    gLogger.info("RequestDBFile._setRequestStatus: Attempting to set status of  %s to %s." % (requestName,requestStatus))
    try:
      # First obtain the request string
      res = self.__getRequestString(requestName)
      if not res['OK']:
        errStr = "RequestDBFile._setRequestStatus: Failed to get the request string for %s." %  requestName
        gLogger.error(errStr,res['Message'])
        return S_ERROR(errStr)
      requestString = res['Value']
      # Delete the original request
      res = self.deleteRequest(requestName)
      if not res['OK']:
        errStr = "RequestDBFile._setRequestStatus: Failed to remove %s." %  requestName
        gLogger.error(errStr,res['Message'])
        return S_ERROR(errStr)
      # Set the request with the desired status
      res = self.setRequest(requestName,requestString,desiredStatus=requestStatus)
      if not res['OK']:
        errStr = "RequestDBFile._setRequestStatus: Failed to update status of %s to %s." %  (requestName,requestStatus)
        gLogger.error(errStr,res['Message'])
        res = self.setRequest(requestName,requestString)
        return S_ERROR(errStr)
      gLogger.info("RequestDBFile._setRequestStatus: Successfully set status of %s to %s." % (requestName,requestStatus))
      return S_OK()
    except Exception, x:
      errStr = "RequestDBFile._setRequestStatus: Exception while setting request status."
      gLogger.exception(errStr,"%s %s" % (requestName,str(x)))
      return S_ERROR(errStr)

  def serveRequest(self,requestType):
    """ Get a request from the DB and serve it (delete locally)
    """
    gLogger.info("RequestDBFile.serveRequest: Attempting to serve request of type %s." % requestType)
    try:
      # get a request type if one is not specified
      if not requestType:
        res = self.getDBSummary()
        if not res['OK']:
          errStr = "RequestDBFile.serveRequest: Failed to get DB summary."
          gLogger.error(errStr,res['Message'])
          return S_ERROR(errStr)
        requestTypes = res['Value'].keys()
        if not requestTypes:
          # There are absolutely no requests in the db
          return S_OK()
        random.shuffle(requestTypes)
        requestType = requestTypes[0]

      # First get a request
      res = self.getRequest(requestType)
      if not res['OK']:
        errStr = "RequestDBFile.serveRequest: Failed to get request of type %s." %  requestType
        gLogger.error(errStr,res['Message'])
        return S_ERROR(errStr)
      if not res['Value']:
        return res
      requestDict = res['Value']
      requestName = requestDict['RequestName']
      # Delete the original request
      res = self.deleteRequest(requestName)
      if not res['OK']:
        errStr = "RequestDBFile.serveRequest: Failed to remove %s." %  requestName
        gLogger.error(errStr,res['Message'])
        return S_ERROR(errStr)
      gLogger.info("RequestDBFile.serveRequest: Successfully served %s." % requestName)
      return S_OK(requestDict)
    except Exception, x:
      errStr = "RequestDBFile.serveRequest: Exception while serving request."
      gLogger.exception(errStr,"%s %s" % (requestType,str(x)))
      return S_ERROR(errStr)

  def updateRequest(self,requestName,requestString):
    """ Update the contents of a pre-existing request
    """
    gLogger.info("RequestDBFile._updateRequest: Attempting to update request %s." % requestName)
    try:
      # Delete the original request
      res = self.deleteRequest(requestName)
      if not res['OK']:
        errStr = "RequestDBFile._updateRequest: Failed to remove %s." %  requestName
        gLogger.error(errStr,res['Message'])
        return S_ERROR(errStr)
      # Set the request string
      res = self.setRequest(requestName,requestString)
      if not res['OK']:
        errStr = "RequestDBFile._updateRequest: Failed to update %s." %  requestName
        gLogger.error(errStr,res['Message'])
        return S_ERROR(errStr)
      gLogger.info("RequestDBFile._updateRequest: Successfully updated %s." % requestName)
      return S_OK()
    except Exception, x:
      errStr = "RequestDBFile._updateRequest: Exception while updating request."
      gLogger.exception(errStr,"%s %s" % (requestName,str(x)))
      return S_ERROR(errStr)

  #######################################################################################
  #
  # These are the internal methods
  #

  def __locateRequest(self,requestName,assigned=False):
    """ Locate the sub requests associated with a requestName
    """
    gLogger.info("RequestDBFile.__locateRequest: Attempting to locate %s." % requestName)
    requestTypes = os.listdir(self.root)
    subRequests = []
    try:
      for requestType in requestTypes:
        reqDir = "%s/%s" % (self.root,requestType)
        if os.path.isdir(reqDir):
          statusList = os.listdir(reqDir)
          if not assigned and 'Assigned' in statusList:
            statusList.remove('Assigned')
          for status in statusList:
            statusDir = '%s/%s' % (reqDir,status)
            if os.path.isdir(statusDir):
              requestNames = os.listdir(statusDir)
              if requestName in requestNames:
                requestPath = '%s/%s' % (statusDir,requestName)
                subRequests.append(requestPath)
      gLogger.info("RequestDBFile.__locateRequest: Successfully located %s." % requestName)
      return S_OK(subRequests)
    except Exception, x:
      errStr = "RequestDBFile.__locateRequest: Exception while locating request."
      gLogger.exception(errStr,"%s %s" % (requestName,str(x)))
      return S_ERROR(errStr)

  def __getRequestString(self,requestName):
    """ Obtain the string for request (including all sub-requests)
    """
    gLogger.info("RequestDBFile.__getRequestString: Attempting to get string for %s." % requestName)
    res = self.__locateRequest(requestName)
    if not res['OK']:
      return res
    subRequestPaths = res['Value']
    try:
      oRequest = RequestContainer(init=False)
      for subRequestPath in subRequestPaths:
        res = self.__readSubRequestString(subRequestPath)
        if not res['OK']:
          return res
        subRequestString = res['Value']
        tempRequest = RequestContainer(subRequestString)#,init=False)
        oRequest.setRequestAttributes(tempRequest.getRequestAttributes()['Value'])
        oRequest.update(tempRequest)
      requestString = oRequest.toXML()['Value']
      gLogger.info("RequestDBFile.__getRequestString: Successfully obtained string for %s." % requestName)
      return S_OK(requestString)
    except Exception, x:
      errStr = "RequestDBFile.__getRequestString: Exception while obtaining request string."
      gLogger.exception(errStr,"%s %s" % (requestName,str(x)))
      return S_ERROR(errStr)

  def __readSubRequestString(self,subRequestPath):
    """ Read the contents of the supplied sub-request path
    """
    gLogger.info("RequestDBFile.__readSubRequestString: Attempting to read contents of %s." % subRequestPath)
    try:
      subRequestFile = open(subRequestPath,'r')
      requestString = subRequestFile.read()
      gLogger.info("RequestDBFile.__readSubRequestString: Successfully read contents of %s." % subRequestPath)
      return S_OK(str(requestString))
    except Exception, x:
      errStr = "RequestDBFile.__readSubRequestString: Exception while reading sub-request."
      gLogger.exception(errStr,"%s %s" % (subRequestPath,str(x)))
      return S_ERROR(errStr)

  def __selectRequestCursor(self,requestList,lastRequest,lastRequestIndex):
    """ Select the next valid request in the data base
    """
    gLogger.info("RequestDBFile.__selectRequestCursor: Attempting to select next valid request.")
    try:
      if lastRequest in requestList:
        lastIndex = requestList.index(lastRequest)
        newIndex = lastIndex+1
      elif lastRequestIndex:
        newIndex = lastRequestIndex+1
      else:
        newIndex = 0
      if newIndex >= len(requestList):
        newIndex = 0
      nextRequestName = requestList[newIndex]
      gLogger.info("RequestDBFile.__selectRequestCursor: Selected %s as next request." % nextRequestName)
      return S_OK((nextRequestName,newIndex))
    except Exception, x:
      errStr = "RequestDBFile.__selectRequestCursor: Exception while selecting next valid request."
      gLogger.exception(errStr,str(x))
      return S_ERROR(errStr)

