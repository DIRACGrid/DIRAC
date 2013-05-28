#############################################################################
# $HeadURL$
#############################################################################
""" :mod: RequestDBFile 
    ===================
 
    .. module: RequestDBFile
    :synopsis: RequestDBFile is the plug in for the file backend.
    :deprecated:
"""
## imports
import os, os.path
import threading
import random
## from DIRAC
from DIRAC import gLogger, gConfig, S_OK, S_ERROR, rootPath
from DIRAC.RequestManagementSystem.Client.RequestContainer import RequestContainer
from DIRAC.ConfigurationSystem.Client import PathFinder

## RCSID
__RCSID__ = "$Id"

class RequestDBFile( object ):
  """
  .. class:: RequestDBFile

  The fs based request backend interface.

  """
  def __init__( self, systemInstance = "Default" ):
    """ c'tor
    
    :param self: self reference
    :param str systemInstanace: system instance
    """
    self.root = self.__getRequestDBPath()
    self.lastRequest = {}
    self.getIdLock = threading.Lock()
    self.log = gLogger.getSubLogger( self.__class__.__name__ )

  @staticmethod
  def __getRequestDBPath():
    """ get the fs root path of the requestDB from the DIRAC configuration

    :warn: if root path doesn't exist, it will be created 
    """
    csSection = csSection = PathFinder.getServiceSection( "RequestManagement/RequestManager" )
    root = gConfig.getValue( '%s/Path' % csSection, 'requestdb' )
    diracRoot = gConfig.getValue( '/LocalSite/InstancePath', rootPath )
    # if the path return by the gConfig is absolute the following line does not change it,
    # otherwise it makes it relative to diracRoot
    root = os.path.join( diracRoot, root )
    if not os.path.exists( root ):
      os.makedirs( root )
    return root

  #######################################################################################
  #
  # These are the methods that expose the common functionality
  #

  def getDBSummary( self ):
    """ Obtain a summary of the contents of the requestDB

    :param self: self reference
    :return: S_OK with dict[requestType][status] => nb of files
    """
    self.log.info( "getDBSummary: Attempting to get database summary." )
    requestTypes = os.listdir( self.root )
    try:
      summaryDict = {}
      for requestType in requestTypes:
        summaryDict[requestType] = {}
        reqTypeDir = os.path.join( self.root, requestType )
        if os.path.isdir( reqTypeDir ):
          statusList = os.listdir( reqTypeDir )
          for status in statusList:
            reqTypeStatusDir = os.path.join( reqTypeDir, status )
            requests = os.listdir( reqTypeStatusDir )
            summaryDict[requestType][status] = len( requests )
      self.log.info( "getDBSummary: Successfully obtained database summary." )
      return S_OK( summaryDict )
    except Exception, x:
      errStr = "getDBSummary: Exception while getting DB summary."
      self.log.exception( errStr, lException = x )
      return S_ERROR( errStr )

  def setRequest( self, requestName, requestString, desiredStatus = None ):
    """ Set request to the database (including all sub-requests)

    :param self: self reference
    :param str requestName: request name
    :param str requestString: serilised request
    :param mixed desiredState: optional request status, defult = None
    """
    self.log.info( "setRequest: Attempting to set %s." % requestName )
    request = RequestContainer( requestString )
    requestTypes = request.getSubRequestTypes()['Value']
    try:
      for requestType in requestTypes:
        subRequestString = request.toXML( desiredType = requestType )['Value']
        if subRequestString:
          if desiredStatus:
            status = desiredStatus
          elif not request.isRequestTypeEmpty( requestType )['Value']:
            status = 'Waiting'
          else:
            status = 'Done'
          subRequestDir = os.path.join( self.root, requestType, status )
          if not os.path.exists( subRequestDir ):
            os.makedirs( subRequestDir )
          subRequestPath = os.path.join( subRequestDir, requestName )
          subRequestFile = open( subRequestPath, 'w' )
          subRequestFile.write( subRequestString )
          subRequestFile.close()
      self.log.info( "setRequest: Successfully set %s." % requestName )
      return S_OK()
    except Exception, error:
      errStr = "setRequest: Exception while setting request."
      self.log.exception( errStr, requestName, lException = error )
      self.deleteRequest( requestName )
      return S_ERROR( errStr )

  def deleteRequest( self, requestName ):
    """ Delete all sub requests associated to a request
    
    :param self: self reference
    :param str requestName: request name
    """
    self.log.info( "deleteRequest: Attempting to delete %s." % requestName )
    res = self.__locateRequest( requestName, assigned = True )
    if not res['OK']:
      self.log.info( "deleteRequest: Failed to delete %s." % requestName )
      return res
    subRequests = res['Value']
    try:
      for subRequest in subRequests:
        os.remove( subRequest )
      self.log.info( "deleteRequest: Successfully deleted %s." % requestName )
      return S_OK()
    except Exception, error:
      errStr = "deleteRequest: Exception while deleting request."
      self.log.exception( errStr, requestName, lException = error )
      return S_ERROR( errStr )

  def getRequest( self, requestType ):
    """ Obtain a request from the database of a :requestType: type
    
    :param self: self reference
    :param str requestType: request type
    """
    self.log.info( "getRequest: Attempting to get %s type request." % requestType )
    try:
      # Determine the request name to be obtained
      candidateRequests = []
      reqDir = os.path.join( self.root, requestType, "Waiting" )
      self.getIdLock.acquire()
      if os.path.exists( reqDir ):
        candidateRequests = [ os.path.basename( requestFile ) for requestFile in
                              sorted( filter( os.path.isfile,
                                              [ os.path.join( reqDir, requestName ) 
                                                for requestName in os.listdir( reqDir ) ] ),
                                      key = os.path.getctime ) ]
      if not len( candidateRequests ) > 0:
        self.getIdLock.release()
        self.log.info( "getRequest: No request of type %s found." % requestType )
        return S_OK()

      # Select a request
      if requestType not in self.lastRequest:
        self.lastRequest[requestType] = ( '', 0 )
      lastRequest, lastRequestIndex = self.lastRequest[requestType]
      res = self.__selectRequestCursor( candidateRequests, lastRequest, lastRequestIndex )
      if not res['OK']:
        self.getIdLock.release()
        errStr = "getRequest: Failed to get request cursor."
        self.log.error( errStr, res['Message'] )
        return S_ERROR( errStr )
      selectedRequestName, selectedRequestIndex = res['Value']

      # Obtain the string for the selected request
      res = self.__getRequestString( selectedRequestName )
      if not res['OK']:
        self.getIdLock.release()
        errStr = "getRequest: Failed to get request string for %s." % selectedRequestName
        self.log.error( errStr, res['Message'] )
        return S_ERROR( errStr )
      selectedRequestString = res['Value']

      # Set the request status to assigned
      res = self.setRequestStatus( selectedRequestName, 'Assigned' )
      if not res['OK']:
        self.getIdLock.release()
        errStr = "getRequest: Failed to set %s status to 'Assigned'." % selectedRequestName
        self.log.error( errStr, res['Message'] )
        return S_ERROR( errStr )

      # Update the request cursor and return the selected request
      self.lastRequest[requestType] = ( selectedRequestName, selectedRequestIndex )
      self.getIdLock.release()
      self.log.info( "getRequest: Successfully obtained %s request." % selectedRequestName )
      oRequest = RequestContainer( request = selectedRequestString )
      jobID = oRequest.getJobID()
      jobID = jobID["Value"] if jobID["OK"] and jobID["Value"] else 0
      try:
        jobID = int( jobID )
      except (TypeError, ValueError), error:
        self.log.error( "getRequest: could not get JobID from Request, setting it to 0: %s" % str(error) )
        jobID = 0
      return S_OK( { "RequestString" : selectedRequestString, "RequestName" : selectedRequestName, "JobID" : jobID } )
    except Exception, x:
      errStr = "getRequest: Exception while getting request."
      self.log.exception( errStr, requestType, lException = x )
      return S_ERROR( errStr )

  def setRequestStatus( self, requestName, requestStatus ):
    """ set the status for :requestName: to :requestStatus:  
    
    :param self: self reference
    :param str requestName: request name 
    :param str requestStatus: new status 
    """
    self.log.info( "setRequestStatus: Attempting to set status of  %s to %s." % ( requestName, requestStatus ) )
    try:
      # First obtain the request string
      res = self.__getRequestString( requestName )
      if not res['OK']:
        errStr = "setRequestStatus: Failed to get the request string for %s." % requestName
        self.log.error( errStr, res['Message'] )
        return S_ERROR( errStr )
      requestString = res['Value']
      # Delete the original request
      res = self.deleteRequest( requestName )
      if not res['OK']:
        errStr = "setRequestStatus: Failed to remove %s." % requestName
        self.log.error( errStr, res['Message'] )
        return S_ERROR( errStr )
      # Set the request with the desired status
      res = self.setRequest( requestName, requestString, desiredStatus = requestStatus )
      if not res['OK']:
        errStr = "setRequestStatus: Failed to update status of %s to %s." % ( requestName, requestStatus )
        self.log.error( errStr, res['Message'] )
        res = self.setRequest( requestName, requestString )
        return S_ERROR( errStr )
      self.log.info( "setRequestStatus: Successfully set status of %s to %s." % ( requestName, requestStatus ) )
      return S_OK()
    except Exception, error:
      errStr = "setRequestStatus: Exception while setting request status."
      self.log.exception( errStr, requestName, lException = error )
      return S_ERROR( errStr )

  def getRequestStatus( self, requestName ):
    """ check status for request :requestName:
    
    :param self: self reference
    :param str requestName: 
    """
    res = self.__locateRequest( requestName, assigned=True )
    if not res["OK"]:
      return res
    subRequestPaths = res['Value']
    if not subRequestPaths:
      return S_ERROR( "getRequestStatus: request '%s' not found" % requestName )
    ## figure out subrequests status
    result = list( set( [ path.rstrip( requestName ).split("/")[-2] for path in subRequestPaths ] ) )      
    subRequestStatus = "Unknown"
    if "Empty" in result:
      subRequestStatus = "Empty"
    elif "Waiting" in result:
      subRequestStatus = "Waiting"
    elif "Assigned" in result:
      subRequestStatus = "Assigned"
    elif "Failed" in result:
      subRequestStatus = "Failed"
    elif "Done" in result:
      subRequestStatus = "Done"
    ## ...and same for request status
    if subRequestStatus in ( "Waiting", "Assigned", "Unknown" ):
      requestStatus = "Waiting"
    elif subRequestStatus in ( "Empty", "Failed", "Done" ):
      requestStatus = "Done"

    return S_OK( { "RequestStatus" : requestStatus, "SubRequestStatus" : subRequestStatus }  )

  def serveRequest( self, requestType ):
    """ Get a request from the DB and serve it (delete locally)

    :param self: self reference
    :param str requestType: request type
    """
    self.log.info( "serveRequest: Attempting to serve request of type %s." % requestType )
    try:
      # get a request type if one is not specified
      if not requestType:
        res = self.getDBSummary()
        if not res['OK']:
          errStr = "serveRequest: Failed to get DB summary."
          self.log.error( errStr, res['Message'] )
          return S_ERROR( errStr )
        candidates = []
        for requestType, requestsCount in res["Value"].items():
          if "Waiting" in requestsCount:
            count = requestsCount["Waiting"]
            if count:
              candidates.append( requestType )
        if not candidates:
          # There are absolutely no requests in the db
          return S_OK()
        random.shuffle( candidates )
        requestType = candidates[0]

      # First get a request
      res = self.getRequest( requestType )
      if not res['OK']:
        errStr = "serveRequest: Failed to get request of type %s." % requestType
        self.log.error( errStr, res['Message'] )
        return S_ERROR( errStr )
      if not res['Value']:
        return res
      requestDict = res['Value']
      requestName = requestDict['RequestName']
      # Delete the original request
      res = self.deleteRequest( requestName )
      if not res['OK']:
        errStr = "serveRequest: Failed to remove %s." % requestName
        self.log.error( errStr, res['Message'] )
        return S_ERROR( errStr )
      self.log.info( "serveRequest: Successfully served %s." % requestName )
      return S_OK( requestDict )
    except Exception, error:
      errStr = "serveRequest: Exception while serving request."
      self.log.exception( errStr, requestType, error )
      return S_ERROR( errStr )

  def updateRequest( self, requestName, requestString ):
    """ update the contents of a pre-existing request, set the request's status


    :param self: self reference
    :param str requestName: request name
    :param str requestString: serialised request
    """
    self.log.info( "updateRequest: Attempting to update request %s." % requestName )
    try:
      # Delete the original request
      res = self.deleteRequest( requestName )
      if not res['OK']:
        errStr = "updateRequest: Failed to remove %s." % requestName
        self.log.error( errStr, res['Message'] )
        return S_ERROR( errStr )
      # Set the request string
      res = self.setRequest( requestName, requestString )
      if not res['OK']:
        errStr = "updateRequest: Failed to update %s." % requestName
        self.log.error( errStr, res['Message'] )
        return S_ERROR( errStr )
      self.log.info( "updateRequest: Successfully updated %s." % requestName )

      requestStatus = self.getRequestStatus( requestName )
      if not requestStatus["OK"]:
        self.log.error( "updateRequest: unable to get request status %s: %s" % ( requestName, 
                                                                                 requestStatus["Message"] ) )
        return requestStatus

      requestStatus = requestStatus["Value"]
      subRequestStatus = requestStatus["SubRequestStatus"]
      if subRequestStatus in ( "Empty", "Done", "Failed" ):
        setRequestStatus = self.setRequestStatus( requestName, "Done" )
        if not setRequestStatus["OK"]:
          self.log.error( "updateRequest: unable to set status to 'Done' for %s: %s" % ( requestName, 
                                                                                         setRequestStatus["Message"] ) )
          return setRequestStatus
      return S_OK()
    except Exception, error:
      errStr = "updateRequest: Exception while updating request."
      self.log.exception( errStr, requestName, lException = error )
      return S_ERROR( errStr )

  @staticmethod
  def getCurrentExecutionOrder( requestID ):
    """ Get the current subrequest execution order for the given request,
        fake method to satisfy the standard interface
        
        What??? O'rly? 

    """
    return S_OK( 0 )

  #######################################################################################
  #
  # These are the internal methods
  #

  def __locateRequest( self, requestName, assigned = False ):
    """ Locate the sub requests associated with a requestName

    :param self: self reference
    :param str requestName: request name
    :param bool assigned: flag to include/exclude Assigned requests
    """
    self.log.info( "__locateRequest: Attempting to locate %s." % requestName )
    requestTypes = os.listdir( self.root )
    subRequests = []
    try:
      for requestType in requestTypes:
        reqDir = "%s/%s" % ( self.root, requestType )
        if os.path.isdir( reqDir ):
          statusList = os.listdir( reqDir )
          if not assigned and 'Assigned' in statusList:
            statusList.remove( 'Assigned' )
          for status in statusList:
            statusDir = os.path.join( reqDir, status )
            if os.path.isdir( statusDir ):
              requestNames = os.listdir( statusDir )
              if requestName in requestNames:
                requestPath = os.path.join( statusDir, requestName )
                subRequests.append( requestPath )
      self.log.info( "__locateRequest: Successfully located %s." % requestName )
      return S_OK( subRequests )
    except Exception, error:
      errStr = "__locateRequest: Exception while locating request."
      self.log.exception( errStr, requestName, lException = error )
      return S_ERROR( errStr )

  def __getRequestString( self, requestName ):
    """ Obtain the string for request (including all sub-requests)

    :param self: self reference
    :param str requestName: request name
    """
    self.log.info( "__getRequestString: Attempting to get string for %s." % requestName )
    res = self.__locateRequest( requestName )
    if not res['OK']:
      return res
    subRequestPaths = res['Value']
    try:
      oRequest = RequestContainer( init = False )
      for subRequestPath in subRequestPaths:
        res = self.__readSubRequestString( subRequestPath )
        if not res['OK']:
          return res
        subRequestString = res['Value']
        tempRequest = RequestContainer( subRequestString )
        oRequest.setRequestAttributes( tempRequest.getRequestAttributes()['Value'] )
        oRequest.update( tempRequest )
      requestString = oRequest.toXML()['Value']
      self.log.info( "__getRequestString: Successfully obtained string for %s." % requestName )
      result = S_OK( requestString )
      result['Request'] = oRequest
      return result
    except Exception, error:
      errStr = "__getRequestString: Exception while obtaining request string."
      self.log.exception( errStr, requestName, lException = error )
      return S_ERROR( errStr )

  def _getRequestAttribute( self, attribute, requestName ):
    """ Get attribute of request specified by :requestName:

    :param self: self reference
    :param str attribute: attribute name
    :param str requestName: request name

    TODO: KeyError??? Not AttributeError???

    """
    result = self.__getRequestString( requestName )
    if not result['OK']:
      return result
    request = result['Request']
    try:
      return request.getAttribute( attribute )
    except KeyError:
      return S_OK( None )

  def __readSubRequestString( self, subRequestPath ):
    """ Read the contents of the supplied sub-request path
    
    :param self: self reference
    :param str subRequestPath: abs path to file containing serilised sub-request 
    """
    self.log.info( "__readSubRequestString: Attempting to read contents of %s." % subRequestPath )
    try:
      subRequestFile = open( subRequestPath, 'r' )
      requestString = subRequestFile.read()
      self.log.info( "__readSubRequestString: Successfully read contents of %s." % subRequestPath )
      return S_OK( str( requestString ) )
    except Exception, error:
      errStr = "__readSubRequestString: Exception while reading sub-request."
      self.log.exception( errStr, subRequestPath, lException = error )
      return S_ERROR( errStr )

  def __selectRequestCursor( self, requestList, lastRequest, lastRequestIndex ):
    """ Select the next valid request in the data base

    :param self: self reference
    :param list requestList: request list (list containg paths to files???)
    :param str lastRequest: last serverd request
    :param int lastRequestIndex: index of :lastRequest: in :requestList: 
    """
    self.log.info( "__selectRequestCursor: Attempting to select next valid request." )
    try:
      if lastRequest in requestList:
        lastIndex = requestList.index( lastRequest )
        newIndex = lastIndex + 1
      elif lastRequestIndex:
        newIndex = lastRequestIndex + 1
      else:
        newIndex = 0
      if newIndex >= len( requestList ):
        newIndex = 0
      nextRequestName = requestList[newIndex]
      self.log.info( "__selectRequestCursor: Selected %s as next request." % nextRequestName )
      return S_OK( ( nextRequestName, newIndex ) )
    except Exception, error:
      errStr = "__selectRequestCursor: Exception while selecting next valid request."
      self.log.exception( errStr, lException = error )
      return S_ERROR( errStr )
