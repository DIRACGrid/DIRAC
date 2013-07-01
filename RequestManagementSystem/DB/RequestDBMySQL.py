# $HeadURL$

""" RequestDBMySQL is the MySQL plug in for the request DB
    :deprecated:
"""

__RCSID__ = "$Id$"

from DIRAC.Core.Base.DB import DB
from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Utilities.List import intListToString, stringListToString
from DIRAC.RequestManagementSystem.Client.RequestContainer import RequestContainer

import os
import threading
import types
import random, time

class RequestDBMySQL( DB ):
  """
  .. class:: RequestDBmySQL 

  An interface to mysql RequestDB database. 
  """

  def __init__( self, systemInstance = 'Default', maxQueueSize = 10 ):
    """ c'tor
    
    :param self: self reference
    :param str systemInstance: ??? 
    :param int maxQueueSize: queue size    
    """
    DB.__init__( self, 'RequestDB', 'RequestManagement/RequestDB', maxQueueSize )
    self.getIdLock = threading.Lock()

  def _setRequestStatus( self, requestType, requestName, status ):
    """ set request Status to :status:

    :param self: self reference
    :param str requestType: request type
    :param str requestName: requets name
    :param str status: new status
    """
    attrName = 'RequestID'
    res = self._getRequestAttribute( attrName, requestName = requestName )
    if not res['OK']:
      return res
    requestID = res['Value']
    attrName = 'Status'
    attrValue = status
    res = self._setRequestAttribute( requestID, attrName, attrValue )
    return res

  def _getSubRequests( self, requestID ):
    """ Get subrequest IDs for the given request

    :param self: self reference
    :param int requestID: Requests.RequestID 
    :return: list with SubRequestIDs
    """
    subRequestList = []
    req = "SELECT `SubRequestID` FROM `SubRequests` WHERE `RequestID`=%d" % requestID
    result = self._query( req )
    if not result['OK']:
      return result

    if result['Value']:
      subRequestList = [ int( x[0] ) for x in result['Value']]

    return S_OK( subRequestList )

  def setRequestStatus( self, requestName, requestStatus, subRequest_flag = True ):
    """ Set request status and optionally subrequest status

    :param str requestName: Requests.RequestName
    :param str requestStatus: new value for Requests.Status
    :param bool subReuquest_flag: trigger update of SubRequests.Status at well
    """
    res = self._getRequestAttribute( 'RequestID', requestName = requestName )
    if not res['OK']:
      return res
    requestID = res['Value']
    res = self._setRequestAttribute( requestID, 'Status', requestStatus )
    if not res['OK']:
      return res

    if not subRequest_flag:
      return S_OK()

    result = self._getSubRequests( requestID )
    if result['OK']:
      for subRequestID in result['Value']:
        res = self._setSubRequestAttribute( requestID, subRequestID, 'Status', requestStatus )

    return S_OK()

  def __buildCondition( self, condDict, older = None, newer = None ):
    """ build SQL condition statement from provided condDict
        and other extra conditions
    """
    condition = ''
    conjunction = "WHERE"
    if condDict != None:
      for attrName, attrValue in condDict.items():
        if type( attrValue ) == types.ListType:
          multiValue = ','.join( ['"' + x.strip() + '"' for x in attrValue] )
          condition = ' %s %s %s in (%s)' % ( condition,
                                             conjunction,
                                             str( attrName ),
                                             multiValue )
        else:
          condition = ' %s %s %s=\'%s\'' % ( condition,
                                             conjunction,
                                             str( attrName ),
                                             str( attrValue ) )
        conjunction = "AND"

    if older:
      condition = ' %s %s S.LastUpdate < \'%s\'' % ( condition,
                                                 conjunction,
                                                 str( older ) )
      conjunction = "AND"

    if newer:
      condition = ' %s %s S.LastUpdate >= \'%s\'' % ( condition,
                                                 conjunction,
                                                 str( newer ) )

    return condition

  def selectRequests( self, selectDict, limit = 100 ):
    """ Select requests according to specified criteria
    """
    condDict = {}
    older = None
    newer = None
    for key, value in selectDict.items():
      if key == 'ToDate':
        older = value
      elif key == 'FromDate':
        newer = value
      else:
        condDict[key] = value
    result = self.getFields( 'Requests', ['RequestID', 'RequestName'], condDict = condDict, limit = limit,
                    older = older, newer = newer, timeStamp = 'LastUpdate' )

    if not result['OK']:
      return result

    requestIDs = dict( [ ( x[0], x[1] ) for x in result['Value'] ] )
    return S_OK( requestIDs )

  def _serveRequest( self ):
    self.getIdLock.acquire()
    req = "SELECT MAX(RequestID) FROM Requests;"
    res = self._query( req )
    if not res['OK']:
      err = 'RequestDB._serveRequest: Failed to retrieve max RequestID'
      self.getIdLock.release()
      return S_ERROR( '%s\n%s' % ( err, res['Message'] ) )
    requestID = res['Value'][0][0]
    #_getRequest(
    #_removeRequest(
    req = "SELECT * from SubRequests WHERE RequestID=%s" % requestID
    res = self._query( req )
    if not res['OK']:
      err = 'RequestDB._serveRequest: Failed to retrieve SubRequest IDs for RequestID %s' % requestID
      self.getIdLock.release()
      return S_ERROR( '%s\n%s' % ( err, res['Message'] ) )
    subRequestIDs = []
    for subRequestID in res['Value']:
      subRequestIDs.append( subRequestID[0] )
    req = "SELECT * from SubRequests WHERE RequestID IN =%s" % requestID
    self.getIdLock.release()
    #THIS IS WHERE I AM IN THIS METHOD
    # STILL TO DO: compile the request string
    # STILL TO DO: remove the request completely from the db
    # STILL TO DO: return the request string
    # apparently lost in space or MIA

  def getDBSummary( self ):
    """ get the summary of the Request DB contents
    
    TODO: needs refactoring, all information can be read in one go
    """
    summaryDict = {}
    req = "SELECT DISTINCT(`RequestType`) FROM `SubRequests`;"
    result = self._query( req )
    if not result['OK']:
      return S_ERROR( 'RequestDBMySQL.getDBSummary: Failed to retrieve request info' )
    typeList = []
    for row in result['Value']:
      typeList.append( row[0] )

    req = "SELECT DISTINCT(`Status`) FROM `SubRequests`;"
    result = self._query( req )
    if not result['OK']:
      return S_ERROR( 'RequestDBMySQL.getDBSummary: Failed to retrieve request info' )
    statusList = []
    for row in result['Value']:
      statusList.append( row[0] )

    if not typeList:
      return S_OK( summaryDict )

    for rtype in typeList:
      summaryDict[rtype] = {}
      for status in statusList:
        req = "SELECT COUNT(*) FROM `SubRequests` WHERE `RequestType`='%s' AND `Status`='%s'" % ( rtype, status )
        result = self._query( req )
        if not result['OK']:
          summaryDict[rtype][status] = 0
        elif not result['Value']:
          summaryDict[rtype][status] = 0
        else:
          summaryDict[rtype][status] = int( result['Value'][0][0] )

    return S_OK( summaryDict )

  def getRequestFileStatus( self, requestID, files ):
    """ collect and return all file statuses given :requestID:

    :param int requestID: Requests.RequestID
    :param list files: [ Files.LFN ]
    """
    req = "SELECT DISTINCT `SubRequestID` FROM `SubRequests` WHERE `RequestID`=%d;" % requestID
    res = self._query( req )
    if not res['OK']:
      return res
    subRequests = []
    for subRequestID in res['Value'][0]:
      subRequests.append( subRequestID )
    req = "SELECT `LFN`,`Status` FROM `Files` WHERE `SubRequestID` IN (%s) AND `LFN` in (%s);" %\
        ( intListToString( subRequests ), stringListToString( files ) )
    res = self._query( req )
    if not res['OK']:
      return res
    files = {}
    for lfn, status in res['Value']:
      files[lfn] = status
    return S_OK( files )

  def getRequest( self, requestType ):
    """ Get a request of a given type eligible for execution
    """
    # RG: What if requestType is not given?
    # the first query will return nothing.
    # KC: maybe returning S_ERROR would be enough?
    # alternatively we should check if requestType is known (in 'transfer', 'removal', 'register' and 'diset') 

    if not requestType or type( requestType ) not in types.StringTypes:
      return S_ERROR( "Request type not given." )

    myRequestType = self._escapeString( requestType )
    if not myRequestType:
      return myRequestType

    myRequestType = myRequestType['Value']

    start = time.time()
    dmRequest = RequestContainer( init = False )
    requestID = 0
    subIDList = []

    fields = ['RequestID', 'SubRequestID', 'Operation', 'Arguments',
              'ExecutionOrder', 'SourceSE', 'TargetSE', 'Catalogue',
              'CreationTime', 'SubmissionTime', 'LastUpdate', 'Status', 'RequestType']
    # get the pending SubRequest sorted by ExecutionOrder and LastUpdate
    req = "SELECT `RequestID`,`ExecutionOrder`,`Status`,`RequestType`,`LastUpdate` FROM `SubRequests` "\
        "WHERE `Status` IN ( 'Waiting', 'Assigned' ) ORDER BY `ExecutionOrder`,`LastUpdate`"
    # now get sorted list of RequestID (according to the above)
    req = "SELECT * FROM ( %s ) as T1 GROUP BY `RequestID`" % req
    # and get the 100 oldest ones of Type requestType
    req = "SELECT `RequestID`,`ExecutionOrder` FROM ( %s ) as T2 WHERE `RequestType`=%s "\
        "ORDER BY `LastUpdate` LIMIT 100" % ( req, myRequestType )
    # and now get all waiting SubRequest for the selected RequestID and ExecutionOrder 
    req = "SELECT A.%s FROM SubRequests AS A, ( %s ) AS B WHERE " % ( ', A.'.join( fields ), req )
    req = "%s A.RequestID=B.RequestID AND A.ExecutionOrder=B.ExecutionOrder" % ( req )

    result = self._query( req )
    if not result['OK']:
      err = 'RequestDB._getRequest: Failed to retrieve Requests'
      return S_ERROR( '%s\n%s' % ( err, result['Message'] ) )
    if not result['Value']:
      return S_OK()

    # We get up to 10 Request candidates, to add some randomness 
    reqDict = {}
    for row in result['Value']:
      if ('"%s"' % row[-1]) != myRequestType:
        continue
      if row[-2] != 'Waiting':
        continue
      reqDict.setdefault( row[0], [] )
      reqDict[row[0]].append( row[1:-2] )

    reqIDList = reqDict.keys()
    random.shuffle( reqIDList )

    for reqID in reqIDList:
      sidList = [ x[0] for x in reqDict[reqID] ]
      for subID in sidList:
        req = "UPDATE SubRequests SET Status='Assigned' WHERE RequestID=%s AND SubRequestID=%s;" % ( reqID, subID )
        resAssigned = self._update( req )
        if not resAssigned['OK']:
          if subIDList:
            self.__releaseSubRequests( reqID, subIDList )
          return S_ERROR( 'Failed to assign subrequests: %s' % resAssigned['Message'] )
        if resAssigned['Value'] == 0:
          # Somebody has assigned this request
          gLogger.warn( 'Already assigned subrequest %d of request %d' % ( subID, reqID ) )
        else:
          subIDList.append( subID )
      if subIDList:
        # We managed to get some requests, can continue now
        requestID = reqID

        break
    # Haven't succeeded to get any request        
    if not requestID:
      return S_OK()

    dmRequest.setRequestID( requestID )

    fields = ['FileID', 'LFN', 'Size', 'PFN', 'GUID', 'Md5', 'Addler', 'Attempt', 'Status' ]
    for subRequestID, operation, arguments, executionOrder, sourceSE, targetSE, catalogue, \
          creationTime, submissionTime, lastUpdate in reqDict[requestID]:
      if not subRequestID in subIDList: continue
      res = dmRequest.initiateSubRequest( requestType )
      ind = res['Value']
      subRequestDict = {
                        'Status'        : 'Waiting',
                        'SubRequestID'  : subRequestID,
                        'Operation'     : operation,
                        'Arguments'     : arguments,
                        'ExecutionOrder': int( executionOrder ),
                        'SourceSE'      : sourceSE,
                        'TargetSE'      : targetSE,
                        'Catalogue'     : catalogue,
                        'CreationTime'  : creationTime,
                        'SubmissionTime': submissionTime,
                        'LastUpdate'    : lastUpdate
                       }
      res = dmRequest.setSubRequestAttributes( ind, requestType, subRequestDict )
      if not res['OK']:
        err = 'RequestDB._getRequest: Failed to set subRequest attributes for RequestID %s' % requestID
        self.__releaseSubRequests( requestID, subIDList )
        return S_ERROR( '%s\n%s' % ( err, res['Message'] ) )

      req = "SELECT %s FROM `Files` WHERE `SubRequestID`=%s ORDER BY `FileID`;" % ( ', '.join( fields ), 
                                                                                    subRequestID )
      res = self._query( req )
      if not res['OK']:
        err = 'RequestDB._getRequest: Failed to get File attributes for RequestID %s.%s' % ( requestID, 
                                                                                             subRequestID )
        self.__releaseSubRequests( requestID, subIDList )
        return S_ERROR( '%s\n%s' % ( err, res['Message'] ) )
      files = []
      for fileID, lfn, size, pfn, guid, md5, addler, attempt, status in res['Value']:
        fileDict = {'FileID':fileID, 'LFN':lfn, 'Size':size, 'PFN':pfn, 'GUID':guid, 
                    'Md5':md5, 'Addler':addler, 'Attempt':attempt, 'Status':status}
        files.append( fileDict )
      res = dmRequest.setSubRequestFiles( ind, requestType, files )
      if not res['OK']:
        err = 'RequestDB._getRequest: Failed to set files into Request for RequestID %s.%s' % ( requestID, 
                                                                                                subRequestID )
        self.__releaseSubRequests( requestID, subIDList )
        return S_ERROR( '%s\n%s' % ( err, res['Message'] ) )

      req = "SELECT Dataset,Status FROM Datasets WHERE SubRequestID = %s;" % subRequestID
      res = self._query( req )
      if not res['OK']:
        err = 'RequestDB._getRequest: Failed to get Datasets for RequestID %s.%s' % ( requestID, subRequestID )
        self.__releaseSubRequests( requestID, subIDList )
        return S_ERROR( '%s\n%s' % ( err, res['Message'] ) )
      datasets = []
      for dataset, status in res['Value']:
        datasets.append( dataset )
      res = dmRequest.setSubRequestDatasets( ind, requestType, datasets )
      if not res['OK']:
        err = 'RequestDB._getRequest: Failed to set datasets into Request for RequestID %s.%s' % ( requestID, 
                                                                                                   subRequestID )
        self.__releaseSubRequests( requestID, subIDList )
        return S_ERROR( '%s\n%s' % ( err, res['Message'] ) )

    fields = ['RequestName', 'JobID', 'OwnerDN', 'OwnerGroup',
              'DIRACSetup', 'SourceComponent', 'CreationTime',
              'SubmissionTime', 'LastUpdate']

    req = "SELECT %s FROM `Requests` WHERE `RequestID`=%s;" % ( ', '.join( fields ), requestID )
    res = self._query( req )
    if not res['OK']:
      err = 'RequestDB._getRequest: Failed to retrieve max RequestID'
      self.__releaseSubRequests( requestID, subIDList )
      return S_ERROR( '%s\n%s' % ( err, res['Message'] ) )
    requestName, jobID, ownerDN, ownerGroup, diracSetup, sourceComponent, \
        creationTime, submissionTime, lastUpdate = res['Value'][0]
    dmRequest.setRequestName( requestName )
    dmRequest.setJobID( jobID )
    dmRequest.setOwnerDN( ownerDN )
    dmRequest.setOwnerGroup( ownerGroup )
    dmRequest.setDIRACSetup( diracSetup )
    dmRequest.setSourceComponent( sourceComponent )
    dmRequest.setCreationTime( str( creationTime ) )
    dmRequest.setLastUpdate( str( lastUpdate ) )
    res = dmRequest.toXML()
    if not res['OK']:
      err = 'RequestDB._getRequest: Failed to create XML for RequestID %s' % ( requestID )
      self.__releaseSubRequests( requestID, subIDList )
      return S_ERROR( '%s\n%s' % ( err, res['Message'] ) )
    requestString = res['Value']
    #still have to manage the status of the dataset properly
    resultDict = {}
    resultDict['RequestName'] = requestName
    resultDict['RequestString'] = requestString
    resultDict['JobID'] = jobID
    return S_OK( resultDict )

  def __releaseSubRequests( self, requestID, subRequestIDs ):
    """ set SubRequests.Status from :Assigned: to :Waiting:

    :param int requestID: Requests.RequestID
    :param list subRequestIDs: [ SubRequests.SubRequestID, ... ]
    """

    for subRequestID in subRequestIDs:
      res = self._setSubRequestAttribute( requestID, subRequestID, 'Status', 'Waiting' )

  def setRequest( self, requestName, requestString ):
    """ insert request :requestName: to teh db
    
    :param str requestName: Requests.RequestName
    :param str requestString: xml-serialised request
    """
    request = RequestContainer( init = True, request = requestString )
    requestTypes = request.getSubRequestTypes()['Value']
    failed = False
    res = self._getRequestID( requestName )
    if not res['OK']:
      # we have a special case here: if request already exists, we override it if it
      # comes from a DIRAC job. This is identified by having a meaningful JobID in
      # the request
      if res['Message'].find( 'Duplicate' ) != -1:
        # Duplicate request
        jobID = request.getJobID()['Value']
        if jobID == "Unknown":
          return res
        try:
          jobID = int( jobID )
        except:
          return res
        if jobID > 0:
          # Remove the existing request
          result = self._deleteRequest( requestName )
          if not result['OK']:
            message = res['Message']
            return S_ERROR( 'Failed to set request: ' + message + ' can not override' )
          res = self._getRequestID( requestName )
          if not res['OK']:
            return res
      else:
        return res
    requestID = res['Value']
    subRequestIDs = {}
    res = self.__setRequestAttributes( requestID, request )
    if res['OK']:
      for requestType in requestTypes:
        res = request.getNumSubRequests( requestType )
        numRequests = res['Value']
        for ind in range( numRequests ):
          res = self._getSubRequestID( requestID, requestType )
          if res['OK']:
            subRequestID = res['Value']
            res = self.__setSubRequestAttributes( requestID, ind, requestType, subRequestID, request )
            if res['OK']:
              subRequestIDs[subRequestID] = res['Value']
              res = self.__setSubRequestFiles( ind, requestType, subRequestID, request )
              if res['OK']:
                res = self.__setSubRequestDatasets( ind, requestType, subRequestID, request )
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
    for subRequestID, status in subRequestIDs.items():
      if not status:
        status = "Waiting"
      res = self._setSubRequestAttribute( requestID, subRequestID, 'Status', status )
      if not res['OK']:
        failed = True
        message = res['Message']
    res = self._setRequestAttribute( requestID, 'Status', 'Waiting' )
    if not res['OK']:
      failed = True
      message = res['Message']
    if failed:
      res = self._deleteRequest( requestName )
      return S_ERROR( 'Failed to set request: ' + message )
    else:
      return S_OK( requestID )

  def updateRequest( self, requestName, requestString ):
    """ update request given its name and xml serilised string

    :param str requestName: Requests.RequestName
    :param str requestString: request serilised to xml
    """
    request = RequestContainer( request = requestString )
    requestTypes = ['transfer', 'register', 'removal', 'stage', 'diset', 'logupload']
    requestID = request.getRequestID()['Value']
    updateRequestFailed = False
    for requestType in requestTypes:
      res = request.getNumSubRequests( requestType )
      if res['OK']:
        numRequests = res['Value']
        for ind in range( numRequests ):
          res = request.getSubRequestAttributes( ind, requestType )
          if res['OK']:
            subRequestDict = res['Value']
            if 'SubRequestID' in subRequestDict:
              subRequestID = res['Value']['SubRequestID']
              res = self.__updateSubRequestFiles( ind, requestType, subRequestID, request )
              if res['OK']:
                if request.isSubRequestDone( ind, requestType )['Value']:
                  res = self._setSubRequestAttribute( requestID, subRequestID, 'Status', 'Done' )
                else:
                  res = self._setSubRequestAttribute( requestID, subRequestID, 'Status', 'Waiting' )
                if not res['OK']:
                  updateRequestFailed = True
              else:
                updateRequestFailed = True
              if "Error" in subRequestDict:
                result = self._setSubRequestAttribute( requestID, subRequestID, 
                                                       'Error', subRequestDict['Error'] )
                if not result['OK']:
                  updateRequestFailed = True
            else:
              updateRequestFailed = True
          else:
            updateRequestFailed = True
      else:
        updateRequestFailed = True
    if updateRequestFailed:
      errStr = 'Failed to update request %s.' % requestID
      return S_ERROR( errStr )
    else:
      requestStatus = self.getRequestStatus( requestID )
      if not requestStatus["OK"]:
        return requestStatus
      requestStatus = requestStatus["Value"]
      if requestStatus["SubRequestStatus"] not in ( "Waiting", "Assigned" ):
        res = self._setRequestAttribute( requestID, 'Status', 'Done' )
        if not res['OK']:
          errStr = 'Failed to update request status of %s to Done.' % requestID
          return S_ERROR( errStr )

      return S_OK()

  def deleteRequest( self, requestName ):
    """ delete request of name :requestName: from db

    :param self: self reference
    :param str requestName: request name
    """
    return self._deleteRequest( requestName )

  def _deleteRequest( self, requestName ):
    """ nothig more than above 
    
    :param str requestName: Requests.RequestName
    """
    #This method needs extended to truely remove everything that is being removed i.e.fts job entries etc.
    failed = False
    req = "SELECT `RequestID` FROM `Requests` WHERE `RequestName`='%s';" % requestName
    res = self._query( req )
    if res['OK']:
      if res['Value']:
        requestID = res['Value'][0]
        req = "SELECT `SubRequestID` FROM `SubRequests` WHERE `RequestID`=%s;" % requestID
        res = self._query( req )
        if res['OK']:
          subRequestIDs = []
          for reqID in res['Value']:
            subRequestIDs.append( reqID[0] )
          if subRequestIDs:
            idString = intListToString( subRequestIDs )
            req = "DELETE FROM `Files` WHERE `SubRequestID` IN (%s);" % idString
            res = self._update( req )
            if not res['OK']:
              failed = True
            req = "DELETE FROM `Datasets` WHERE `SubRequestID` IN (%s);" % idString
            res = self._update( req )
            if not res['OK']:
              failed = True
          req = "DELETE FROM `SubRequests` WHERE `RequestID`=%s;" % requestID
          res = self._update( req )
          if not res['OK']:
            failed = True
          req = "DELETE FROM `Requests` WHERE `RequestID`=%s;" % requestID
          res = self._update( req )
          if not res['OK']:
            failed = True
          if failed:
            errStr = 'RequestDB._deleteRequest: Failed to fully remove Request %s' % requestID
            return S_ERROR( errStr )
          else:
            return S_OK()
        else:
          errStr = 'RequestDB._deleteRequest: Unable to retrieve SubRequestIDs for Request %s' % requestID
          return S_ERROR( errStr )
      else:
        errStr = 'RequestDB._deleteRequest: No RequestID found for %s' % requestName
        return S_ERROR( errStr )
    else:
      errStr = "RequestDB._deleteRequest: Failed to retrieve RequestID for %s" % requestName
      return S_ERROR( errStr )

  def __setSubRequestDatasets( self, ind, requestType, subRequestID, request ):
    res = request.getSubRequestDatasets( ind, requestType )
    if not res['OK']:
      return S_ERROR( 'Failed to get request datasets' )
    datasets = res['Value']
    res = S_OK()
    for dataset in datasets:
      res = self._setDataset( subRequestID, dataset )
      if not res['OK']:
        return S_ERROR( 'Failed to set dataset in DB' )
    return res

  def __updateSubRequestFiles( self, ind, requestType, subRequestID, request ):
    res = request.getSubRequestFiles( ind, requestType )
    if not res['OK']:
      return S_ERROR( 'Failed to get request files' )
    files = res['Value']
    for fileDict in files:
      if not fileDict.has_key( 'FileID' ):
        return S_ERROR( 'No FileID associated to file' )
      fileID = fileDict['FileID']
      req = "UPDATE Files SET"
      for fileAttribute, attributeValue in fileDict.items():
        if not fileAttribute == 'FileID':
          if attributeValue:
            req = "%s %s='%s'," % ( req, fileAttribute, attributeValue )
      req = req.rstrip( ',' )
      req = "%s WHERE SubRequestID = %s AND FileID = %s;" % ( req, subRequestID, fileID )
      res = self._update( req )
      if not res['OK']:
        return S_ERROR( 'Failed to update file in db' )
    
    return S_OK()

  def __setSubRequestFiles( self, ind, requestType, subRequestID, request ):
    """ This is the new method for updating the File table
    """
    res = request.getSubRequestFiles( ind, requestType )
    if not res['OK']:
      return S_ERROR( 'Failed to get request files' )
    files = res['Value']
    for fileDict in files:
      fileAttributes = ['SubRequestID']
      attributeValues = [subRequestID]
      for fileAttribute, attributeValue in fileDict.items():
        if not fileAttribute in ['FileID', 'TargetSE']:
          if attributeValue:
            fileAttributes.append( fileAttribute )
            attributeValues.append( attributeValue )
      if not 'Status' in fileAttributes:
        fileAttributes.append( 'Status' )
        attributeValues.append( 'Waiting' )
      res = self._insert( 'Files', fileAttributes, attributeValues )
      if not res['OK']:
        gLogger.error( 'Failed to insert file into db', res['Message'] )
        return S_ERROR( 'Failed to insert file into db' )
    return S_OK()

  def __setSubRequestAttributes( self, requestID, ind, requestType, subRequestID, request ):
    res = request.getSubRequestAttributes( ind, requestType )
    if not res['OK']:
      return S_ERROR( 'Failed to get sub request attributes' )
    requestAttributes = res['Value']
    status = 'Waiting'
    for requestAttribute, attributeValue in requestAttributes.items():
      if requestAttribute == 'Status':
        status = attributeValue
      elif not requestAttribute == 'SubRequestID':
        res = self._setSubRequestAttribute( requestID, subRequestID, requestAttribute, attributeValue )
        if not res['OK']:
          return S_ERROR( 'Failed to set sub request in DB' )
    return S_OK( status )

  def __setRequestAttributes( self, requestID, request ):
    """ Insert into the DB the request attributes
    """
    res = self._setRequestAttribute( requestID, 'CreationTime', request.getCreationTime()['Value'] )
    if not res['OK']:
      return res
    jobID = request.getJobID()['Value']
    if jobID and not jobID == 'Unknown':
      res = self._setRequestAttribute( requestID, 'JobID', int( jobID ) )
      if not res['OK']:
        return res
    res = self._setRequestAttribute( requestID, 'OwnerDN', request.getOwnerDN()['Value'] )
    if not res['OK']:
      return res
    res = self._setRequestAttribute( requestID, 'OwnerGroup', request.getOwnerGroup()['Value'] )
    if not res['OK']:
      return res
    res = self._setRequestAttribute( requestID, 'DIRACSetup', request.getDIRACSetup()['Value'] )
    return res

  def _setRequestAttribute( self, requestID, attrName, attrValue ):
    """ set request attribute :attrName: to :attrValue: given :requestID:

    :param int requestID: Requests.RequestID
    :param str attrName: col name
    :param mixed attrValue: new value
    """
    attrValue = str(attrValue) if type(attrValue) != list else ",".join( [str(i) for i in attrValue] )
    req = "UPDATE `Requests` SET `%s`='%s', `LastUpdate`=UTC_TIMESTAMP() WHERE `RequestID`='%s';" % ( attrName, 
                                                                                                      attrValue, 
                                                                                                      requestID )
    res = self._update( req )
    if res['OK']:
      return res
    else:
      return S_ERROR( 'RequestDB.setRequestAttribute: failed to set attribute' )

  def _getRequestAttribute( self, attrName, requestID = None, requestName = None ):
    """ read :attrName: from Requests given :requestID: or :requestName:

    :param str atrName: col name
    :param int requestID: Requests.RequestID
    :param str requestName: Requests.RequestName
    """
    if requestID:
      req = "SELECT `%s` from `Requests` WHERE `RequestID`=%s;" % ( attrName, requestID )
    elif requestName:
      req = "SELECT `%s` from `Requests` WHERE `RequestName`='%s';" % ( attrName, requestName )
    else:
      return S_ERROR( 'RequestID or RequestName must be supplied' )
    res = self._query( req )
    if not res['OK']:
      return res
    if res['Value']:
      attrValue = res['Value'][0][0]
      return S_OK( attrValue )
    else:
      errStr = 'Failed to retrieve %s for Request %s/%s' % ( attrName, requestID, requestName )
      return S_ERROR( errStr )

  def _setSubRequestAttribute( self, requestID, subRequestID, attrName, attrValue ):
    """ set :attrName: to :attrValue: for subrequest given :requestID: and :subRequestID:

    :param int requestID: Requests.RequestID
    :param int subRequestID: SubRequests.SubRequestID
    :param str attrName: column name
    :param mixed attrValue: new value
    """
    attrValue = str(attrValue) if type(attrValue) != list else ",".join( [ str(i) for i in attrValue ] )
    req = "UPDATE `SubRequests` SET `%s`='%s', `LastUpdate`=UTC_TIMESTAMP() WHERE `RequestID`=%s AND `SubRequestID`=%s;" % ( attrName, 
                                                                                                                             attrValue, 
                                                                                                                             requestID, 
                                                                                                                             subRequestID )
    res = self._update( req )
    if res['OK']:
      return res
    else:
      return S_ERROR( 'RequestDB.setSubRequestAttribute: failed to set attribute' )

  def _setSubRequestLastUpdate( self, requestID, subRequestID ):
    """ set LastUpdate for subrequest given :requestID: and :subRequestID:

    :param int requestID: Requests.RequestID
    :param int subRequestID: SubRequests.SubRequestID
    """
    req = "UPDATE `SubRequests` SET `LastUpdate`=UTC_TIMESTAMP() WHERE `RequestID`=%s AND `SubRequestID`=%s;" % ( requestID, 
                                                                                                                  subRequestID )
    res = self._update( req )
    if res['OK']:
      return res
    else:
      return S_ERROR( 'RequestDB.setSubRequestLastUpdate: failed to set LastUpdate' )

  def _setFileAttribute( self, subRequestID, fileID, attrName, attrValue ):
    """ set File attribute :attrName: to :attrValue: given :fileID: and :subRequestID:

    :param int subRequestID: SubRequests.SubRequestID
    :param int fileID: FIles.FileID
    :param str attrName: col name
    :param mixed attrValue: new value
    """
    attrValue = str(attrValue) if type(attrValue) != list else ",".join( [ str(i) for i in attrValue ] )
    req = "UPDATE `Files` SET `%s`='%s' WHERE `SubRequestID`='%s' AND `FileID`='%s';" % ( attrName, attrValue, 
                                                                                          subRequestID, fileID )
    res = self._update( req )
    if res['OK']:
      return res
    else:
      return S_ERROR( 'RequestDB.setFileAttribute: failed to set attribute' )

  def _setDataset( self, subRequestID, dataset ):
    req = "INSERT INTO Datasets (Dataset,SubRequestID) VALUES ('%s',%s);" % ( dataset, subRequestID )
    res = self._update( req )
    if res['OK']:
      return res
    else:
      return S_ERROR( 'RequestDB.setFileAttribute: failed to set attribute' )

  def _getFileID( self, subRequestID ):
    self.getIdLock.acquire()
    req = "INSERT INTO Files (Status,SubRequestID) VALUES ('%s','%s');" % ( 'New', subRequestID )
    res = self._update( req )
    if not res['OK']:
      self.getIdLock.release()
      err = 'RequestDB._getFileID: Failed to insert New  SubRequestID'
      return S_ERROR( '%s\n%s' % ( err, res['Message'] ) )
    req = 'SELECT MAX(FileID) FROM Files WHERE SubRequestID=%s' % subRequestID
    res = self._query( req )
    if not res['OK']:
      self.getIdLock.release()
      err = 'RequestDB._getFileID: Failed to get FileID'
      return S_ERROR( '%s\n%s' % ( err, res['Message'] ) )
    self.getIdLock.release()
    try:
      fileID = int( res['Value'][0][0] )
      self.log.info( 'RequestDB: New FileID served "%s"' % fileID )
    except Exception, x:
      err = 'RequestDB._getFileID: Failed to get FileID'
      return S_ERROR( '%s\n%s' % ( err, str( x ) ) )
    return S_OK( fileID )

  def _getRequestID( self, requestName ):
    """ read :requestID: given :requestName:

    :param str requestName: Requests.RequestName
    """
    self.getIdLock.acquire()
    req = "SELECT `RequestID` from `Requests` WHERE `RequestName`='%s';" % requestName
    res = self._query( req )
    if not res['OK']:
      self.getIdLock.release()
      err = 'RequestDB._getRequestID: Failed to get RequestID from RequestName'
      return S_ERROR( '%s\n%s' % ( err, res['Message'] ) )
    if not len( res['Value'] ) == 0:
      self.getIdLock.release()
      err = 'RequestDB._getRequestID: Duplicate entry for RequestName'
      return S_ERROR( err )
    req = 'INSERT INTO `Requests` (`RequestName`,`SubmissionTime`) VALUES ("%s",UTC_TIMESTAMP());' % requestName
    err = 'RequestDB._getRequestID: Failed to retrieve RequestID'
    res = self._update( req )
    if not res['OK']:
      self.getIdLock.release()
      return S_ERROR( '%s\n%s' % ( err, res['Message'] ) )
    req = 'SELECT MAX(RequestID) FROM Requests'
    res = self._query( req )
    if not res['OK']:
      self.getIdLock.release()
      return S_ERROR( '%s\n%s' % ( err, res['Message'] ) )
    try:
      RequestID = int( res['Value'][0][0] )
      self.log.info( 'RequestDB: New RequestID served "%s"' % RequestID )
    except Exception, x:
      self.getIdLock.release()
      return S_ERROR( '%s\n%s' % ( err, str( x ) ) )
    self.getIdLock.release()
    return S_OK( RequestID )

  def _getSubRequestID( self, requestID, requestType ):
    self.getIdLock.acquire()
    req = 'INSERT INTO SubRequests (RequestID,RequestType,SubmissionTime) VALUES (%s,"%s",UTC_TIMESTAMP())' % ( requestID, requestType )
    err = 'RequestDB._getSubRequestID: Failed to retrieve SubRequestID'
    res = self._update( req )
    if not res['OK']:
      self.getIdLock.release()
      return S_ERROR( '%s\n%s' % ( err, res['Message'] ) )
    req = 'SELECT MAX(SubRequestID) FROM SubRequests;'
    res = self._query( req )
    if not res['OK']:
      self.getIdLock.release()
      return S_ERROR( '%s\n%s' % ( err, res['Message'] ) )
    try:
      subRequestID = int( res['Value'][0][0] )
      self.log.info( 'RequestDB: New SubRequestID served "%s"' % subRequestID )
    except Exception, x:
      self.getIdLock.release()
      return S_ERROR( '%s\n%s' % ( err, str( x ) ) )
    self.getIdLock.release()
    return S_OK( subRequestID )

  def getRequestForJobs( self, jobIDs ):
    """ Get the request names associated to the jobsIDs

    :param list jobIDs: lis of jobIDs
    """
    if not jobIDs:
      return S_ERROR( "RequestDB: unable to select requests, no jobIDs supplied" )

    req = "SELECT `JobID`,`RequestName` FROM `Requests` WHERE `JobID` IN (%s);" % intListToString( jobIDs )
    res = self._query( req )
    if not res:
      return res
    jobIDs = {}
    for jobID, requestName in res['Value']:
      jobIDs[jobID] = requestName
    return S_OK( jobIDs )

  def readRequestsForJobs( self, jobIDs ):
    """ read and return Requests for jobs 

    :param mixed jobIDs: list with jobIDs or long JobIDs
    """
    if type(jobIDs) != list:
      return S_ERROR("RequestDB: wrong format for jobIDs argument, got %s, expecting a list" )
    # make sure list is uniqe and has only longs
    jobIDs = list( set( [ int(jobID) for jobID in jobIDs if int(jobID) != 0 ] ) )
    reqCols = [ "RequestID", "RequestName", "JobID", "Status", 
                "OwnerDN", "OwnerGroup", "DIRACSetup", "SourceComponent", 
                "CreationTime", "SubmissionTime", "LastUpdate" ] 
    subCols = [ "SubRequestID", "Operation", "Arguments", "RequestType", "ExecutionOrder", "Error",
                "SourceSE", "TargetSE", "Catalogue", "CreationTime", "SubmissionTime", "LastUpdate" ]
    fileCols = [ "FileID", "LFN", "Size", "PFN", "GUID", "Md5", "Addler", "Attempt", "Status" , "Error" ]

    requestNames = self.getRequestForJobs( jobIDs )
    if not requestNames["OK"]:
      return requestNames
    requestNames = requestNames["Value"]

    ## this will be returned
    retDict = { "Successful" : dict(), "Failed" : dict() }
    for jobID in jobIDs:
      ## missing requests
      if jobID not in requestNames:
        retDict["Failed"][jobID] = "Request not found"  
        continue
      
      requestName = requestNames[jobID]

      ## get request
      queryStr = "SELECT %s FROM Requests WHERE RequestName = '%s';" % ( ",".join( reqCols ), requestName ) 
      queryRes = self._query( queryStr )
      if not queryRes["OK"]:
        retDict["Failed"][jobID] = queryRes["Message"] 
        continue
      
      queryRes = queryRes["Value"] if queryRes["Value"] else None
      if not queryRes:
        retDict["Failed"][jobID] = "Unable to read request attributes."  
        continue

      requestObj = RequestContainer( init=False )
      reqAttrs = dict( zip( reqCols, queryRes[0] ) )      
      requestObj.setRequestAttributes( reqAttrs )

      queryStr = "SELECT %s FROM `SubRequests` WHERE `RequestID`=%s;" % ( ",".join(subCols), reqAttrs["RequestID"] )
      queryRes = self._query( queryStr )
      if not queryRes["OK"]:
        retDict["Failed"][jobID] = queryRes["Message"] 
        continue
      
      queryRes = queryRes["Value"] if queryRes["Value"] else None
      if not queryRes:
        retDict["Failed"][jobID] = "Unable to read subrequest attributes."  
        continue
      
      ## get sub-requests
      for recTuple in queryRes:
        subReqAttrs = dict( zip( subCols, recTuple ) )
        subType = subReqAttrs["RequestType"]
        subReqAttrs["ExecutionOrder"] = int( subReqAttrs["ExecutionOrder"] )
        del subReqAttrs["RequestType"]
        index = requestObj.initiateSubRequest( subType )
        index = index["Value"]
        requestObj.setSubRequestAttributes( index, subType, subReqAttrs )

        ## get files
        subFiles = []
        fileQuery = "SELECT %s FROM `Files` WHERE `SubRequestID` = %s ORDER BY `FileID`;" % ( ",".join(fileCols), 
                                                                                              subReqAttrs["SubRequestID"] )
        fileQueryRes = self._query( fileQuery )
        if fileQueryRes["OK"] and fileQueryRes["Value"]:
          for fileRec in fileQueryRes["Value"]:
            subFiles.append( dict( zip(fileCols, fileRec) ) )
        if subFiles:
          requestObj.setSubRequestFiles( index, subType, subFiles )
      
      retDict["Successful"][jobID] = requestObj.toXML()["Value"]
      
    return S_OK( retDict )
    
  def getDigest( self, requestID ):
    """ Get digest of the given request specified by its requestID
    """
    digest = ''
    digestStrings = []

    req = "SELECT `RequestType`,`Operation`,`Status`,`ExecutionOrder`,`TargetSE`,`Catalogue`,`SubRequestID` from `SubRequests` \
           WHERE `RequestID`=%d" % int( requestID )
    result = self._query( req )
    if not result['OK']:
      return result

    if not result['Value']:
      return S_OK( '' )

    for row in result['Value']:
      digestList = []
      digestList.append( str( row[0] ) )
      digestList.append( str( row[1] ) )
      digestList.append( str( row[2] ) )
      digestList.append( str( row[3] ) )
      if row[0] == "transfer" or row[0] == "register":
        digestList.append( str( row[4] ) )
      if row[0] == "register":
        digestList.append( str( row[5] ) )
      subRequestID = int( row[6] )
      req = "SELECT `LFN` FROM `Files` WHERE `SubRequestID`=%s ORDER BY `FileID`;" % subRequestID
      resFile = self._query( req )
      if resFile['OK']:
        if resFile['Value']:
          lfn = resFile['Value'][0][0]
          digestList.append( os.path.basename( lfn ) )

      digestStrings.append( ":".join( digestList ) )

    digest = '\n'.join( digestStrings )
    return S_OK( digest )

  def getRequestInfo( self, requestID ):
    """ Get the request information from the Requests table """
    req = "SELECT `RequestID`,`Status`,`RequestName`,`JobID`,`OwnerDN`,`OwnerGroup`,`DIRACSetup`,`SourceComponent`," \
        "`CreationTime`,`SubmissionTime`,`LastUpdate` FROM `Requests` WHERE `RequestID`=%d;" % requestID
    res = self._query( req )
    if not res['OK']:
      return res
    requestID, status, requestName, jobID, dn, group, setup, source, creation, submission, lastupdate = res['Value'][0]
    return S_OK( ( requestID, status, requestName, jobID, dn, group, setup, source, creation, submission, lastupdate ) )

  def getRequestStatus( self, requestID ):
    """ Get status of the request and its subrequests
    """
    req = "SELECT `Status` FROM `Requests` WHERE `RequestID`=%d" % int( requestID )
    result = self._query( req )
    if not result['OK']:
      return result
    requestStatus = result['Value'][0][0]
    req = "SELECT `Status` FROM `SubRequests` WHERE `RequestID`=%d" % int( requestID )
    result = self._query( req )
    if not result['OK']:
      return result

    subRequestStatus = "Unknown"
    if not result["Value"]:
      subRequestStatus = "Empty"
    else:
      ## make it unique
      result = list( set( [ row[0] for row in result["Value"] ] ) )
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

    return S_OK( { "RequestStatus" : requestStatus,
                   "SubRequestStatus" : subRequestStatus } )

  def getCurrentExecutionOrder( self, requestID ):
    """ Get the current subrequest execution order for the given request
    """
    req = "SELECT `Status`,`ExecutionOrder` from `SubRequests` WHERE `RequestID`=%d" % int( requestID )
    result = self._query( req )
    if not result['OK']:
      return result

    if not result['Value']:
      return S_ERROR( 'No SubRequests found' )

    current_order = 999
    for row in result['Value']:
      status, order = row
      if status != "Done" and order < current_order:
        current_order = order

    return S_OK( current_order )

  def getRequestSummaryWeb( self, selectDict, sortList, startItem, maxItems ):
    """ Get summary of the requests in the database
    """
    resultDict = {}
    rparameterList = ['RequestID', 'RequestName', 'JobID', 'OwnerDN', 'OwnerGroup']
    sparameterList = ['RequestType', 'Status', 'Operation']
    parameterList = rparameterList + sparameterList
    parameterList.append( 'Error' )
    parameterList.append( 'CreationTime' )
    parameterList.append( 'LastUpdateTime' )

    req = "SELECT R.RequestID, R.RequestName, R.JobID, R.OwnerDN, R.OwnerGroup,"
    req += "S.RequestType, S.Status, S.Operation, S.Error, S.CreationTime, S.LastUpdate FROM Requests as R, SubRequests as S "

    new_selectDict = {}
    older = None
    newer = None
    for key, value in selectDict.items():
      if key in rparameterList:
        new_selectDict['R.' + key] = value
      elif key in sparameterList:
        new_selectDict['S.' + key] = value
      elif key == 'ToDate':
        older = value
      elif key == 'FromDate':
        newer = value

    condition = ''
    if new_selectDict or older or newer:
      condition = self.__buildCondition( new_selectDict, older = older, newer = newer )
      req += condition

    if condition:
      req += " AND R.RequestID=S.RequestID"
    else:
      req += " WHERE R.RequestID=S.RequestID"

    if sortList:
      req += " ORDER BY %s %s" % ( sortList[0][0], sortList[0][1] )
    result = self._query( req )
    if not result['OK']:
      return result

    if not result['Value']:
      resultDict['ParameterNames'] = parameterList
      resultDict['Records'] = []
      return S_OK( resultDict )

    nRequests = len( result['Value'] )

    if startItem <= len( result['Value'] ):
      firstIndex = startItem
    else:
      return S_ERROR( 'Requested index out of range' )

    if ( startItem + maxItems ) <= len( result['Value'] ):
      secondIndex = startItem + maxItems
    else:
      secondIndex = len( result['Value'] )

    records = []
    columnWidth = [ 0 for x in range( len( parameterList ) ) ]
    for i in range( firstIndex, secondIndex ):
      row = result['Value'][i]
      records.append( [ str( x ) for x in row] )
      for ind in range( len( row ) ):
        if len( str( row[ind] ) ) > columnWidth[ind]:
          columnWidth[ind] = len( str( row[ind] ) )

    resultDict['ParameterNames'] = parameterList
    resultDict['ColumnWidths'] = columnWidth
    resultDict['Records'] = records
    resultDict['TotalRecords'] = nRequests

    return S_OK( resultDict )

