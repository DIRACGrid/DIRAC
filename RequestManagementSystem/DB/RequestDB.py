########################################################################
# $HeadURL $
# File: RequestDB.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/12/04 08:06:30
########################################################################
""" :mod: RequestDB
    =======================

    .. module: RequestDB
    :synopsis: db holding Requests
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    db holding Request, Operation and File
"""
__RCSID__ = "$Id $"
# #
# @file RequestDB.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/12/04 08:06:51
# @brief Definition of RequestDB class.

# # imports
import random
import threading
# Get rid of the annoying Deprecation warning of the current MySQLdb
# FIXME: compile a newer MySQLdb version
import warnings
with warnings.catch_warnings():
  warnings.simplefilter( 'ignore', DeprecationWarning )
  import MySQLdb.cursors
from MySQLdb import Error as MySQLdbError
# # from DIRAC
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Base.DB import DB
from DIRAC.Core.Utilities.List import stringListToString
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.File import File

########################################################################
class RequestDB( DB ):
  """
  .. class:: RequestDB

  db holding requests
  """

  def __init__( self, systemInstance = 'Default', maxQueueSize = 10 ):
    """c'tor

    :param self: self reference
    """
    self.getIdLock = threading.Lock()
    DB.__init__( self, "ReqDB", "RequestManagement/ReqDB", maxQueueSize )

  def createTables( self, toCreate = None, force = False ):
    """ create tables """
    toCreate = toCreate if toCreate else []
    if not toCreate:
      return S_OK()
    tableMeta = self.getTableMeta()
    metaCreate = {}
    for tableName in toCreate:
      metaCreate[tableName] = tableMeta[tableName]
    if metaCreate:
      return self._createTables( metaCreate, force )
    return S_OK()

  @staticmethod
  def getTableMeta():
    """ get db schema in a dict format """
    return dict( [ ( classDef.__name__, classDef.tableDesc() )
                   for classDef in ( Request, Operation, File ) ] )

  def getTables( self ):
    """ get tables """
    showTables = self._query( "SHOW TABLES;" )
    if not showTables["OK"]:
      return showTables
    return S_OK( [ table[0] for table in showTables["Value"] if table ] )

  def dictCursor( self, conn = None ):
    """ get dict cursor for connection :conn:

    :return: S_OK( { "cursor": MySQLdb.cursors.DictCursor, "connection" : connection  } ) or S_ERROR
    """
    if not conn:
      retDict = self._getConnection()
      if not retDict["OK"]:
        self.log.error( retDict["Message"] )
        return retDict
      conn = retDict["Value"]
    cursor = conn.cursor( cursorclass = MySQLdb.cursors.DictCursor )
    return S_OK( ( conn, cursor ) )

  def _transaction( self, queries ):
    """ execute transaction """
    queries = [ queries ] if type( queries ) == str else queries
    # # get cursor and connection
    getCursorAndConnection = self.dictCursor()
    if not getCursorAndConnection["OK"]:
      self.log.error( getCursorAndConnection["Message"] )
      return getCursorAndConnection
    connection, cursor = getCursorAndConnection["Value"]
    # # this will be returned as query result
    ret = { "OK" : True }
    queryRes = { }
    # # switch off autocommit
    connection.autocommit( False )
    try:
      # # execute queries
      for query in queries:
        cursor.execute( query )
        queryRes[query] = list( cursor.fetchall() )
      # # commit
      connection.commit()
      # # save last row ID
      lastrowid = cursor.lastrowid
      # # close cursor
      cursor.close()
      ret["Value"] = queryRes
      ret["lastrowid"] = lastrowid
      connection.autocommit( True )
      return ret
    except MySQLdbError, error:
      self.log.exception( error )
      # # rollback
      connection.rollback()
      # # rever autocommit
      connection.autocommit( True )
      # # close cursor
      cursor.close()
      return S_ERROR( str( error ) )

  def putRequest( self, request ):
    """ update or insert request into db

    :param Request request: Request instance
    """
    query = "SELECT `RequestID` from `Request` WHERE `RequestName` = '%s'" % request.RequestName
    exists = self._transaction( query )
    if not exists["OK"]:
      self.log.error( "putRequest: %s" % exists["Message"] )
      return exists
    exists = exists["Value"]

    if exists[query] and exists[query][0]["RequestID"] != request.RequestID:
      return S_ERROR( "putRequest: request '%s' already exists in the db (RequestID=%s)"\
                       % ( request.RequestName, exists[query][0]["RequestID"] ) )
    reqSQL = request.toSQL()
    if not reqSQL["OK"]:
      return reqSQL
    reqSQL = reqSQL["Value"]
    putRequest = self._transaction( reqSQL )
    if not putRequest["OK"]:
      self.log.error( "putRequest: %s" % putRequest["Message"] )
      return putRequest
    lastrowid = putRequest["lastrowid"]
    putRequest = putRequest["Value"]

    cleanUp = request.cleanUpSQL()
    if cleanUp:
      dirty = self._transaction( cleanUp )
      if not dirty["OK"]:
        self.log.error( "putRequest: unable to delete dirty Operation records: %s" % dirty["Message"] )
        return dirty

    # # flag for a new request
    isNew = False
    # # set RequestID when necessary
    if request.RequestID == 0:
      isNew = True
      request.RequestID = lastrowid

    for operation in request:

      cleanUp = operation.cleanUpSQL()
      if cleanUp:
        dirty = self._transaction( [ cleanUp ] )
        if not dirty["OK"]:
          self.log.error( "putRequest: unable to delete dirty File records: %s" % dirty["Message"] )
          return dirty

      opSQL = operation.toSQL()["Value"]
      putOperation = self._transaction( opSQL )
      if not putOperation["OK"]:
        self.log.error( "putRequest: unable to put operation %d: %s" % ( request.indexOf( operation ),
                                                                         putOperation["Message"] ) )
        if isNew:
          deleteRequest = self.deleteRequest( request.RequestName )
          if not deleteRequest["OK"]:
            self.log.error( "putRequest: unable to delete request '%s': %s"\
                             % ( request.RequestName, deleteRequest["Message"] ) )
            return deleteRequest
        return putOperation
      lastrowid = putOperation["lastrowid"]
      putOperation = putOperation["Value"]
      if operation.OperationID == 0:
        operation.OperationID = lastrowid
      filesToSQL = [ opFile.toSQL()["Value"] for opFile in operation ]
      if filesToSQL:
        putFiles = self._transaction( filesToSQL )
        if not putFiles["OK"]:
          self.log.error( "putRequest: unable to put files for operation %d: %s" % ( request.indexOf( operation ),
                                                                                    putFiles["Message"] ) )
          if isNew:
            deleteRequest = self.deleteRequest( request.requestName )
            if not deleteRequest["OK"]:
              self.log.error( "putRequest: unable to delete request '%s': %s"\
                              % ( request.RequestName, deleteRequest["Message"] ) )
              return deleteRequest
          return putFiles

    return S_OK( request.RequestID )

  def getScheduledRequest( self, operationID ):
    """ read scheduled request given its FTS operationID """
    query = "SELECT `Request`.`RequestName` FROM `Request` JOIN `Operation` ON "\
      "`Request`.`RequestID`=`Operation`.`RequestID` WHERE `OperationID` = %s;" % operationID
    requestName = self._query( query )
    if not requestName["OK"]:
      self.log.error( "getScheduledRequest: %s" % requestName["Message"] )
      return requestName
    requestName = requestName["Value"]
    if not requestName:
      return S_OK()
    return self.getRequest( requestName[0][0] )

  def getRequestName( self, requestID ):
    """ get Request.RequestName for a given Request.RequestID """
    query = "SELECT `RequestName` FROM `Request` WHERE `RequestID` = %s" % requestID
    query = self._query( query )
    if not query["OK"]:
      self.log.error( "getRequestName: %s" % query["Message"] )
    query = query["Value"]
    if not query:
      return S_ERROR( "getRequestName: no request found for RequestID=%s" % requestID )
    return S_OK( query[0][0] )


  def getRequest( self, requestName = '', assigned = True ):
    """ read request for execution

    :param str requestName: request's name (default None)
    """
    requestID = None
    log = self.log.getSubLogger( 'getRequest' if assigned else 'peekRequest' )
    if requestName:
      log.verbose( "selecting request '%s'%s" % ( requestName, ' (Assigned)' if assigned else '' ) )
      reqIDQuery = "SELECT `RequestID`, `Status` FROM `Request` WHERE `RequestName` = '%s';" % str( requestName )
      reqID = self._transaction( reqIDQuery )
      if not reqID["OK"]:
        log.error( reqID["Message"] )
        return reqID
      requestID = reqID["Value"][reqIDQuery][0]["RequestID"] if "RequestID" in reqID["Value"][reqIDQuery][0] else None
      status = reqID["Value"][reqIDQuery][0]["Status"] if "Status" in reqID["Value"][reqIDQuery][0] else None
      if not all( ( requestID, status ) ):
        return S_ERROR( "getRequest: request '%s' not exists" % requestName )
      if requestID and status and status == "Assigned" and assigned:
        return S_ERROR( "getRequest: status of request '%s' is 'Assigned', request cannot be selected" % requestName )
    else:
      reqIDsQuery = "SELECT `RequestID` FROM `Request` WHERE `Status` = 'Waiting' ORDER BY `LastUpdate` ASC LIMIT 100;"
      reqAscIDs = self._transaction( reqIDsQuery )
      if not reqAscIDs['OK']:
        log.error( reqAscIDs["Message"] )
        return reqAscIDs
      reqIDs = set( [reqID['RequestID'] for reqID in reqAscIDs["Value"][reqIDsQuery]] )
      reqIDsQuery = "SELECT `RequestID` FROM `Request` WHERE `Status` = 'Waiting' ORDER BY `LastUpdate` DESC LIMIT 50;"
      reqDescIDs = self._transaction( reqIDsQuery )
      if not reqDescIDs['OK']:
        log.error( reqDescIDs["Message"] )
        return reqDescIDs
      reqIDs |= set( [reqID['RequestID'] for reqID in reqDescIDs["Value"][reqIDsQuery]] )
      if not reqIDs:
        return S_OK()
      reqIDs = list( reqIDs )
      random.shuffle( reqIDs )
      requestID = reqIDs[0]

    selectQuery = [ "SELECT * FROM `Request` WHERE `RequestID` = %s;" % requestID,
                    "SELECT * FROM `Operation` WHERE `RequestID` = %s;" % requestID ]
    selectReq = self._transaction( selectQuery )
    if not selectReq["OK"]:
      log.error( selectReq["Message"] )
      return S_ERROR( selectReq["Message"] )
    selectReq = selectReq["Value"]

    request = Request( selectReq[selectQuery[0]][0] )
    if not requestName:
      log.verbose( "selected request '%s'%s" % ( request.RequestName, ' (Assigned)' if assigned else '' ) )
    for records in sorted( selectReq[selectQuery[1]], key = lambda k: k["Order"] ):
      # # order is ro, remove
      del records["Order"]
      operation = Operation( records )
      getFilesQuery = "SELECT * FROM `File` WHERE `OperationID` = %s;" % operation.OperationID
      getFiles = self._transaction( getFilesQuery )
      if not getFiles["OK"]:
        log.error( getFiles["Message"] )
        return getFiles
      getFiles = getFiles["Value"][getFilesQuery]
      for getFile in getFiles:
        getFileDict = dict( [ ( key, value ) for key, value in getFile.items() if value != None ] )
        operation.addFile( File( getFileDict ) )
      request.addOperation( operation )

    if assigned:
      setAssigned = self._transaction( "UPDATE `Request` SET `Status` = 'Assigned', `LastUpdate`=UTC_TIMESTAMP() WHERE RequestID = %s;" % requestID )
      if not setAssigned["OK"]:
        log.error( "%s" % setAssigned["Message"] )
        return setAssigned

    return S_OK( request )

  def peekRequest( self, requestName ):
    """ get request (ro), no update on states

    :param str requestName: Request.RequestName
    """
    return self.getRequest( requestName, False )

  def getRequestNamesList( self, statusList = None, limit = None ):
    """ select requests with status in :statusList: """
    statusList = statusList if statusList else list( Request.FINAL_STATES )
    limit = limit if limit else 100
    query = "SELECT `RequestName`, `Status`, `LastUpdate` FROM `Request` WHERE "\
      " `Status` IN (%s) ORDER BY `LastUpdate` ASC LIMIT %s;" % ( stringListToString( statusList ), limit )
    reqNamesList = self._query( query )
    if not reqNamesList["OK"]:
      self.log.error( "getRequestNamesList: %s" % reqNamesList["Message"] )
      return reqNamesList
    reqNamesList = reqNamesList["Value"]
    return S_OK( [ reqName for reqName in reqNamesList] )

  def deleteRequest( self, requestName ):
    """ delete request given its name

    :param str requestName: request.RequestName
    :param mixed connection: connection to use if any
    """
    requestIDs = self._transaction( 
      "SELECT r.RequestID, o.OperationID FROM `Request` r LEFT JOIN `Operation` o "\
        "ON r.RequestID = o.RequestID WHERE `RequestName` = '%s'" % requestName )

    if not requestIDs["OK"]:
      self.log.error( "deleteRequest: unable to read RequestID and OperationIDs: %s" % requestIDs["Message"] )
      return requestIDs
    requestIDs = requestIDs["Value"]
    trans = []
    requestID = None
    for records in requestIDs.values():
      for record in records:
        requestID = record["RequestID"] if record["RequestID"] else None
        operationID = record["OperationID"] if record["OperationID"] else None
        if operationID and requestID:
          trans.append( "DELETE FROM `File` WHERE `OperationID` = %s;" % operationID )
          trans.append( "DELETE FROM `Operation` WHERE `RequestID` = %s AND `OperationID` = %s;" % ( requestID,
                                                                                                    operationID ) )
    # # last bit: request itself
    if requestID:
      trans.append( "DELETE FROM `Request` WHERE `RequestID` = %s;" % requestID )

    delete = self._transaction( trans )
    if not delete["OK"]:
      self.log.error( "deleteRequest: unable to delete request '%s': %s" % ( requestName, delete["Message"] ) )
      return delete
    return S_OK()

  def getRequestProperties( self, requestName, columnNames ):
    """ submit query """
    return self._query( self._getRequestProperties( requestName, columnNames ) )

  def _getRequestProperties( self, requestName, columnNames = None ):
    """ select :columnNames: from Request table  """
    columnNames = columnNames if columnNames else Request.tableDesc()["Fields"].keys()
    columnNames = ",".join( [ '`%s`' % str( columnName ) for columnName in columnNames ] )
    return "SELECT %s FROM `Request` WHERE `RequestName` = '%s';" % ( columnNames, requestName )

  def _getOperationProperties( self, operationID, columnNames = None ):
    """ select :columnNames: from Operation table  """
    columnNames = columnNames if columnNames else Operation.tableDesc()["Fields"].keys()
    columnNames = ",".join( [ '`%s`' % str( columnName ) for columnName in columnNames ] )
    return "SELECT %s FROM `Operation` WHERE `OperationID` = %s;" % ( columnNames, int( operationID ) )

  def _getFileProperties( self, fileID, columnNames = None ):
    """ select :columnNames: from File table  """
    columnNames = columnNames if columnNames else File.tableDesc()["Fields"].keys()
    columnNames = ",".join( [ '`%s`' % str( columnName ) for columnName in columnNames ] )
    return "SELECT %s FROM `File` WHERE `FileID` = %s;" % ( columnNames, int( fileID ) )

  def getDBSummary( self ):
    """ get db summary """

    # # this will be returned
    retDict = { "Request" : {}, "Operation" : {}, "File" : {} }
    transQueries = { "SELECT `Status`, COUNT(`Status`) FROM `Request` GROUP BY `Status`;" : "Request",
                     "SELECT `Type`,`Status`,COUNT(`Status`) FROM `Operation` GROUP BY `Type`,`Status`;" : "Operation",
                     "SELECT `Status`, COUNT(`Status`) FROM `File` GROUP BY `Status`;" : "File" }
    ret = self._transaction( transQueries.keys() )
    if not ret["OK"]:
      self.log.error( "getDBSummary: %s" % ret["Message"] )
      return ret
    ret = ret["Value"]
    for k, v in ret.items():
      if transQueries[k] == "Request":
        for aDict in v:
          status = aDict.get( "Status" )
          count = aDict.get( "COUNT(`Status`)" )
          if status not in retDict["Request"]:
            retDict["Request"][status] = 0
          retDict["Request"][status] += count
      elif transQueries[k] == "File":
        for aDict in v:
          status = aDict.get( "Status" )
          count = aDict.get( "COUNT(`Status`)" )
          if status not in retDict["File"]:
            retDict["File"][status] = 0
          retDict["File"][status] += count
      else:  # # operation
        for aDict in v:
          status = aDict.get( "Status" )
          oType = aDict.get( "Type" )
          count = aDict.get( "COUNT(`Status`)" )
          if oType not in retDict["Operation"]:
            retDict["Operation"][oType] = {}
          if status not in retDict["Operation"][oType]:
            retDict["Operation"][oType][status] = 0
          retDict["Operation"][oType][status] += count
    return S_OK( retDict )

  def getRequestSummaryWeb( self, selectDict, sortList, startItem, maxItems ):
    """ get db summary for web

    :param dict selectDict: whatever
    :param list sortList: whatever
    :param int startItem: limit
    :param int maxItems: limit


    """
    resultDict = {}
    rparameterList = [ 'RequestID', 'RequestName', 'JobID', 'OwnerDN', 'OwnerGroup']
    sparameterList = [ 'Type', 'Status', 'Operation']
    parameterList = rparameterList + sparameterList + [ "Error", "CreationTime", "LastUpdate"]
    # parameterList.append( 'Error' )
    # parameterList.append( 'CreationTime' )
    # parameterList.append( 'LastUpdateTime' )

    req = "SELECT R.RequestID, R.RequestName, R.JobID, R.OwnerDN, R.OwnerGroup,"
    req += "O.Type, O.Status, O.Type, O.Error, O.CreationTime, O.LastUpdate FROM Request as R, Operation as O "

    new_selectDict = {}
    older = None
    newer = None
    for key, value in selectDict.items():
      if key in rparameterList:
        new_selectDict['R.' + key] = value
      elif key in sparameterList:
        new_selectDict['O.' + key] = value
      elif key == 'ToDate':
        older = value
      elif key == 'FromDate':
        newer = value

    condition = ''
    if new_selectDict or older or newer:
      condition = self.__buildCondition( new_selectDict, older = older, newer = newer )
      req += condition

    if condition:
      req += " AND R.RequestID=O.RequestID"
    else:
      req += " WHERE R.RequestID=O.RequestID"

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

  def getRequestNamesForJobs( self, jobIDs ):
    """ read request names for jobs given jobIDs

    :param list jobIDs: list of jobIDs
    """
    self.log.debug( "getRequestForJobs: got %s jobIDs to check" % str( jobIDs ) )
    if not jobIDs:
      return S_ERROR( "Must provide jobID list as argument." )
    if type( jobIDs ) in ( long, int ):
      jobIDs = [ jobIDs ]
    jobIDs = list( set( [ int( jobID ) for jobID in jobIDs ] ) )
    reqDict = { "Successful": {}, "Failed": {} }
    # # filter out 0
    jobIDsStr = ",".join( [ str( jobID ) for jobID in jobIDs if jobID ] )
    # # request names
    requestNames = "SELECT `RequestName`, `JobID` FROM `Request` WHERE `JobID` IN (%s);" % jobIDsStr
    requestNames = self._query( requestNames )
    if not requestNames["OK"]:
      self.log.error( "getRequestsForJobs: %s" % requestNames["Message"] )
      return requestNames
    requestNames = requestNames["Value"]
    for requestName, jobID in requestNames:
      reqDict["Successful"][jobID] = requestName
    reqDict["Failed"] = dict.fromkeys( [ jobID for jobID in jobIDs if jobID not in reqDict["Successful"] ],
                                       "Request not found" )
    return S_OK( reqDict )

  def readRequestsForJobs( self, jobIDs = None ):
    """ read request for jobs

    :param list jobIDs: list of JobIDs
    :return: S_OK( "Successful" : { jobID1 : Request, jobID2: Request, ... }
                   "Failed" : { jobID3: "error message", ... } )
    """
    self.log.debug( "readRequestForJobs: got %s jobIDs to check" % str( jobIDs ) )
    requestNames = self.getRequestNamesForJobs( jobIDs )
    if not requestNames["OK"]:
      self.log.error( "readRequestForJobs: %s" % requestNames["Message"] )
      return requestNames
    requestNames = requestNames["Value"]
    # # this will be returned
    retDict = { "Failed": requestNames["Failed"], "Successful": {} }

    self.log.debug( "readRequestForJobs: got %d request names" % len( requestNames["Successful"] ) )
    for jobID in requestNames['Successful']:
      request = self.peekRequest( requestNames['Successful'][jobID] )
      if not request["OK"]:
        retDict["Failed"][jobID] = request["Message"]
        continue
      retDict["Successful"][jobID] = request["Value"]
    return S_OK( retDict )

  def getRequestStatus( self, requestName ):
    """ get request status for a given request name """
    self.log.debug( "getRequestStatus: checking status for '%s' request" % requestName )
    query = "SELECT `Status` FROM `Request` WHERE `RequestName` = '%s'" % requestName
    query = self._query( query )
    if not query["OK"]:
      self.log.error( "getRequestStatus: %s" % query["Message"] )
      return query
    requestStatus = query['Value'][0][0]
    return S_OK( requestStatus )

  def getRequestFileStatus( self, requestName, lfnList ):
    """ get status for files in request given its name

    :param str requestName: Request.RequestName
    :param list lfnList: list of LFNs
    """
    if type( requestName ) == int:
      requestName = self.getRequestName( requestName )
      if not requestName["OK"]:
        self.log.error( "getRequestFileStatus: %s" % requestName["Message"] )
        return requestName
      else:
        requestName = requestName["Value"]

    req = self.peekRequest( requestName )
    if not req["OK"]:
      self.log.error( "getRequestFileStatus: %s" % req["Message"] )
      return req

    req = req["Value"]
    res = dict.fromkeys( lfnList, "UNKNOWN" )
    for op in req:
      for opFile in op:
        if opFile.LFN in lfnList:
          res[opFile.LFN] = opFile.Status
    return S_OK( res )

  def getRequestInfo( self, requestName ):
    """ get request info given Request.RequestID """
    if type( requestName ) == int:
      requestName = self.getRequestName( requestName )
      if not requestName["OK"]:
        self.log.error( "getRequestInfo: %s" % requestName["Message"] )
        return requestName
      else:
        requestName = requestName["Value"]
    requestInfo = self.getRequestProperties( requestName, [ "RequestID", "Status", "RequestName", "JobID",
                                                            "OwnerDN", "OwnerGroup", "DIRACSetup", "SourceComponent",
                                                            "CreationTime", "SubmitTime", "lastUpdate" ] )
    if not requestInfo["OK"]:
      self.log.error( "getRequestInfo: %s" % requestInfo["Message"] )
      return requestInfo
    requestInfo = requestInfo["Value"][0]
    return S_OK( requestInfo )

  def getDigest( self, requestName ):
    """ get digest for request given its name

    :param str requestName: request name
    """
    self.log.debug( "getDigest: will create digest for request '%s'" % requestName )
    request = self.getRequest( requestName, False )
    if not request["OK"]:
      self.log.error( "getDigest: %s" % request["Message"] )
    request = request["Value"]
    if not isinstance( request, Request ):
      self.log.info( "getDigest: request '%s' not found" )
      return S_OK()
    return request.getDigest()

  @staticmethod
  def __buildCondition( condDict, older = None, newer = None ):
    """ build SQL condition statement from provided condDict
       and other extra conditions

       blindly copied from old code, hope it works
    """
    condition = ''
    conjunction = "WHERE"
    if condDict != None:
      for attrName, attrValue in condDict.items():
        if type( attrValue ) == list:
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
      condition = ' %s %s O.LastUpdate < \'%s\'' % ( condition,
                                                 conjunction,
                                                 str( older ) )
      conjunction = "AND"

    if newer:
      condition = ' %s %s O.LastUpdate >= \'%s\'' % ( condition,
                                                 conjunction,
                                                 str( newer ) )

    return condition


