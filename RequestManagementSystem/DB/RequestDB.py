########################################################################
# $HeadURL $
# File: RequestDB.py
# Date: 2012/12/04 08:06:30
########################################################################
""" :mod: RequestDB
    =======================

    .. module: RequestDB
    :synopsis: db holding Requests

    db holding Request, Operation and File
"""
__RCSID__ = "$Id $"

import random
import socket

import datetime
# # from DIRAC
from DIRAC import S_OK, S_ERROR, gConfig, gLogger
from DIRAC.Core.Base.DB import DB
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.File import File
from DIRAC.ConfigurationSystem.Client.PathFinder import getDatabaseSection

from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm import relationship, backref, sessionmaker, joinedload_all, mapper
from sqlalchemy.sql import update
from sqlalchemy import create_engine, func, Table, Column, MetaData, ForeignKey,\
                       Integer, String, DateTime, Enum, BLOB, BigInteger


# Metadata instance that is used to bind the engine, Object and tables
metadata = MetaData()


# Description of the file table

fileTable = Table( 'File', metadata,
            Column( 'FileID', Integer, primary_key = True ),
            Column( 'OperationID', Integer,
                        ForeignKey( 'Operation.OperationID', ondelete = 'CASCADE' ),
                        nullable = False ),
            Column( 'Status', Enum( 'Waiting', 'Done', 'Failed', 'Scheduled' ), server_default = 'Waiting' ),
            Column( 'LFN', String( 255 ), index = True ),
            Column( 'PFN', String( 255 ) ),
            Column( 'ChecksumType', Enum( 'ADLER32', 'MD5', 'SHA1', '' ), server_default = '' ),
            Column( 'Checksum', String( 255 ) ),
            Column( 'GUID', String( 36 ) ),
            Column( 'Size', BigInteger ),
            Column( 'Attempt', Integer ),
            Column( 'Error', String( 255 ) ),
            mysql_engine = 'InnoDB'
        )

# Map the File object to the fileTable, with a few special attributes

mapper( File, fileTable, properties = {
   '_Status': fileTable.c.Status,
   '_LFN': fileTable.c.LFN,
   '_ChecksumType' : fileTable.c.ChecksumType,
   '_GUID' : fileTable.c.GUID,
} )


# Description of the Operation table

operationTable = Table( 'Operation', metadata,
                        Column( 'TargetSE', String( 255 ) ),
                        Column( 'CreationTime', DateTime ),
                        Column( 'SourceSE', String( 255 ) ),
                        Column( 'Arguments', BLOB ),
                        Column( 'Error', String( 255 ) ),
                        Column( 'Type', String( 64 ), nullable = False ),
                        Column( 'Order', Integer, nullable = False ),
                        Column( 'Status', Enum( 'Waiting', 'Assigned', 'Queued', 'Done', 'Failed', 'Canceled', 'Scheduled' ), server_default = 'Queued' ),
                        Column( 'LastUpdate', DateTime ),
                        Column( 'SubmitTime', DateTime ),
                        Column( 'Catalog', String( 255 ) ),
                        Column( 'OperationID', Integer, primary_key = True ),
                        Column( 'RequestID', Integer,
                                  ForeignKey( 'Request.RequestID', ondelete = 'CASCADE' ),
                                  nullable = False ),
                       mysql_engine = 'InnoDB'
                       )


# Map the Operation object to the operationTable, with a few special attributes

mapper(Operation, operationTable, properties={
   '_CreationTime': operationTable.c.CreationTime,
   '_Order': operationTable.c.Order,
   '_Status': operationTable.c.Status,
   '_LastUpdate': operationTable.c.LastUpdate,
   '_SubmitTime': operationTable.c.SubmitTime,
   '_Catalog': operationTable.c.Catalog,
   '__files__':relationship( File,
                            backref = backref( '_parent', lazy = 'immediate' ),
                            lazy = 'immediate',
                            passive_deletes = True,
                            cascade = "all, delete-orphan" )

})


# Description of the Request Table

requestTable = Table( 'Request', metadata,
                        Column( 'DIRACSetup', String( 32 ) ),
                        Column( 'CreationTime', DateTime ),
                        Column( 'JobID', Integer, server_default = '0' ),
                        Column( 'OwnerDN', String( 255 ) ),
                        Column( 'RequestName', String( 255 ), nullable = False, unique = True ),
                        Column( 'Error', String( 255 ) ),
                        Column( 'Status', Enum( 'Waiting', 'Assigned', 'Done', 'Failed', 'Canceled', 'Scheduled' ), server_default = 'Waiting' ),
                        Column( 'LastUpdate', DateTime ),
                        Column( 'OwnerGroup', String( 32 ) ),
                        Column( 'SubmitTime', DateTime ),
                        Column( 'RequestID', Integer, primary_key = True ),
                        Column( 'SourceComponent', BLOB ),
                        mysql_engine = 'InnoDB'

                       )

# Map the Request object to the requestTable, with a few special attributes

mapper( Request, requestTable, properties = {
   '_CreationTime': requestTable.c.CreationTime,
   '_Status': requestTable.c.Status,
   '_LastUpdate': requestTable.c.LastUpdate,
   '_SubmitTime': requestTable.c.SubmitTime,
   '__operations__' : relationship( Operation,
                                  backref = backref( '_parent', lazy = 'immediate' ),
                                  order_by = operationTable.c.Order,
                                  lazy = 'immediate',
                                  passive_deletes = True,
                                  cascade = "all, delete-orphan"
                                )

} )







########################################################################
class RequestDB( object ):
  """
  .. class:: RequestDB

  db holding requests
  """


  def __getDBConnectionInfo( self, fullname ):
    """ Collect from the CS all the info needed to connect to the DB.
        This should be in a base class eventually
    """
    self.fullname = fullname
    self.cs_path = getDatabaseSection( self.fullname )

    self.dbHost = ''
    result = gConfig.getOption( self.cs_path + '/Host' )
    if not result['OK']:
      raise RuntimeError( 'Failed to get the configuration parameters: Host' )
    self.dbHost = result['Value']
    # Check if the host is the local one and then set it to 'localhost' to use
    # a socket connection
    if self.dbHost != 'localhost':
      localHostName = socket.getfqdn()
      if localHostName == self.dbHost:
        self.dbHost = 'localhost'

    self.dbPort = 3306
    result = gConfig.getOption( self.cs_path + '/Port' )
    if not result['OK']:
      # No individual port number found, try at the common place
      result = gConfig.getOption( '/Systems/Databases/Port' )
      if result['OK']:
        self.dbPort = int( result['Value'] )
    else:
      self.dbPort = int( result['Value'] )

    self.dbUser = ''
    result = gConfig.getOption( self.cs_path + '/User' )
    if not result['OK']:
      # No individual user name found, try at the common place
      result = gConfig.getOption( '/Systems/Databases/User' )
      if not result['OK']:
        raise RuntimeError( 'Failed to get the configuration parameters: User' )
    self.dbUser = result['Value']
    self.dbPass = ''
    result = gConfig.getOption( self.cs_path + '/Password' )
    if not result['OK']:
      # No individual password found, try at the common place
      result = gConfig.getOption( '/Systems/Databases/Password' )
      if not result['OK']:
        raise RuntimeError( 'Failed to get the configuration parameters: Password' )
    self.dbPass = result['Value']
    self.dbName = ''
    result = gConfig.getOption( self.cs_path + '/DBName' )
    if not result['OK']:
      raise RuntimeError( 'Failed to get the configuration parameters: DBName' )
    self.dbName = result['Value']


  def __init__( self, systemInstance = 'Default', maxQueueSize = 10 ):
    """c'tor

    :param self: self reference
    """

    self.log = gLogger.getSubLogger( 'RequestDB' )
    # Initialize the connection info
    self.__getDBConnectionInfo( 'RequestManagement/ReqDB' )



    runDebug = ( gLogger.getLevel() == 'DEBUG' )
    self.engine = create_engine( 'mysql://%s:%s@%s/%s' % ( self.dbUser, self.dbPass, self.dbHost, self.dbName ),
                                 echo = runDebug )

    metadata.bind = self.engine

    self.DBSession = sessionmaker( bind = self.engine )


  def createTables( self, toCreate = None, force = False ):
    """ create tables """
    try:
      metadata.create_all( self.engine )
    except Exception, e:
      return S_ERROR( e )
    return S_OK()

  @staticmethod
  def getTableMeta():
    """ get db schema in a dict format """
    return dict( [ ( classDef.__name__, None )
                   for classDef in ( Request, Operation, File ) ] )


  def getTables(self):
    """ Return the table names """
    return S_OK( metadata.tables.keys() )

  def cancelRequest(self, request_name):
    session = self.DBSession()
    try:
      updateRet = session.execute( update( Request )\
                         .where( Request.RequestName == request_name )\
                         .values( {Request._Status : 'Canceled',
                                   Request._LastUpdate : datetime.datetime.utcnow()\
                                                        .strftime( Request._datetimeFormat )
                                  }
                                 )
                       )
      session.commit()
      
      # No row was changed
      if not updateRet.rowcount:
        return S_ERROR("No such request %s"%request_name)

      return S_OK()

    except Exception, e:
      session.rollback()
      self.log.exception( "cancelRequest: unexpected exception", lException = e )
      return S_ERROR( "cancelRequest: unexpected exception %s" % e )
    finally:
      session.close()


  def putRequest( self, request ):
    """ update or insert request into db

    :param Request request: Request instance
    """
    
    session = self.DBSession( expire_on_commit = False )
    try:

      try:
        existingReqID, status = session.query( Request.RequestID, Request._Status )\
                                   .filter( Request.RequestName == request.RequestName )\
                                   .one()

        if existingReqID and existingReqID != request.RequestID:
          return S_ERROR( "putRequest: request '%s' already exists in the db (RequestID=%s)"\
                         % ( request.RequestName, existingReqID ) )
  
        if status == 'Canceled':
          self.log.info( "Request %s was canceled, don't put it back" % request.RequestName )
          return S_OK( request.RequestID )

      except NoResultFound, e:
        pass

    

      session.add( request )
      session.commit()
      session.expunge_all()
  
      return S_OK( request.RequestID )

    except Exception, e:
      session.rollback()
      self.log.exception( "putRequest: unexpected exception", lException = e )
      return S_ERROR( "putRequest: unexpected exception %s" % e )
    finally:
      session.close()


  def getScheduledRequest( self, operationID ):
    session = self.DBSession()
    try:
      requestName = session.query( Request.RequestName )\
                           .join( Request.__operations__ )\
                           .filter( Operation.OperationID == operationID )\
                           .one()
      return self.getRequest( requestName[0] )
    except NoResultFound, e:
      return S_OK()
    finally:
      session.close()


  def getRequestName( self, requestID ):
    """ get Request.RequestName for a given Request.RequestID """

    session = self.DBSession()
    try:
      requestName = session.query( Request.RequestName )\
                           .filter( Request.RequestID == requestID )\
                           .one()
      return S_OK( requestName[0] )
    except NoResultFound, e:
      return S_ERROR( "getRequestName: no request found for RequestID=%s" % requestID )
    finally:
      session.close()


  def getRequest( self, requestName = '', assigned = True ):
    """ read request for execution

    :param str requestName: request's name (default None)
    """

    # expire_on_commit is set to False so that we can still use the object after we close the session
    session = self.DBSession( expire_on_commit = False )
    log = self.log.getSubLogger( 'getRequest' if assigned else 'peekRequest' )

    requestID = None
    try:

      if requestName:

        log.verbose( "selecting request '%s'%s" % ( requestName, ' (Assigned)' if assigned else '' ) )
        status = None
        try:
          requestID, status = session.query( Request.RequestID, Request._Status )\
                                     .filter( Request.RequestName == requestName )\
                                     .one()
        except NoResultFound, e:
          return S_ERROR( "getRequest: request '%s' not exists" % requestName )
  
        if requestID and status and status == "Assigned" and assigned:
          return S_ERROR( "getRequest: status of request '%s' is 'Assigned', request cannot be selected" % requestName )

      else:
        reqIDs = set()
        try:
          reqAscIDs = session.query( Request.RequestID )\
                             .filter( Request._Status == 'Waiting' )\
                             .order_by( Request._LastUpdate )\
                             .limit( 100 )\
                             .all()

          reqIDs = set( [reqID[0] for reqID in reqAscIDs] )

          reqDescIDs = session.query( Request.RequestID )\
                              .filter( Request._Status == 'Waiting' )\
                              .order_by( Request._LastUpdate.desc() )\
                              .limit( 50 )\
                              .all()

          reqIDs |= set( [reqID[0] for reqID in reqDescIDs] )
        # No Waiting requests
        except NoResultFound, e:
          return S_OK()
  
        reqIDs = list( reqIDs )
        random.shuffle( reqIDs )
        requestID = reqIDs[0]


      # If we are here, the request MUST exist, so no try catch
      # the joinedload_all is to force the non-lazy loading of all the attributes, especially _parent
      request = session.query( Request )\
                       .options( joinedload_all( '__operations__.__files__' ) )\
                       .filter( Request.RequestID == requestID )\
                       .one()
  
      if not requestName:
        log.verbose( "selected request '%s'%s" % ( request.RequestName, ' (Assigned)' if assigned else '' ) )
  
  
      if assigned:
        session.execute( update( Request )\
                         .where( Request.RequestID == requestID )\
                         .values( {Request._Status : 'Assigned'} )
                       )
        session.commit()

      session.expunge_all()
      return S_OK( request )
    
    except Exception, e:
      session.rollback()
      log.exception( "getRequest: unexpected exception", lException = e )
      return S_ERROR( "getRequest: unexpected exception : %s" % e )
    finally:
      session.close()


  def getBulkRequests( self, numberOfRequest = 10, assigned = True ):
    """ read as many requests as requested for execution

    :param int numberOfRequest: Number of Request we want (default 10)
    :param bool assigned: if True, the status of the selected requests are set to assign

    :returns a dictionary of Request objects indexed on the RequestID

    """
    
    # expire_on_commit is set to False so that we can still use the object after we close the session
    session = self.DBSession( expire_on_commit = False )
    log = self.log.getSubLogger( 'getBulkRequest' if assigned else 'peekBulkRequest' )

    requestDict = {}

    try:

      # If we are here, the request MUST exist, so no try catch
      # the joinedload_all is to force the non-lazy loading of all the attributes, especially _parent
      try:
        requests = session.query( Request )\
                          .options( joinedload_all( '__operations__.__files__' ) )\
                          .filter( Request._Status == 'Waiting' )\
                          .order_by( Request._LastUpdate )\
                          .limit( numberOfRequest )\
                          .all()
        requestDict = dict((req.RequestID, req) for req in requests)
      # No Waiting requests
      except NoResultFound, e:
        pass
      
      if assigned and requestDict:
        session.execute( update( Request )\
                         .where( Request.RequestID.in_( requestDict.keys() ) )\
                         .values( {Request._Status : 'Assigned'} )
                       )
        session.commit()

      session.expunge_all()

    except Exception, e:
      session.rollback()
      log.exception( "unexpected exception", lException = e )
      return S_ERROR( "getBulkRequest: unexpected exception : %s" % e )
    finally:
      session.close()

    return S_OK( requestDict )



  def peekRequest( self, requestName ):
    """ get request (ro), no update on states

    :param str requestName: Request.RequestName
    """
    return self.getRequest( requestName, False )



  def getRequestNamesList( self, statusList = None, limit = None, since = None, until = None ):
    """ select requests with status in :statusList: """
    statusList = statusList if statusList else list( Request.FINAL_STATES )
    limit = limit if limit else 100
    session = self.DBSession()
    requests = []
    try:
      reqQuery = session.query( Request.RequestName )\
                        .filter( Request._Status.in_( statusList ) )
      if since:
        reqQuery = reqQuery.filter( Request._LastUpdate > since )
      if until:
        reqQuery = reqQuery.filter( Request._LastUpdate < until )

      reqQuery = reqQuery.order_by( Request._LastUpdate )\
                         .limit( limit )
      requests = [reqNameTuple[0] for reqNameTuple in reqQuery.all()]

    except Exception, e:
      session.rollback()
      self.log.exception( "getRequestNamesList: unexpected exception", lException = e )
      return S_ERROR( "getRequestNamesList: unexpected exception : %s" % e )
    finally:
      session.close()

    return S_OK( requests )



  def deleteRequest( self, requestName ):
    """ delete request given its name

    :param str requestName: request.RequestName
    :param mixed connection: connection to use if any
    """
    
    session = self.DBSession()

    try:
      session.query( Request ).filter( Request.RequestName == requestName ).delete()
      session.commit()
    except Exception, e:
      session.rollback()
      self.log.exception( "deleteRequest: unexpected exception", lException = e )
      return S_ERROR( "deleteRequest: unexpected exception : %s" % e )
    finally:
      session.close()

    return S_OK()


  def getDBSummary( self ):
    """ get db summary """
    # # this will be returned
    retDict = { "Request" : {}, "Operation" : {}, "File" : {} }
 
    session = self.DBSession()
 
    try:
      requestQuery = session.query(Request._Status, func.count(Request.RequestID)).group_by(Request._Status).all()
      for status, count in requestQuery:
        retDict["Request"][status] = count
 
      operationQuery = session.query(Operation.Type, Operation._Status, func.count(Operation.OperationID))\
                              .group_by(Operation.Type, Operation._Status).all()
      for oType, status, count in operationQuery:
        retDict['Operation'].setdefault( oType, {} )[status] = count
      
      
      fileQuery = session.query(File._Status, func.count(File.FileID)).group_by(File._Status).all()
      for status, count in fileQuery:
        retDict["File"][status] = count
 
    except Exception, e:
      self.log.exception( "getDBSummary: unexpected exception", lException = e )
      return S_ERROR( "getDBSummary: unexpected exception : %s" % e )
    finally:
      session.close()

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
    jobIDs = set( jobIDs )

    reqDict = { "Successful": {}, "Failed": {} }

    session = self.DBSession()

    try:
      ret = session.query( Request.JobID, Request.RequestName )\
                   .filter( Request.JobID.in_( jobIDs ) )\
                  .all()

      reqDict['Successful'] = dict((jobId, reqName) for jobId, reqName in ret)
      reqDict['Failed'] = dict( (jobid, 'Request not found') for jobid in jobIDs - set(reqDict['Successful']))
    except Exception, e:
      self.log.exception( "getRequestNamesForJobs: unexpected exception", lException = e )
      return S_ERROR( "getRequestNamesForJobs: unexpected exception : %s" % e )
    finally:
      session.close()

    return S_OK( reqDict )


  def readRequestsForJobs( self, jobIDs = None ):
    """ read request for jobs

    :param list jobIDs: list of JobIDs
    :return: S_OK( "Successful" : { jobID1 : Request, jobID2: Request, ... }
                   "Failed" : { jobID3: "error message", ... } )
    """
    self.log.debug( "readRequestForJobs: got %s jobIDs to check" % str( jobIDs ) )
    if not jobIDs:
      return S_ERROR( "Must provide jobID list as argument." )
    if type( jobIDs ) in ( long, int ):
      jobIDs = [ jobIDs ]
    jobIDs = set( jobIDs )

    reqDict = { "Successful": {}, "Failed": {} }

    # expire_on_commit is set to False so that we can still use the object after we close the session
    session = self.DBSession( expire_on_commit = False )

    try:
      ret = session.query( Request.JobID, Request )\
                   .options( joinedload_all( '__operations__.__files__' ) )\
                   .filter( Request.JobID.in_( jobIDs ) ).all()

      reqDict['Successful'] = dict( ( jobId, reqObj ) for jobId, reqObj in ret )

      reqDict['Failed'] = dict( ( jobid, 'Request not found' ) for jobid in jobIDs - set( reqDict['Successful'] ) )
      session.expunge_all()
    except Exception, e:
      self.log.exception( "readRequestsForJobs: unexpected exception", lException = e )
      return S_ERROR( "readRequestsForJobs: unexpected exception : %s" % e )
    finally:
      session.close()

    return S_OK( reqDict )


  def getRequestStatus( self, requestName ):
    """ get request status for a given request name """
    self.log.debug( "getRequestStatus: checking status for '%s' request" % requestName )
    session = self.DBSession()
    try:
      status = session.query( Request._Status ).filter( Request.RequestName == requestName ).one()
    except  NoResultFound, e:
      return S_ERROR( "Request %s does not exist" % requestName )
    finally:
      session.close()
    return S_OK( status[0] )


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

    session = self.DBSession()
    try:
      res = dict.fromkeys( lfnList, "UNKNOWN" )
      requestRet = session.query( File._LFN, File._Status )\
                       .join( Request.__operations__ )\
                       .join( Operation.__files__ )\
                       .filter( Request.RequestName == requestName )\
                       .filter( File._LFN.in_( lfnList ) )\
                       .all()

      for lfn, status in requestRet:
        res[lfn] = status
      return S_OK( res )

    except Exception, e:
      self.log.exception( "getRequestFileStatus: unexpected exception", lException = e )
      return S_ERROR( "getRequestFileStatus: unexpected exception : %s" % e )
    finally:
      session.close()


  def getRequestInfo( self, requestNameOrID ):
    """ get request info given Request.RequestID """

    session = self.DBSession()

    try:

      requestInfoQuery = session.query( Request.RequestID, Request._Status, Request.RequestName,
                                        Request.JobID, Request.OwnerDN, Request.OwnerGroup,
                                        Request.DIRACSetup, Request.SourceComponent, Request._CreationTime,
                                        Request._SubmitTime, Request._LastUpdate )

      if type( requestNameOrID ) == int:
        requestInfoQuery = requestInfoQuery.filter( Request.RequestID == requestNameOrID )
      else:
        requestInfoQuery = requestInfoQuery.filter( Request.RequestName == requestNameOrID )

      try:
        requestInfo = requestInfoQuery.one()
      except NoResultFound, e:
        return S_ERROR( 'No such request' )

      return S_OK( tuple( requestInfo ) )

    except Exception, e:
      self.log.exception( "getRequestInfo: unexpected exception", lException = e )
      return S_ERROR( "getRequestInfo: unexpected exception : %s" % e )

    finally:
      session.close()

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
