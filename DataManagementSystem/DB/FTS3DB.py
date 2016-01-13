########################################################################
# File: RequestDB.py
# Date: 2012/12/04 08:06:30
########################################################################
__RCSID__ = "$Id $"

# pylint: disable=no-member

import random

import datetime
# # from DIRAC
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.DataManagementSystem.Client.FTS3Operation import FTS3Operation, FTS3TransferOperation, FTS3StagingOperation
from DIRAC.DataManagementSystem.Client.FTS3File import FTS3File
from DIRAC.DataManagementSystem.Client.FTS3Job import FTS3Job
from DIRAC.ConfigurationSystem.Client.Utilities import getDBParameters

from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm.util import polymorphic_union
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql.expression import and_
from sqlalchemy.orm import relationship, backref, sessionmaker, joinedload_all, mapper, column_property, with_polymorphic
from sqlalchemy.sql import update
from sqlalchemy import create_engine, func, Table, Column, MetaData, ForeignKey, \
                       Integer, String, DateTime, Enum, BLOB, BigInteger, distinct, SmallInteger, select, Float

import copy

metadata = MetaData()


fts3FileTable = Table( 'Files', metadata,
                       Column( 'fileID', Integer, primary_key = True ),
                       Column( 'operationID', Integer,
                                ForeignKey( 'Operations.operationID', ondelete = 'CASCADE' ),
                                nullable = False ),
                       Column( 'attempt', Integer, server_default = '0' ),
                       Column( 'lastUpdate', DateTime ),
                       Column( 'rmsFileID', Integer, server_default = '0' ),
                       Column( 'lfn', String( 1024 ) ),
                       Column( 'checksum', String( 255 ) ),
                       Column( 'size', BigInteger ),
                       Column( 'targetSE', String( 255 ), nullable = False ),
                       Column( 'error', String( 1024 ) ),
                       Column( 'status', Enum( *FTS3File.ALL_STATES ),
                               server_default = FTS3File.INIT_STATE,
                               index = True ),
                       mysql_engine = 'InnoDB',
                     )

mapper( FTS3File, fts3FileTable )



fts3JobTable = Table( 'Jobs', metadata,
                      Column( 'jobID', Integer, primary_key = True ),
                      Column( 'operationID', Integer,
                               ForeignKey( 'Operations.operationID', ondelete = 'CASCADE' ),
                               nullable = False ),
                      Column( 'submitTime', DateTime ),
                      Column( 'lastUpdate', DateTime ),
                      Column( 'lastMonitor', DateTime ),
                      Column( 'completeness', Float ),
                      Column( 'username', String( 255 ) ),  # Could be fetched from Operation, but bad for perf
                      Column( 'userGroup', String( 255 ) ),  # Could be fetched from Operation, but bad for perf
                      Column( 'ftsGUID', String( 255 ) ),
                      Column( 'ftsServer', String( 255 ) ),
                      Column( 'error', String( 1024 ) ),
                      Column( 'status', Enum( *FTS3Job.ALL_STATES ),
                              server_default = FTS3Job.INIT_STATE,
                              index = True ),
                      Column( 'assignment', String( 255 ), server_default = None ),
                      mysql_engine = 'InnoDB',
                     )

mapper( FTS3Job, fts3JobTable )



fts3OperationTable = Table( 'Operations', metadata,
                            Column( 'operationID', Integer, primary_key = True ),
                            Column( 'username', String( 255 ) ),
                            Column( 'userGroup', String( 255 ) ),
                            Column( 'rmsReqID', Integer, server_default = '-1' ),  # -1 because with 0 we get any request
                            Column( 'rmsOpID', Integer, server_default = '0' ),
                            Column( 'sourceSEs', String( 255 ) ),
                            Column( 'activity', String( 255 ) ),
                            Column( 'priority', SmallInteger ),
                            Column( 'creationTime', DateTime ),
                            Column( 'lastUpdate', DateTime ),
                            Column( 'status', Enum( *FTS3Operation.ALL_STATES ),
                                              server_default = FTS3Operation.INIT_STATE,
                                              index = True ),
                           Column( 'error', String( 1024 ) ),
                           Column( 'type', String( 255 ) ),
                           Column( 'assignment', String( 255 ), server_default = None ),
                           mysql_engine = 'InnoDB',
                          )


fts3Operation_mapper = mapper( FTS3Operation, fts3OperationTable,
        properties = {'ftsFiles':relationship( FTS3File,
                                                lazy = 'joined',  # Immediately load the entirety of the object
                                                innerjoin = True,  # Use inner join instead of left outer join
                                                cascade = 'all, delete-orphan',  # if a File is removed from the list, remove it from the DB
                                                passive_deletes = True,  # used together with cascade='all, delete-orphan'
                                               ),
                      'ftsJobs':relationship( FTS3Job,
                                              lazy = 'joined',  # Immediately load the entirety of the object
                                              cascade = 'all, delete-orphan',  # if a File is removed from the list, remove it from the DB
                                              passive_deletes = True,  # used together with cascade='all, delete-orphan'
                                            ),
                      },
        polymorphic_on = 'type',
        polymorphic_identity = 'Abs'
        )

mapper( FTS3TransferOperation, fts3OperationTable,
        inherits = fts3Operation_mapper,
        polymorphic_identity = 'Transfer'
        )

mapper( FTS3StagingOperation, fts3OperationTable,
        inherits = fts3Operation_mapper,
        polymorphic_identity = 'Staging'
        )



########################################################################
class FTS3DB( object ):
  """
  .. class:: RequestDB

  db holding requests
  """


  def __getDBConnectionInfo( self, fullname ):
    """ Collect from the CS all the info needed to connect to the DB.
        This should be in a base class eventually
    """

    result = getDBParameters( fullname )
    if not result[ 'OK' ]:
      raise Exception( 'Cannot get database parameters: %s' % result[ 'Message' ] )

    dbParameters = result[ 'Value' ]
    self.dbHost = dbParameters[ 'Host' ]
    self.dbPort = dbParameters[ 'Port' ]
    self.dbUser = dbParameters[ 'User' ]
    self.dbPass = dbParameters[ 'Password' ]
    self.dbName = dbParameters[ 'DBName' ]


  def __init__( self ):
    """c'tor

    :param self: self reference
    """

    self.log = gLogger.getSubLogger( 'FTS3DB' )
    # Initialize the connection info
    self.__getDBConnectionInfo( 'DataManagement/FTS3DB' )



    runDebug = ( gLogger.getLevel() == 'DEBUG' )
    self.engine = create_engine( 'mysql://%s:%s@%s:%s/%s' % ( self.dbUser, self.dbPass, self.dbHost, self.dbPort, self.dbName ),
                                 echo = runDebug )

    metadata.bind = self.engine

    self.dbSession = sessionmaker( bind = self.engine )



#   def __init__( self ):
#     """c'tor
#
#     :param self: self reference
#     """
#
#     self.log = gLogger.getSubLogger( 'FTS3DB' )
#     # Initialize the connection info
#
#
#
#     self.engine = create_engine( 'mysql://Dirac:Dirac@localhost:3306/FTS3DB' ,
#                                  echo = True )
#
#     metadata.bind = self.engine
#
#     self.dbSession = sessionmaker( bind = self.engine )

  def createTables( self ):
    """ create tables """
    try:
      metadata.create_all( self.engine )
    except SQLAlchemyError, e:
      return S_ERROR( e )
    return S_OK()
  

  def persistOperation( self, operation ):
    """ update or insert request into db
        Also release the assignment tag

    :param operation: FTS3Operation instance
    """
    
    session = self.dbSession( expire_on_commit = False )

    # set the assignment to NULL
    # so that another agent can work on the request
    operation.assignment = None
    try:

      # Merge it in case it already is in the DB
      operation = session.merge( operation )
      session.add( operation )
      session.commit()
      session.expunge_all()
  
      return S_OK( operation.operationID )

    except SQLAlchemyError, e:
      session.rollback()
      self.log.exception( "persistOperation: unexpected exception", lException = e )
      return S_ERROR( "persistOperation: unexpected exception %s" % e )
    finally:
      session.close()



  def getOperation( self, operationID ):
    """ read request

      This does not set the assignment flag

    :param operationID: ID of the FTS3Operation

    """

    # expire_on_commit is set to False so that we can still use the object after we close the session
    session = self.dbSession( expire_on_commit = False )

    try:
      # the joinedload_all is to force the non-lazy loading of all the attributes
#       operation = session.query( FTS3Operation )\
#                          .options( joinedload_all( 'ftsFiles' ) )\
#                          .options( joinedload_all( 'ftsJobs' ) )\
#                          .filter( getattr( FTS3Operation, 'operationID' ) == operationID )\
#                         .one()
      operation = session.query( FTS3Operation )\
                         .filter( getattr( FTS3Operation, 'operationID' ) == operationID )\
                         .one()

      session.commit()
      
      ###################################
      session.expunge_all()
      return S_OK( operation )

    except NoResultFound, e:
      return S_ERROR( "No FTS3Operation with id %s" % operationID )
    except SQLAlchemyError, e:
      return S_ERROR( "getOperation: unexpected exception : %s" % e )
    finally:
      session.close()

  def getActiveJobs( self, limit = 20, lastMonitor = None, jobAssignmentTag = "Assigned" ):
    """ Get  the FTSJobs that are not in a final state, and are not assigned for monitoring
        or has its operation being treated

        By assigning the job to the DB:
          * it cannot be monitored by another agent
          * the operation to which it belongs cannot be treated

       :param limit: max number of Jobs to retrieve
       :param lastMonitor: jobs monitored earlier than the given date
       :param jobAssignmentTag: if not None, block the Job for other queries,
                              and use it as a prefix for the value in the operation table

       :returns: list of FTS3Jobs

    """

    session = self.dbSession( expire_on_commit = False )

    try:
      # the tild sign is for "not"
      ftsJobsQuery = session.query( FTS3Job )\
                       .join( FTS3Operation )\
                       .filter( ~FTS3Job.status.in_( FTS3Job.FINAL_STATES ) )\
                       .filter( FTS3Job.assignment == None )\
                       .filter( FTS3Operation.assignment == None )\

      if lastMonitor:
        ftsJobsQuery = ftsJobsQuery.filter( FTS3Job.lastMonitor < lastMonitor )

      if jobAssignmentTag:
        ftsJobsQuery = ftsJobsQuery.with_for_update()



      ftsJobsQuery = ftsJobsQuery.limit( limit )

      ftsJobs = ftsJobsQuery.all()


      if jobAssignmentTag:
        jobAssignmentTag += "_%s" % datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )


        jobIds = [job.jobID for job in ftsJobs]
        if jobIds:
          session.execute( update( FTS3Job )\
                     .where( FTS3Job.jobID.in_( jobIds )
                             )\
                     .values( { 'assignment' : jobAssignmentTag} )
                   )

      session.commit()

      session.expunge_all()

      return S_OK( ftsJobs )

    except SQLAlchemyError, e:
      session.rollback()
      return S_ERROR( "getAllActiveJobs: unexpected exception : %s" % e )
    finally:
      session.close()
      
  def updateFileStatus( self, fileStatusDict ):
    """Update the file ftsStatus and error
        The update is only done if the file is not in a final state



       :param fileStatusDict : { fileID : { status , error } }

    """
    session = self.dbSession()
    try:
      
      for fileID, valueDict in fileStatusDict.iteritems():

        updateDict = {FTS3File.status : valueDict['status']}
        
        # We only update error if it is specified
        if 'error' in valueDict:
          newError = valueDict['error']
          # Replace empty string with None
          if not newError:
            newError = None
          updateDict[FTS3File.error] = newError

        session.execute( update( FTS3File )\
                         .where( and_( FTS3File.fileID == fileID,
                                       ~ FTS3File.status.in_( FTS3File.FINAL_STATES )
                                      )\
                                )\
                         .values( updateDict )
                       )
#         session.execute( update( FTS3File )\
#                          .where( FTS3File.fileID == fileID
#
#                                 )\
#                          .values( {FTS3File.status : valueDict['status'],  # pylint: disable=no-member
#                                    FTS3File.error : valueDict.get( 'error' ),  # pylint: disable=no-member
#                                   }
#                                  )
#                         )
      session.commit()

      return S_OK()

    except SQLAlchemyError, e:
      session.rollback()
      self.log.exception( "updateFileFtsStatus: unexpected exception", lException = e )
      return S_ERROR( "updateFileFtsStatus: unexpected exception %s" % e )
    finally:
      session.close()


  def updateJobStatus( self, jobStatusDict ):
    """ Update the job Status and error
        The update is only done if the job is not in a final state
        The assignment flag is released

       :param jobStatusDict : { jobID : { status , error, completeness } }
    """
    session = self.dbSession()
    try:

      for jobID, valueDict in jobStatusDict.iteritems():
        
        updateDict = {FTS3Job.status : valueDict['status']}
        
        # We only update error if it is specified
        if 'error' in valueDict:
          newError = valueDict['error']
          # Replace empty string with None
          if not newError:
            newError = None
          updateDict[FTS3Job.error] = newError

        if 'completeness' in valueDict:
          updateDict[FTS3Job.completeness] = valueDict['completeness']

        updateDict[FTS3Job.assignment] = None

        
        session.execute( update( FTS3Job )\
                         .where( and_( FTS3Job.jobID == jobID,
                                      ~ FTS3Job.status.in_( FTS3Job.FINAL_STATES )
                                      )
                                 )\
                         .values( updateDict )
                       )
      session.commit()

      return S_OK()

    except SQLAlchemyError, e:
      session.rollback()
      self.log.exception( "updateJobStatus: unexpected exception", lException = e )
      return S_ERROR( "updateJobStatus: unexpected exception %s" % e )
    finally:
      session.close()

#   def getProcessedOperations( self, limit = 20 ):
#     """ Get all the FTS3Operations that are missing a callback, i.e.
#         in 'Processed' state
#         :param limit: max number of operations to retrieve
#         :return: list of Operations
#     """
#
#     session = self.dbSession( expire_on_commit = False )
#
#     try:
#
#       ftsOperations = []
#
#       # We need to do the select in two times because the join clause that makes the limit difficult
#       operationIDs = session.query( FTS3Operation.operationID )\
#                         .filter( FTS3Operation.status == 'Processed' )\
#                         .limit( limit )\
#                         .all()
#
#       operationIDs = [oidTuple[0] for oidTuple in operationIDs]
#
#       if operationIDs:
#         # Fetch the operation object for these IDs
#         ftsOperations = session.query( FTS3Operation )\
#                           .filter( FTS3Operation.operationID.in_( operationIDs ) )\
#                           .all()
#
#
#       session.expunge_all()
#
#       return S_OK( ftsOperations )
#
#     except SQLAlchemyError, e:
#       session.rollback()
#       return S_ERROR( "getAllProcessedOperations: unexpected exception : %s" % e )
#     finally:
#       session.close()


  def getNonFinishedOperations( self, limit = 20, operationAssignmentTag = "Assigned" ):
    """ Get all the non assigned FTS3Operations that are not yet finished, so either Active or Processed.
        An operation won't be picked if it is already assigned, or one of its job is.

        :param limit: max number of operations to retrieve
        :param operationAssignmentTag: if not None, block the operations for other queries,
                              and use it as a prefix for the value in the operation table
        :return: list of Operations
    """

    session = self.dbSession( expire_on_commit = False )

    try:

      ftsOperations = []

      # We need to do the select in two times because the join clause that makes the limit difficult
      operationIDsQuery = session.query( FTS3Operation.operationID )\
                        .outerjoin( FTS3Job )\
                        .filter( FTS3Operation.status.in_( ['Active', 'Processed'] ) )\
                        .filter( FTS3Operation.assignment == None )\
                        .filter( FTS3Job.assignment == None )\
                        .limit( limit )

      # Block the Operations for other requests
      if operationAssignmentTag:
        operationIDsQuery = operationIDsQuery.with_for_update()
        
      operationIDs = operationIDsQuery.all()

      operationIDs = [oidTuple[0] for oidTuple in operationIDs]

      if operationIDs:
        # Fetch the operation object for these IDs
        ftsOperations = session.query( FTS3Operation )\
                          .filter( FTS3Operation.operationID.in_( operationIDs ) )\
                          .all()

        if operationAssignmentTag:
          operationAssignmentTag += "_%s" % datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )



          session.execute( update( FTS3Operation )\
                     .where( FTS3Operation.operationID.in_( operationIDs )
                             )\
                     .values( { 'assignment' : operationAssignmentTag} )
                   )

      session.commit()
      session.expunge_all()

      return S_OK( ftsOperations )

    except SQLAlchemyError, e:
      session.rollback()
      return S_ERROR( "getAllProcessedOperations: unexpected exception : %s" % e )
    finally:
      session.close()




  # USELESS ?
#
#   def getOperationsWithFilesToSubmit( self, limit = 20 ):
#     """ Get all the FTS3Operations that have files in New or Failed state
#         (reminder: Failed is NOT terminal for files. Failed is when fts failed, but we
#          can retry)
#         :param limit: max number of operations to retrieve
#         :return: list of Operations
#     """
#
#     session = self.dbSession( expire_on_commit = False )
#
#     try:
#       ftsOperations = []
#
#       # unfortunately we cannot use subquery because even the latest MySQL/MariaDB
#       # versions do not support using limit statement in subqueries
#
#       # Find all the operationIDs that have files in state New and Failed
#       operationIDs = session.query( FTS3File.operationID )\
#                         .distinct( FTS3File.operationID )\
#                         .filter( FTS3File.status.in_( ( 'New', 'Failed' ) ) )\
#                         .limit( limit )\
#                         .all()
#
#       operationIDs = [oidTuple[0] for oidTuple in operationIDs]
#
#       if operationIDs:
#         # Fetch the operation object for these IDs
#         ftsOperations = session.query( FTS3Operation )\
#                           .options( joinedload_all( 'ftsFiles' ) )\
#                           .options( joinedload_all( 'ftsJobs' ) )\
#                           .filter( FTS3Operation.operationID.in_( operationIDs ) )\
#                           .all()
#
#
#       session.expunge_all()
#
#       return S_OK( ftsOperations )
#
#     except SQLAlchemyError, e:
#       session.rollback()
#       return S_ERROR( "getAllNonFinishedOperations: unexpected exception : %s" % e )
#     finally:
#       session.close()
