########################################################################
# File: FTSDB.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/04/02 15:13:51
########################################################################
""" :mod: FTSDB
    ===========

    .. module: FTSDB
    :synopsis: FTS DB
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    FTS DB
"""

# #
# @file FTSDB.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/04/02 15:13:57
# @brief Definition of FTSDB class.

# # imports
# Get rid of the annoying Deprecation warning of the current MySQLdb
# FIXME: compile a newer MySQLdb version
import warnings
with warnings.catch_warnings():
  warnings.simplefilter( 'ignore', DeprecationWarning )
  import MySQLdb.cursors

import decimal
from MySQLdb import Error as MySQLdbError
# # from DIRAC
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Base.DB import DB
from DIRAC.Core.Utilities.LockRing import LockRing
from DIRAC.Core.Utilities.List import stringListToString, intListToString
# # ORMs
from DIRAC.DataManagementSystem.Client.FTSJob import FTSJob
from DIRAC.DataManagementSystem.Client.FTSFile import FTSFile
from DIRAC.DataManagementSystem.private.FTSHistoryView import FTSHistoryView

########################################################################
class FTSDB( DB ):
  """
  .. class:: FTSDB

  database holding FTS jobs and their files
  """

  def __init__( self, systemInstance = "Default", maxQueueSize = 10 ):
    """c'tor

    :param self: self reference
    :param str systemInstance: ???
    :param int maxQueueSize: size of queries queue
    """
    DB.__init__( self, "FTSDB", "DataManagement/FTSDB", maxQueueSize )
#    self.log = gLogger.getSubLogger( "DataManagement/FTSDB" )
    # # private lock
    self.getIdLock = LockRing().getLock( "FTSDBLock" )
    # # max attempt for reschedule
    self.maxAttempt = 100
    # # check tables

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

  def getTables( self ):
    """ get tables """
    showTables = self._query( "SHOW TABLES;" )
    if not showTables['OK']:
      return showTables
    return S_OK( [ table[0] for table in showTables['Value'] if table and table != "FTSHistoryView" ] )

  @staticmethod
  def getTableMeta():
    """ get db schema in a dict format """
    return dict( [ ( classDef.__name__, classDef.tableDesc() )
                   for classDef in ( FTSJob, FTSFile ) ] )
  @staticmethod
  def getViewMeta():
    """ return db views in dict format

    at the moment only one view - FTSHistoryView
    """
    return { 'FTSHistoryView': FTSHistoryView.viewDesc() }

  def createViews( self, force = False ):
    """ create views """
    return self._createViews( self.getViewMeta(), force )

  def _checkTables( self, force = False ):
    """ create tables if not existing

    :param bool force: flag to trigger recreation of db schema
    """
    return self._createTables( self.getTableMeta(), force = force )

  def dictCursor( self, conn = None ):
    """ get dict cursor for connection :conn:

    :return: S_OK( { "cursor": cursors.DictCursor, "connection" : connection  } ) or S_ERROR
    """
    if not conn:
      retDict = self._getConnection()
      if not retDict['OK']:
        return retDict
      conn = retDict['Value']
    cursor = conn.cursor( cursorclass = cursors.DictCursor )
    return S_OK( { "cursor" : cursor, "connection" : conn  } )

  def _transaction( self, queries, connection = None ):
    """ execute transaction """
    queries = [ queries ] if type( queries ) == str else queries
    # # get cursor and connection
    getCursorAndConnection = self.dictCursor( connection )
    if not getCursorAndConnection['OK']:
      return getCursorAndConnection
    cursor = getCursorAndConnection['Value']["cursor"]
    connection = getCursorAndConnection['Value']["connection"]

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
      ret['Value'] = queryRes
      ret["lastrowid"] = lastrowid
      connection.autocommit( True )
      return ret
    except MySQLdbError, error:
      self.log.exception( error )
      # # roll back
      connection.rollback()
      # # revert auto commit
      connection.autocommit( True )
      # # close cursor
      cursor.close()
      return S_ERROR( str( error ) )

  def putFTSSite( self, ftsSite ):
    """ put FTS site into DB """
    if not ftsSite.FTSSiteID:
      existing = self._query( "SELECT `FTSSiteID` FROM `FTSSite` WHERE `Name` = '%s'" % ftsSite.Name )
      if not existing["OK"]:
        self.log.error( "putFTSSite: %s" % existing["Message"] )
        return existing
      existing = existing["Value"]
      if existing:
        return S_ERROR( "putFTSSite: site of '%s' name is already defined at FTSSiteID = %s" % ( ftsSite.Name,
                                                                                                 existing[0][0] ) )
    ftsSiteSQL = ftsSite.toSQL()
    if not ftsSiteSQL["OK"]:
      self.log.error( "putFTSSite: %s" % ftsSiteSQL["Message"] )
      return ftsSiteSQL
    ftsSiteSQL = ftsSiteSQL["Value"]

    putFTSSite = self._transaction( ftsSiteSQL )
    if not putFTSSite["OK"]:
      self.log.error( putFTSSite["Message"] )
    return putFTSSite

  def getFTSSite( self, ftsSiteID ):
    """ read FTSSite given FTSSiteID """
    getFTSSiteQuery = "SELECT * FROM `FTSSite` WHERE `FTSSiteID`=%s" % int( ftsSiteID )
    getFTSSite = self._transaction( [ getFTSSiteQuery ] )
    if not getFTSSite["OK"]:
      self.log.error( "getFTSSite: %s" % getFTSSite["Message"] )
      return getFTSSite
    getFTSSite = getFTSSite["Value"]
    if getFTSSiteQuery in getFTSSite and getFTSSite[getFTSSiteQuery]:
      getFTSSite = FTSSite( getFTSSite[getFTSSiteQuery][0] )
      return S_OK( getFTSSite )
    # # if we land here FTSSite does not exist
    return S_OK()

  def deleteFTSSite( self, ftsSiteID ):
    """ delete FTSSite given its FTSSiteID """
    delete = "DELETE FROM `FTSSite` WHERE `FTSSiteID` = %s;" % int( ftsSiteID )
    delete = self._transaction( [ delete ] )
    if not delete["OK"]:
      self.log.error( delete["Message"] )
    return delete

  def getFTSSitesList( self ):
    """ bulk read of FTS sites """
    ftsSitesQuery = "SELECT * FROM `FTSSite`;"
    ftsSites = self._transaction( [ ftsSitesQuery ] )
    if not ftsSites["OK"]:
      self.log.error( "getFTSSites: %s" % ftsSites["Message"] )
      return ftsSites
    ftsSites = ftsSites["Value"][ftsSitesQuery] if ftsSitesQuery in ftsSites["Value"] else []
    return S_OK( [ FTSSite( ftsSiteDict ) for ftsSiteDict  in ftsSites ] )

  def putFTSFile( self, ftsFile ):
    """ put FTSFile into fts db """
    ftsFileSQL = ftsFile.toSQL()
    if not ftsFileSQL['OK']:
      self.log.error( ftsFileSQL['Message'] )
      return ftsFileSQL
    ftsFileSQL = ftsFileSQL['Value']
    putFTSFile = self._transaction( ftsFileSQL )
    if not putFTSFile['OK']:
      self.log.error( putFTSFile['Message'] )
    return putFTSFile

  def getFTSFile( self, ftsFileID ):
    """ read FTSFile from db given FTSFileID """
    select = "SELECT * FROM `FTSFile` WHERE `FTSFileID` = %s;" % ftsFileID
    select = self._transaction( [ select ] )
    if not select['OK']:
      self.log.error( select['Message'] )
      return select
    select = select['Value']
    if not select.values()[0]:
      return S_OK()
    ftsFile = FTSFile( select.values()[0][0] )
    return S_OK( ftsFile )

  def deleteFTSFiles( self, operationID, opFileIDList = None ):
    """ delete FTSFiles for reschedule

    :param int operationID: ReqDB.Operation.OperationID
    :param list opFileIDList: [ ReqDB.File.FileID, ... ]
    """
    query = [ "DELETE FROM `FTSFile` WHERE OperationID = %s" % operationID ]
    if opFileIDList:
      query.append( " AND `FileID` IN (%s)" % intListToString( opFileIDList ) )
    query.append( ";" )
    return self._update( "".join( query ) )

  def getFTSJobsForRequest( self, requestID, statusList = None ):
    """ get list of FTSJobs with status in :statusList: for request given its requestID

    TODO: should be more smart, i.e. one query to select all ftsfiles
    """

    statusList = statusList if statusList  else list( FTSJob.INITSTATES + FTSJob.TRANSSTATES )
    query = "SELECT * FROM `FTSJob` WHERE `RequestID` = %s AND `Status` in (%s)" % ( requestID,
                                                                                     stringListToString( statusList ) )
    ftsJobs = self._transaction( [ query ] )
    if not ftsJobs['OK']:
      self.log.error( "getFTSJobsForRequest: %s" % ftsJobs['Message'] )
      return ftsJobs

    ftsJobs = ftsJobs['Value'][query] if query in ftsJobs['Value'] else []

    ftsJobs = [ FTSJob( ftsJobDict ) for ftsJobDict in ftsJobs  ]

    for ftsJob in ftsJobs:
      query = "SELECT * FROM `FTSFile` WHERE `FTSGUID` = '%s' AND `RequestID`=%s" % ( ftsJob.FTSGUID,
                                                                                      requestID )
      ftsFiles = self._transaction( [ query ] )
      if not ftsFiles['OK']:
        self.log.error( "getFTSJobsForRequest: %s" % ftsFiles['Message'] )
        return ftsFiles
      ftsFiles = ftsFiles['Value'][query] if query in ftsFiles['Value'] else []
      for ftsFileDict in ftsFiles:
        ftsJob.addFile( FTSFile( ftsFileDict ) )
    return S_OK( ftsJobs )

  def getFTSFilesForRequest( self, requestID, statusList = None ):
    """ get FTSFiles with status in :statusList: for request given its :requestID: """
    requestID = int( requestID )
    statusList = statusList if statusList else [ "Waiting" ]
    query = "SELECT * FROM `FTSFile` WHERE `RequestID` = %s AND `Status` IN (%s);" % ( requestID,
                                                                                       stringListToString( statusList ) )
    ftsFiles = self._transaction( [ query ] )
    if not ftsFiles['OK']:
      self.log.error( "getFTSFilesForRequest: %s" % ftsFiles['Message'] )
      return ftsFiles
    ftsFiles = ftsFiles['Value'][query] if query in ftsFiles['Value'] else []
    return S_OK( [ FTSFile( ftsFileDict ) for ftsFileDict in ftsFiles ] )

  def setFTSFilesWaiting( self, operationID, sourceSE, opFileIDList = None ):
    """ propagate states for descendants in replication tree

    :param int operationID: ReqDB.Operation.OperationID
    :param str sourceSE: waiting source SE
    :param list opFileIDList: [ ReqDB.File.FileID, ... ]
    """
    operationID = int( operationID )
    if opFileIDList:
      opFileIDList = [ int( opFileID ) for opFileID in opFileIDList ]
    status = "Waiting#%s" % sourceSE
    query = "UPDATE `FTSFile` SET `Status` = 'Waiting' WHERE `Status` = '%s' AND `OperationID` = %s " % ( status,
                                                                                                          operationID )
    if opFileIDList:
      query = query + "AND `FileID` IN (%s)" % intListToString( opFileIDList )
    return self._update( query )

  def peekFTSFile( self, ftsFileID ):
    """ peek FTSFile given FTSFileID """
    return self.getFTSFile( ftsFileID )

  def putFTSJob( self, ftsJob ):
    """ put FTSJob to the db (INSERT or UPDATE)

    :param FTSJob ftsJob: FTSJob instance
    """
    ftsJobSQL = ftsJob.toSQL()
    if not ftsJobSQL['OK']:
      return ftsJobSQL
    putJob = [ ftsJobSQL['Value'] ]

    for ftsFile in [ ftsFile.toSQL() for ftsFile in ftsJob ]:
      if not ftsFile['OK']:
        return ftsFile
      putJob.append( ftsFile['Value'] )

    putJob = self._transaction( putJob )
    if not putJob['OK']:
      self.log.error( putJob['Message'] )
    return putJob

  def getFTSJob( self, ftsJobID = None ):
    """ get FTSJob given FTSJobID """

    getJob = [ "SELECT * FROM `FTSJob` WHERE `FTSJobID` = %s;" % ftsJobID ]
    getJob = self._transaction( getJob )
    if not getJob['OK']:
      self.log.error( getJob['Message'] )
      return getJob
    getJob = getJob['Value']
    if not getJob:
      return S_OK()
    ftsJob = FTSJob( getJob.values()[0][0] )
    selectFiles = self._transaction( [ "SELECT * FROM `FTSFile` WHERE `FTSGUID` = '%s';" % ftsJob.FTSGUID ] )
    if not selectFiles['OK']:
      self.log.error( selectFiles['Message'] )
      return selectFiles
    selectFiles = selectFiles['Value']
    ftsFiles = [ FTSFile( item ) for item in selectFiles.values()[0] ]
    for ftsFile in ftsFiles:
      ftsJob.addFile( ftsFile )
    return S_OK( ftsJob )

  def setFTSJobStatus( self, ftsJobID, status ):
    """ Set the status of an FTS job
    """
    setAssigned = "UPDATE `FTSJob` SET `Status`='%s' WHERE `FTSJobID` = %s;" % ( status, ftsJobID )
    setAssigned = self._update( setAssigned )
    if not setAssigned['OK']:
      self.log.error( setAssigned['Message'] )
      return setAssigned
    return setAssigned

  def deleteFTSJob( self, ftsJobID ):
    """ delete FTSJob given ftsJobID """
    delete = "DELETE FROM `FTSJob` WHERE `FTSJobID` = %s;" % ftsJobID
    delete = self._transaction( [ delete ] )
    if not delete['OK']:
      self.log.error( delete['Message'] )
    return delete

  def getFTSJobIDs( self, statusList = [ "Submitted", "Active", "Ready" ] ):
    """ get FTSJobIDs for  a given status list """
    query = "SELECT `FTSJobID` FROM `FTSJob` WHERE `Status` IN (%s);" % stringListToString( statusList )
    query = self._query( query )
    if not query['OK']:
      self.log.error( query['Message'] )
      return query
    # # convert to list of longs
    return S_OK( [ item[0] for item in query['Value'] ] )

  def getFTSFileIDs( self, statusList = None ):
    """ select FTSFileIDs for a given status list """
    statusList = statusList if statusList else [ "Waiting" ]
    query = "SELECT `FTSFileID` FROM `FTSFile` WHERE `Status` IN (%s);" % stringListToString( statusList );
    query = self._query( query )
    if not query['OK']:
      self.log.error( query['Message'] )
      return query
    return S_OK( [ item[0] for item in query['Value'] ] )

  def getFTSJobList( self, statusList = None, limit = 500 ):
    """ select FTS jobs with statuses in :statusList: """
    statusList = statusList if statusList else list( FTSJob.INITSTATES + FTSJob.TRANSSTATES )
    query = "SELECT * FROM `FTSJob` WHERE `Status` IN (%s) ORDER BY `LastUpdate` DESC LIMIT %s;" % ( stringListToString( statusList ),
                                                                                                     limit )
    trn = self._transaction( [ query ] )
    if not trn['OK']:
      self.log.error( "getFTSJobList: %s" % trn['Message'] )
      return trn
    ftsJobs = [ FTSJob( ftsJobDict ) for ftsJobDict in trn['Value'][query] ]
    for ftsJob in ftsJobs:
      query = "SELECT * FROM `FTSFile` WHERE `FTSGUID` = '%s';" % ftsJob.FTSGUID
      trn = self._transaction( query )
      if not trn['OK']:
        self.log.error( "getFTSJobList: %s" % trn['Message'] )
        return trn
      ftsFiles = [ FTSFile( ftsFileDict ) for ftsFileDict in trn['Value'][query] ]
      for ftsFile in ftsFiles:
        ftsJob.addFile( ftsFile )
    return S_OK( ftsJobs )

  def putFTSFileList( self, ftsFileList ):
    """ bulk put of FSTFiles

    :param list ftsFileList: list with FTSFile instances
    """
    queries = []
    for ftsFile in ftsFileList:
      ftsFileSQL = ftsFile.toSQL()
      if not ftsFileSQL['OK']:
        gLogger.error( "putFTSFileList: %s" % ftsFileSQL['Message'] )
        return ftsFileSQL
      queries.append( ftsFileSQL['Value'] )
    if not queries:
      return S_ERROR( "putFTSFileList: no queries to put" )

    put = self._transaction( queries )
    if not put['OK']:
      gLogger.error( "putFTSFileList: %s" % put['Message'] )
    return put

  def getFTSFileList( self, statusList = None, limit = 1000 ):
    """ get at most :limit: FTSFiles with status in :statusList:

    :param list statusList: list with FTSFiles statuses
    :param int limit: select query limit
    """
    statusList = statusList if statusList else [ "Waiting" ]
    reStatus = []
    inStatus = []
    for status in statusList:
      if "%" in status or ".*" in status or ".+" in status:
        reStatus.append( status )
      else:
        inStatus.append( status )
    reQuery = "`Status` REGEXP '%s'" % "|".join( reStatus ) if reStatus else ""
    inQuery = "`Status` IN (%s)" % stringListToString( inStatus ) if inStatus else ""
    whereClause = " OR ".join( [ q for q in ( reQuery, inQuery ) if q ] )
    if whereClause:
      whereClause = "WHERE %s" % whereClause
    query = "SELECT * FROM `FTSFile` %s ORDER BY `LastUpdate` DESC LIMIT %s;" % ( whereClause, limit )
    trn = self._transaction( [query] )
    if not trn['OK']:
      self.log.error( "getFTSFileList: %s" % trn['Message'] )
      return trn
    return S_OK( [ FTSFile( fileDict ) for fileDict in trn['Value'][query] ] )

  def getFTSHistory( self ):
    """ query FTSHistoryView, return list of FTSHistoryViews """
    query = self._transaction( [ "SELECT * FROM `FTSHistoryView`;" ] )
    if not query['OK']:
      return query
    if not query['Value']:
      return S_OK()
    return S_OK( [ FTSHistoryView( fromDict ) for fromDict in query['Value'].values()[0] ] )

  def cleanUpFTSFiles( self, requestID, fileIDs ):
    """ delete FTSFiles for given :requestID: and list of :fileIDs:

    :param int requestID: ReqDB.Request.RequestID
    :param list fileIDs: [ ReqDB.File.FileID, ... ]
    """
    query = "DELETE FROM `FTSFile` WHERE `RequestID`= %s and `FileID` IN (%s)" % ( requestID,
                                                                                   intListToString( fileIDs ) )
    deleteFiles = self._transaction( [query] )
    return deleteFiles

  def getDBSummary( self ):
    """ get DB summary """
    # # this will be returned
    retDict = { "FTSJob": {}, "FTSFile": {}, "FTSHistory": {} }
    transQueries = { "SELECT `Status`, COUNT(`Status`) FROM `FTSJob` GROUP BY `Status`;" : "FTSJob",
                     "SELECT `Status`, COUNT(`Status`) FROM `FTSFile` GROUP BY `Status`;" : "FTSFile",
                     "SELECT * FROM `FTSHistoryView`;": "FTSHistory" }
    ret = self._transaction( transQueries.keys() )

    if not ret['OK']:
      self.log.error( "getDBSummary: %s" % ret['Message'] )
      return ret
    ret = ret['Value']
    for k, v in ret.items():
      if transQueries[k] == "FTSJob":
        for aDict in v:
          status = aDict.get( "Status" )
          count = aDict.get( "COUNT(`Status`)" )
          if status not in retDict["FTSJob"]:
            retDict["FTSJob"][status] = 0
          retDict["FTSJob"][status] += count
      elif transQueries[k] == "FTSFile":
        for aDict in v:
          status = aDict.get( "Status" )
          count = aDict.get( "COUNT(`Status`)" )
          if status not in retDict["FTSFile"]:
            retDict["FTSFile"][status] = 0
          retDict["FTSFile"][status] += count
      else:  # # FTSHistory

        if v:
          newListOfHistoryDicts = []
          for oldHistoryDict in v:
            newHistoryDict = {}
            for key, value in oldHistoryDict.items():
              if type( value ) == decimal.Decimal:
                newHistoryDict[key] = float( value )
              else:
                newHistoryDict[key] = value
            newListOfHistoryDicts.append( newHistoryDict )

        retDict["FTSHistory"] = newListOfHistoryDicts

    return S_OK( retDict )

  def _getFTSJobProperties( self, ftsJobID, columnNames = None ):
    """ select :columnNames: from FTSJob table  """
    columnNames = columnNames if columnNames else FTSJob.tableDesc()["Fields"].keys()
    columnNames = ",".join( [ '`%s`' % str( columnName ) for columnName in columnNames ] )
    return "SELECT %s FROM `FTSJob` WHERE `FTSJobID` = %s;" % ( columnNames, int( ftsJobID ) )

  def _getFTSFileProperties( self, ftsFileID, columnNames = None ):
    """ select :columnNames: from FTSJobFile table  """
    columnNames = columnNames if columnNames else FTSFile.tableDesc()["Fields"].keys()
    columnNames = ",".join( [ '`%s`' % str( columnName ) for columnName in columnNames ] )
    return "SELECT %s FROM `FTSFile` WHERE `FTSFileID` = %s;" % ( columnNames, int( ftsFileID ) )

  def _getFTSHistoryProperties( self, columnNames = None ):
    """ select :columnNames: from FTSHistory view """
    columnNames = columnNames if columnNames else FTSHistoryView.viewDesc()["Fields"].keys()
    return "SELECT %s FROM `FTSHistoryView`;" % ",".join( columnNames )
