try:
  import sqlite3
except:
  pass

import os
import types
import md5
import DIRAC
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.MonitoringSystem.private.Activity import Activity

class MonitoringCatalog:

  def __init__( self, dataPath ):
    """
    Initialize monitoring catalog
    """
    self.dbConn = False
    self.dataPath = dataPath
    self.createSchema()

  def __connect( self ):
    """
    Connect to database
    """
    if not self.dbConn:
      dbPath = "%s/monitoring.db" % self.dataPath
      self.dbConn = sqlite3.connect( dbPath, isolation_level = None )

  def __dbExecute( self, query, values = False ):
    """
    Execute a sql statement
    """
    cursor = self.dbConn.cursor()
    gLogger.debug( "Executing %s" % query )
    if values:
      cursor.execute( query, values )
    else:
      cursor.execute( query )
    return cursor

  def __createTables( self ):
    """
    Create tables if not already created
    """
    gLogger.info( "Creating tables in db" )
    try:
      filePath = "%s/monitoringSchema.sql" % os.path.dirname( __file__ )
      fd = open( filePath )
      buffer = fd.read()
      fd.close()
    except IOError, e:
      DIRAC.abort( 1, "Can't read monitoring schema", filePath )
    while buffer.find( ";" ) > -1:
      limit = buffer.find( ";" ) + 1
      sqlQuery = buffer[ : limit ].replace( "\n", "" )
      buffer = buffer[ limit : ]
      try:
        self.__dbExecute( sqlQuery )
      except Exception, e:
        DIRAC.abort( 1, "Can't create tables", str( e ) )

  def createSchema( self ):
    """
    Create all the sql schema if it does not exist
    """
    self.__connect()
    try:
      c = self.dbConn.execute( "SELECT name FROM sqlite_master WHERE type='table';" )
      tablesList = c.fetchall()
      if len( tablesList ) < 2:
        self.__createTables()
    except Except, e:
      gLogger.fatal( "Failed to startup db engine", str( e ) )
      return False
    return True

  def __delete( self, table, dataDict ):
    """
    Execute an sql delete
    """
    query = "DELETE FROM %s" % table
    valuesList = []
    keysList = []
    for key in dataDict:
      if type( dataDict[ key ] ) == types.ListType:
        orList = []
        for keyValue in dataDict[ key ]:
          valuesList.append( keyValue )
          orList.append( "%s = ?" % key )
        keysList.append( "( %s )" % " OR ".join( orList ) )
      else:
        valuesList.append( dataDict[ key ] )
        keysList.append( "%s = ?" % key )
    if keysList:
      query += " WHERE %s" % ( " AND ".join( keysList ) )
    self.__dbExecute( "%s;" % query, values = valuesList )


  def __select( self, fields, table, dataDict, extraCond = "" ):
    """
    Execute a sql select
    """
    valuesList = []
    keysList = []
    for key in dataDict:
      if type( dataDict[ key ] ) == types.ListType:
        orList = []
        for keyValue in dataDict[ key ]:
          valuesList.append( keyValue )
          orList.append( "%s = ?" % key )
        keysList.append( "( %s )" % " OR ".join( orList ) )
      else:
        valuesList.append( dataDict[ key ] )
        keysList.append( "%s = ?" % key )
    if type( fields ) in ( types.StringType, types.UnicodeType ):
      fields = [ fields ]
    if len( keysList ) > 0:
      whereCond = "WHERE %s" % ( " AND ".join( keysList ) )
    else:
      whereCond = ""
    if extraCond:
      if whereCond:
        whereCond += " AND %s" % extraCond
      else:
        whereCond = "WHERE %s" % extraCond
    query = "SELECT %s FROM %s %s;" % (
                                           ",".join( fields ),
                                           table,
                                           whereCond
                                           )
    c = self.__dbExecute( query, values = valuesList )
    return c.fetchall()

  def __insert( self, table, specialDict, dataDict ):
    """
    Execute an sql insert
    """
    valuesList = []
    valuePoitersList = []
    namesList = []
    for key in specialDict:
      namesList.append( key )
      valuePoitersList.append( specialDict[ key ] )
    for key in dataDict:
      namesList.append( key )
      valuePoitersList.append( "?" )
      valuesList.append( dataDict[ key ] )
    query = "INSERT INTO %s (%s) VALUES (%s);" % ( table,
                                       ", ".join( namesList ),
                                       ",".join( valuePoitersList )
                                       )
    c = self.__dbExecute( query, values = valuesList )
    return c.rowcount

  def registerSource( self, sourceDict ):
    """
    Register an activity source
    """
    retList = self.__select( "id", "sources", sourceDict )
    if len( retList ) > 0:
      return retList[0][0]
    else:
      gLogger.info( "Registering source", str( sourceDict ) )
      if self.__insert( "sources", { 'id' : 'NULL' }, sourceDict ) == 0:
        return -1
      return self.__select( "id", "sources", sourceDict )[0][0]

  def registerActivity( self, sourceId, acName, acDict ):
    """
    Register an activity
    """
    m = md5.new()
    acDict[ 'name' ] = acName
    acDict[ 'sourceId' ] = sourceId
    m.update( str( acDict ) )
    retList = self.__select( "filename", "activities", acDict )
    if len( retList ) > 0:
      return retList[0][0]
    else:
      filePath = m.hexdigest()
      filePath = "%s/%s.rrd" % ( filePath[:2], filePath )
      gLogger.info( "Registering activity", str( acDict ) )
      if self.__insert( "activities", {
                               'id' : 'NULL',
                               'filename' : "'%s'" % filePath
                               },
                               acDict ) == 0:
        return -1
      return self.__select( "filename", "activities", acDict )[0][0]

  def getFilename( self, sourceId, acName ):
    """
    Get rrd filename for an activity
    """
    queryDict = { 'sourceId' : sourceId, "name" : acName }
    retList = self.__select( "filename", "activities", queryDict )
    if len( retList ) == 0:
      return ""
    else:
      return retList[0][0]

  def findActivity( self, sourceId, acName ):
    """
    Find activity
    """
    queryDict = { 'sourceId' : sourceId, "name" : acName }
    retList = self.__select( "id, name, category, unit, type, description, filename", "activities", queryDict )
    if len( retList ) == 0:
      return False
    else:
      return retList[0]

  def queryField( self, field, definedFields ):
    """
    Query the values of a field given a set of defined ones
    """
    retList = self.__select( field, "sources, activities", definedFields, "sources.id = activities.sourceId" )
    return retList

  def getMatchingActivities( self, condDict ):
    """
    Get all activities matching the defined conditions
    """
    retList = self.queryField( Activity.dbFields, condDict )
    acList = []
    for acData in retList:
      acList.append( Activity( acData ) )
    return acList

  def registerView( self, viewName, viewData, varFields ):
    """
    Register a new view
    """
    retList = self.__select( "id", "views", { 'name' : viewName } )
    if len( retList ) > 0:
      return S_ERROR( "Name for view name already exists" )
    retList = self.__select( "name", "views", { 'definition' : viewData } )
    if len( retList ) > 0:
      return S_ERROR( "View specification already defined with name '%s'" % retList[0][0] )
    self.__insert( "views", { 'id' : 'NULL' }, { 'name' : viewName,
                                                 'definition' : viewData,
                                                 'variableFields' : ", ".join( varFields )
                                               } )
    return S_OK()

  def getViews( self, onlyStatic ):
    """
    Get views
    """
    queryCond = {}
    if onlyStatic:
      queryCond[ 'variableFields' ] = ""
    return self.__select( "id, name, variableFields", "views", queryCond )

  def getViewById( self, viewId ):
    """
    Get a view for a given id
    """
    print "VID", type( viewId )
    if type( viewId ) in ( types.StringType, types.UnicodeType ):
      return self.__select( "definition, variableFields", "views", { "name" : viewId } )
    else:
      return self.__select( "definition, variableFields", "views", { "id" : viewId } )

  def deleteView( self, viewId ):
    """
    Delete a view
    """
    self.__delete( "views", { 'id' : viewId } )


  def getSources( self, dbCond ):
    return self.__select( "id, site, componentType, componentLocation, componentName",
                           "sources",
                           dbCond)

  def getActivities( self, dbCond ):
    return self.__select( "id, name, category, unit, type, description",
                          "activities",
                        dbCond)

  def deleteActivity( self, sourceId, activityId ):
    """
    Delete a view
    """
    acCond = { 'sourceId' : sourceId, 'id' : activityId }
    acList = self.__select( "filename", "activities", acCond )
    if len( acList ) == 0:
      return S_ERROR( "Activity does not exist" )
    rrdFile = acList[0][0]
    self.__delete( "activities", acCond )
    acList = self.__select( "id", "activities", { 'sourceId' : sourceId } )
    if len( acList ) == 0:
      self.__delete( "sources", { 'id' : sourceId } )
    return S_OK( rrdFile )
