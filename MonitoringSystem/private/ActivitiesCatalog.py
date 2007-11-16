try:
  import sqlite3
  import xml.etree.ElementTree
except:
  pass

import os
import types
import md5
import DIRAC
from DIRAC import gLogger

class ActivitiesCatalog:

  def __init__( self, dataPath ):
    self.dbConn = False
    self.dataPath = dataPath

  def __connect( self ):
    if not self.dbConn:
      dbPath = "%s/monitoring.db" % self.dataPath
      self.dbConn = sqlite3.connect( dbPath, isolation_level = None )

  def __dbExecute( self, query, values = False ):
    cursor = self.dbConn.cursor()
    gLogger.debug( "Executing %s" % query )
    if values:
      cursor.execute( query, values )
    else:
      cursor.execute( query )
    return cursor

  def __createTables( self ):
    gLogger.info( "Creating tables in db" )
    self.__connect()
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

  def __select( self, fields, table, dataDict ):
    valuesList = []
    keysList = []
    for key in dataDict:
      valuesList.append( dataDict[ key ] )
      keysList.append( "%s = ?" % key )
    if type( fields ) == types.StringType:
      fields = [ fields ]
    query = "SELECT %s FROM %s WHERE %s;" % (
                                           ",".join( fields ),
                                           table,
                                           " AND ".join( keysList )
                                           )
    c = self.__dbExecute( query, values = valuesList )
    return c.fetchall()

  def __insert( self, table, specialDict, dataDict ):
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
    self.__connect()
    retList = self.__select( "id", "sources", sourceDict )
    if len( retList ) > 0:
      return retList[0][0]
    else:
      gLogger.info( "Registering source", str( sourceDict ) )
      if self.__insert( "sources", { 'id' : 'NULL' }, sourceDict ) == 0:
        return -1
      return self.__select( "id", "sources", sourceDict )[0][0]

  def registerActivity( self, sourceId, acName, acDict ):
    self.__connect()
    m = md5.new()
    acDict[ 'name' ] = acName
    acDict[ 'sourceId' ] = sourceId
    m.update( str( acDict ) )
    retList = self.__select( "filename", "activities", acDict )
    if len( retList ) > 0:
      return retList[0][0]
    else:
      gLogger.info( "Registering activity", str( acDict ) )
      if self.__insert( "activities", {
                               'id' : 'NULL',
                               'filename' : "'%s.rrd'" % m.hexdigest()
                               },
                               acDict ) == 0:
        return -1
      return self.__select( "filename", "activities", acDict )[0][0]

  def getFilename( self, sourceId, acName ):
    self.__connect()
    queryDict = { 'sourceId' : sourceId, "name" : acName }
    retList = self.__select( "filename", "activities", queryDict )
    if len( retList ) == 0:
      return ""
    else:
      return retList[0][0]


