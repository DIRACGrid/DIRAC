# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/AccountingSystem/private/Attic/AccountingDB.py,v 1.3 2008/01/23 19:00:17 acasajus Exp $
__RCSID__ = "$Id: AccountingDB.py,v 1.3 2008/01/23 19:00:17 acasajus Exp $"

import time
import threading
from DIRAC.Core.Base.DB import DB
from DIRAC import S_OK, S_ERROR, gLogger, gMonitor
from DIRAC.Core.Utilities import List, ThreadSafe, Time

gSynchro = ThreadSafe.Synchronizer()

class AccountingDB(DB):

  def __init__( self, maxQueueSize = 10 ):
    DB.__init__( self, 'AccountingDB','Accounting/AccountingDB', maxQueueSize )
    self.bucketTime = 900 #15 mins
    self.dbCatalog = {}
    self.dbLocks = {}
    self._createTables( { 'catalogTypes' : { 'Fields' : { 'name' : "VARCHAR(64) UNIQUE",
                                                          'keyFields' : "VARCHAR(256)",
                                                          'valueFields' : "VARCHAR(256)"
                                                       },
                                             'PrimaryKey' : 'name'
                                           }
                        }
                      )
    self.__loadCatalogFromDB()
    gMonitor.registerActivity( "registeradded",
                               "Register added",
                               "Accounting",
                               "entries",
                               gMonitor.OP_ACUM )

  def __loadCatalogFromDB(self):
    retVal = self._query( "SELECT name, keyFields, valueFields FROM catalogTypes" )
    if not retVal[ 'OK' ]:
      raise Exception( retVal[ 'Message' ] )
    for typesEntry in retVal[ 'Value' ]:
      typeName = typesEntry[0]
      keyFields = List.fromChar( typesEntry[1], "," )
      valueFields = List.fromChar( typesEntry[2], "," )
      self.dbCatalog[ typeName ] = { 'keys' : keyFields , 'values' : valueFields, 'allfields' : [] }
      self.dbCatalog[ typeName ][ 'allfields' ].extend( keyFields )
      self.dbCatalog[ typeName ][ 'allfields' ].extend( valueFields )
      self.dbCatalog[ typeName ][ 'allfields' ].extend( [ 'startTime', 'endTime' ] )
      self.dbLocks[ "bucket%s" % typeName ] = threading.Lock()
      for key in keyFields:
        if not key in self.dbLocks:
          self.dbLocks[ "key%s" % key ] = threading.Lock()

  @gSynchro
  def registerType( self, name, keyFields, valueFields ):
    """
    Register a new type
    """
    for field in keyFields:
      if field in valueFields:
        return S_ERROR( "Key field %s is also in the list of value fields" % field )
    for field in valueFields:
      if field in keyFields:
        return S_ERROR( "Value field %s is also in the list of key fields" % field )
    if name in self.dbCatalog:
      gLogger.error( "Type %s is already registered" % name )
      return S_ERROR( "Type %s already exists in db" % name )
    tables = {}
    for key in keyFields:
      tables[ "key%s" % key ] = { 'Fields' : { 'id' : 'INTEGER NOT NULL AUTO_INCREMENT',
                                              'value' : 'VARCHAR(256) UNIQUE'
                                             },
                                  'Indexes' : { 'valueindex' : [ 'value' ] },
                                  'PrimaryKey' : 'id'
                                }
    #Registering type
    fieldsDict = {}
    indexesDict = {}
    bucketFieldsDict = {}
    for key in keyFields:
      indexesDict[ "%sIndex" % key ] = [ key ]
      fieldsDict[ key ] = "INT"
      bucketFieldsDict[ key ] = "INT"
    for field in valueFields:
      fieldsDict[ field ] = "BIGINT"
      bucketFieldsDict[ field ] = "DOUBLE"

    bucketFieldsDict[ 'bucketTime' ] = "DATETIME"
    tables[ "bucket%s" % name ] = { 'Fields' : bucketFieldsDict,
                                    'Indexes' : indexesDict,
                                  }

    fieldsDict[ 'startTime' ] = "DATETIME"
    fieldsDict[ 'endTime' ] = "DATETIME"
    tables[ "type%s" % name ] = { 'Fields' : fieldsDict,
                                  'Indexes' : indexesDict,
                                }
    retVal = self._createTables( tables )

    if not retVal[ 'OK' ]:
      gLogger.error( "Can't create type %s: %s" % ( name, retVal[ 'Message' ] ) )
      return S_ERROR( "Can't create type %s: %s" % ( name, retVal[ 'Message' ] ) )
    self.dbCatalog[ name ] = { 'keys' : keyFields , 'values' : valueFields, 'allfields' : [] }
    self.dbCatalog[ name ][ 'allfields' ].extend( keyFields )
    self.dbCatalog[ name ][ 'allfields' ].extend( valueFields )
    self.dbCatalog[ name ][ 'allfields' ].extend( [ 'startTime', 'endTime' ] )
    self._insert( 'catalogTypes',
                  [ 'name', 'keyFields', 'valueFields' ],
                  [ name, ",".join( keyFields ), ",".join( valueFields ) ] )
    gLogger.info( "Registered type %s" % name )
    self.dbLocks[ "bucket%s" % name ] = threading.Lock()
    for key in keyFields:
      if not key in self.dbLocks:
        self.dbLocks[ "key%s" % key ] = threading.Lock()
    return S_OK()

  def getRegisteredTypes( self ):
    """
    Get list of registered types
    """
    retVal = self._query( "SELECT name, keyFields, valueFields FROM catalogTypes" )
    if not retVal[ 'OK' ]:
      return retVal
    typesList = []
    for typeInfo in retVal[ 'Value' ]:
      typesList.append( [ typeInfo[0], List.fromChar( typeInfo[1] ), List.fromChar( typeInfo[2] ) ] )
    return S_OK( typesList )

  @gSynchro
  def deleteType( self, typeName ):
    """
    Deletes a type
    """
    if typeName not in self.dbCatalog:
      return S_ERROR( "Type %s does not exist" % typeName )
    gLogger.info( "Deleting type", typeName )
    tablesToDelete = []
    for keyField in self.dbCatalog[ typeName ][ 'keys' ]:
      found = False
      for otherType in self.dbCatalog:
        if otherType == typeName:
          continue
        if keyField not in self.dbCatalog[ otherType ][ 'keys' ]:
          found = True
      if not found:
        tablesToDelete.append( "key%s" % keyField )
        gLogger.info( "Deleting key table", keyField )
    tablesToDelete.insert( 0, "type%s" % typeName )
    tablesToDelete.insert( 0, "bucket%s" % typeName )
    retVal = self._query( "DROP TABLE %s" % ", ".join( tablesToDelete ) )
    if not retVal[ 'OK' ]:
      return retVal
    retVal = self._update( "DELETE FROM catalogTypes WHERE name='%s'" % typeName )
    del( self.dbCatalog[ typeName ] )
    return S_OK()

  def __getIdForKeyValue( self, keyName, keyValue ):
    """
      Finds id number for value in a key table
    """
    retVal = self._escapeString( keyValue )
    if not retVal[ 'OK' ]:
      return retVall
    keyValue = retVal[ 'Value' ]
    retVal = self._query( "SELECT `id` FROM key%s WHERE `value`=%s" % ( keyName, keyValue ) )
    if not retVal[ 'OK' ]:
      return retVal
    if len( retVal[ 'Value' ] ) > 0:
      return S_OK( retVal[ 'Value' ][0][0] )
    return S_ERROR( "Key id %s for value %s does not exist althought it shoud" % ( keyName, keyValue ) )

  def __addKeyValue( self, keyName, keyValue ):
    """
      Adds a key value to a key table if not existant
    """
    keyTable = "key%s" % keyName
    self.dbLocks[ keyTable ].acquire()
    try:
      retVal = self.__getIdForKeyValue( keyName, keyValue )
      if retVal[ 'OK' ]:
        return retVal
      else:
        gLogger.info( "Value %s for key %s didn't exist, inserting" % ( keyValue, keyName ) )
        retVal = self._insert( keyTable, [ 'id', 'value' ], [ 0, keyValue ] )
        if not retVal[ 'OK' ]:
          return retVal
        return self.__getIdForKeyValue( keyName, keyValue )
    finally:
      self.dbLocks[ keyTable ].release()
    return S_OK( keyId )

  def __calculateBuckets( self, startTime, endTime ):
    """
    Magic function for calculating buckets between two times and
    the proportional part for each bucket
    """
    startEpoch = int( Time.toEpoch( startTime ) )
    endEpoch = int( Time.toEpoch( endTime ) )
    currentBucketStart = ( startEpoch / self.bucketTime ) * self.bucketTime
    if startEpoch == endEpoch:
      return [ ( Time.fromEpoch( currentBucketStart ),
                 1 ) ]
    buckets = []
    totalLength = endEpoch - startEpoch
    while currentBucketStart < endEpoch:
      start = max( currentBucketStart, startEpoch )
      end = min( currentBucketStart + self.bucketTime, endEpoch )
      proportion = float( end - start ) / totalLength
      buckets.append( ( Time.fromEpoch( currentBucketStart ),
                        proportion ) )
      currentBucketStart += self.bucketTime
    return buckets

  def addEntry( self, typeName, startTime, endTime, valuesList ):
    """
    Add an entry to the type contents
    """
    gMonitor.addMark( "registeradded", 1 )
    if not typeName in self.dbCatalog:
      return S_ERROR( "Type %s has not been defined in the db" % typeName )
    #Discover key indexes
    for keyPos in range( len( self.dbCatalog[ typeName ][ 'keys' ] ) ):
      keyName = self.dbCatalog[ typeName ][ 'keys' ][ keyPos ]
      keyValue = valuesList[ keyPos ]
      retVal = self.__addKeyValue( keyName, keyValue )
      if not retVal[ 'OK' ]:
        return retVal
      gLogger.info( "Value %s for key %s has id %s" % ( keyValue, keyName, retVal[ 'Value' ] ) )
      valuesList[ keyPos ] = retVal[ 'Value' ]
    insertList = list( valuesList )
    insertList.append( startTime.strftime( "%Y-%m-%d %H:%M:%S" ) )
    insertList.append( endTime.strftime( "%Y-%m-%d %H:%M:%S" ) )
    retVal = self._insert( "type%s" % typeName, self.dbCatalog[ typeName ][ 'allfields' ], insertList )
    if not retVal[ 'OK' ]:
      return retVal
    return self.__bucketize( typeName, startTime, endTime, valuesList )

  def __bucketize( self, typeName, startTime, endTime, valuesList ):
    """
    Bucketize a record
    """
    buckets = self.__calculateBuckets( startTime, endTime )
    numKeys = len( self.dbCatalog[ typeName ][ 'keys' ] )
    keyValues = valuesList[ :numKeys ]
    valuesList = valuesList[ numKeys: ]
    for bucketInfo in buckets:
      self.dbLocks[ "bucket%s" % typeName ].acquire()
      try:
        retVal = self.__getBucketFromDB( typeName,
                                         bucketInfo[0],
                                         keyValues )
        if not retVal[ 'OK' ]:
          return retVal
        proportionalValues = []
        for value in valuesList:
          proportionalValues.append( value * bucketInfo[1] )
        if len( retVal[ 'Value' ] ) == 0:
          retVal = self.__insertBucket( typeName, bucketInfo[0], keyValues, proportionalValues )
          if not retVal[ 'OK' ]:
            return retVal
        else:
          bucketValues = retVal[ 'Value' ][0]
          for pos in range( len( bucketValues ) ):
            proportionalValues[ pos ] += bucketValues[ pos ]
          retVal = self.__updateBucket( typeName, bucketInfo[0], keyValues, proportionalValues )
          if not retVal[ 'OK' ]:
            return retVal
      finally:
        self.dbLocks[ "bucket%s" % typeName ].release()
    return S_OK()

  def __generateSQLConditionForKeys( self, typeName, keyValues ):
    """
    Generate sql condition for buckets when coming from the raw insert
    """
    realCondList = []
    for keyPos in range( len( self.dbCatalog[ typeName ][ 'keys' ] ) ):
      keyField = self.dbCatalog[ typeName ][ 'keys' ][ keyPos ]
      keyValue = keyValues[ keyPos ]
      retVal = self._escapeString( keyValue )
      if not retVal[ 'OK' ]:
        return retVal
      keyValue = retVal[ 'Value' ]
      realCondList.append( "`bucket%s`.`%s` = %s" % ( typeName, keyField, keyValue ) )
    return " AND ".join( realCondList )

  def __getBucketFromDB( self, typeName, bucketTime, keyValues ):
    """
    Get a bucket from the DB
    """
    sqlFields = []
    for valueField in self.dbCatalog[ typeName ][ 'values' ]:
      sqlFields.append( "`bucket%s`.`%s`" % ( typeName, valueField ) )
    cmd = "SELECT %s FROM `bucket%s`" % ( ", ".join( sqlFields ), typeName )
    cmd += " WHERE `bucket%s`.`bucketTime`='%s' AND " % ( typeName, bucketTime )
    cmd += self.__generateSQLConditionForKeys( typeName, keyValues )
    return self._query( cmd )

  def __updateBucket( self, typeName, bucketTime, keyValues, bucketValues ):
    """
    Update a bucket when coming from the raw insert
    """
    cmd = "UPDATE `bucket%s` SET " % typeName
    sqlValList = []
    for pos in range( len( self.dbCatalog[ typeName ][ 'values' ] ) ):
      valueField = self.dbCatalog[ typeName ][ 'values' ][ pos ]
      value = bucketValues[ pos ]
      sqlValList.append( "`bucket%s`.`%s`=%s" % ( typeName, valueField, value ) )
    cmd += ", ".join( sqlValList )
    cmd += " WHERE `bucket%s`.`bucketTime`='%s' AND " % ( typeName, bucketTime )
    cmd += self.__generateSQLConditionForKeys( typeName, keyValues )
    return self._update( cmd )

  def __insertBucket( self, typeName, bucketTime, keyValues, bucketValues ):
    """
    Insert a bucket when coming from the raw insert
    """
    sqlFields = [ 'bucketTime' ]
    sqlValues = [ '%s' % bucketTime ]
    for keyPos in range( len( self.dbCatalog[ typeName ][ 'keys' ] ) ):
      sqlFields.append( self.dbCatalog[ typeName ][ 'keys' ][ keyPos ] )
      sqlValues.append( keyValues[ keyPos ] )
    for valPos in range( len( self.dbCatalog[ typeName ][ 'values' ] ) ):
      sqlFields.append( self.dbCatalog[ typeName ][ 'values' ][ valPos ] )
      sqlValues.append( bucketValues[ valPos ] )
    return self._insert( "bucket%s" % typeName, sqlFields, sqlValues )




