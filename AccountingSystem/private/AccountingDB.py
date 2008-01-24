# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/AccountingSystem/private/Attic/AccountingDB.py,v 1.5 2008/01/24 19:19:52 acasajus Exp $
__RCSID__ = "$Id: AccountingDB.py,v 1.5 2008/01/24 19:19:52 acasajus Exp $"

import time
import threading
import types
from DIRAC.Core.Base.DB import DB
from DIRAC import S_OK, S_ERROR, gLogger, gMonitor
from DIRAC.Core.Utilities import List, ThreadSafe, Time

gSynchro = ThreadSafe.Synchronizer()

class AccountingDB(DB):

  def __init__( self, maxQueueSize = 10 ):
    DB.__init__( self, 'AccountingDB','Accounting/AccountingDB', maxQueueSize )
    self.bucketTime = 900 #15 mins
    self.dbCatalog = {}
    self.dbKeys = {}
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
      self.__addToCatalog( typeName, keyFields, valueFields )

  def __addToCatalog( self, typeName, keyFields, valueFields ):
    """
    Add type to catalog
    """
    for key in keyFields:
      if key in self.dbKeys:
        self.dbKeys[ key ] += 1
      else:
        self.dbKeys[ key ] = 1
    self.dbCatalog[ typeName ] = { 'keys' : keyFields , 'values' : valueFields, 'typefields' : [] }
    self.dbCatalog[ typeName ][ 'typefields' ].extend( keyFields )
    self.dbCatalog[ typeName ][ 'typefields' ].extend( valueFields )
    self.dbCatalog[ typeName ][ 'bucketfields' ] = list( self.dbCatalog[ typeName ][ 'typefields' ] )
    self.dbCatalog[ typeName ][ 'typefields' ].extend( [ 'startTime', 'endTime' ] )
    self.dbCatalog[ typeName ][ 'bucketfields' ].append( 'bucketTime' )
    self.dbLocks[ "bucket%s" % typeName ] = threading.Lock()
    for key in keyFields:
      if not key in self.dbLocks:
        self.dbLocks[ "key%s" % key ] = threading.Lock()

  @gSynchro
  def registerType( self, name, definitionKeyFields, definitionAccountingFields ):
    """
    Register a new type
    """
    keyFieldsList = []
    valueFieldsList = []
    for t in definitionKeyFields:
      keyFieldsList.append( t[0] )
    for t in definitionAccountingFields:
      valueFieldsList.append( t[0] )
    for field in definitionKeyFields:
      if field in valueFieldsList:
        return S_ERROR( "Key field %s is also in the list of value fields" % field )
    for field in definitionAccountingFields:
      if field in keyFieldsList:
        return S_ERROR( "Value field %s is also in the list of key fields" % field )
    if name in self.dbCatalog:
      gLogger.error( "Type %s is already registered" % name )
      return S_ERROR( "Type %s already exists in db" % name )
    tables = {}
    for key in definitionKeyFields:
      if key[0] not in self.dbKeys:
        gLogger.info( "Table for key %s has to be created" % key[0] )
        tables[ "key%s" % key[0] ] = { 'Fields' : { 'id' : 'INTEGER NOT NULL AUTO_INCREMENT',
                                                    'value' : '%s UNIQUE' % key[1]
                                                  },
                                       'Indexes' : { 'valueindex' : [ 'value' ] },
                                       'PrimaryKey' : 'id'
                                     }
    #Registering type
    fieldsDict = {}
    indexesDict = {}
    bucketFieldsDict = {}
    for field in definitionKeyFields:
      indexesDict[ "%sIndex" % field[0] ] = [ field[0] ]
      fieldsDict[ field[0] ] = "INTEGER"
      bucketFieldsDict[ field[0] ] = "INTEGER"
    for field in definitionAccountingFields:
      fieldsDict[ field[0]  ] = field[1]
      bucketFieldsDict[ field[0]  ] = field[1]

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
    self._insert( 'catalogTypes',
                  [ 'name', 'keyFields', 'valueFields' ],
                  [ name, ",".join( keyFieldsList ), ",".join( valueFieldsList ) ] )
    self.__addToCatalog( name, keyFieldsList, valueFieldsList )
    gLogger.info( "Registered type %s" % name )
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
      self.dbKeys[ keyField ] -= 1
      if self.dbKeys[ keyField ] == 0:
        tablesToDelete.append( "key%s" % keyField )
        gLogger.info( "Deleting key table", keyField )
        del( self.dbKeys[ keyField ] )
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
    retVal = self._insert( "type%s" % typeName, self.dbCatalog[ typeName ][ 'typefields' ], insertList )
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

  def __checkFieldsExistsInType( self, typeName, fields, tableType ):
    """
    Check wether a list of fields exist for a given typeName
    """
    missing = []
    for key in fields:
      if key not in self.dbCatalog[ typeName ][ '%sfields' % tableType ]:
        missing.append( key )
    return missing

  def __checkIncomingFieldsForQuery( self, typeName, condDict, valueFields, groupFields, tableType ):
    missing = self.__checkFieldsExistsInType( typeName, condDict, tableType )
    if missing:
      return S_ERROR( "Condition keys %s are not defined" % ", ".join( missing ) )
    missing = self.__checkFieldsExistsInType( typeName, [ vT[0] for vT in valueFields ], tableType )
    if missing:
      return S_ERROR( "Value keys %s are not defined" % ", ".join( missing ) )
    missing = self.__checkFieldsExistsInType( typeName, groupFields, tableType )
    if missing:
      return S_ERROR( "Group fields %s are not defined" % ", ".join( missing ) )
    return S_OK()


  def retrieveBucketedData( self, typeName, startTime, endTime, condDict, valueFields, groupFields ):
    """
    Get data from the DB
      Parameters:
        typeName -> typeName
        startTime & endTime -> datetime objects. Do I need to explain the meaning?
        condDict -> conditions for the query
                    key -> name of the field
                    value -> list of possible values
        valueFields -> list of fields to retrieve. Has to contain tuples with:
                        ( <name of value field>, <function to apply> )
        groupFields -> list of fields to group by
    """
    if typeName not in self.dbCatalog:
      return S_ERROR( "Type %s is not defined" % typeName )
    retVal = self.__checkIncomingFieldsForQuery( typeName, condDict, valueFields, groupFields, "bucket" )
    if not retVal[ 'OK' ]:
      return retVal
    return self.__generateSQLSelect( typeName,
                                     startTime,
                                     endTime,
                                     condDict,
                                     valueFields,
                                     groupFields,
                                     "bucket" )

  def __generateSQLSelect( self, typeName, startTime, endTime, condDict, valueFields, groupFields, tableType ):
    cmd = "SELECT"
    sqlValues = []
    sqlLinkList = []
    for vTu in valueFields:
      if vTu[0] in self.dbCatalog[ typeName ][ 'keys' ]:
        sqlValues.append( "`key%s`.`value`" % ( vTu[0] ) )
        List.appendUnique( sqlLinkList, "`%s%s`.`%s` = `key%s`.`id`" % ( tableType,
                                                                         typeName,
                                                                         vTu[0],
                                                                         vTu[0] ) )
      else:
        sqlValues.append( "`%s%s`.`%s`" % ( tableType, typeName, vTu[0] ) )
      if vTu[1]:
        if not groupFields:
          return S_OK( "Can't do a %s function without specifying grouping fields" )
        sqlValues[-1] = "%s(%s)" % ( vTu[1], sqlValues[-1] )
    cmd += " %s" % ", ".join( sqlValues )
    cmd += " FROM `%s%s`" % ( tableType, typeName )
    for key in condDict:
      cmd += ", `key%s`" % key
    if tableType == "bucket":
      cmd += " WHERE `bucket%s`.`bucketTime` >= '%s' AND `bucket%s`.`bucketTime` <= '%s'" % (
                                                    typeName,
                                                    startTime.strftime( "%Y-%m-%d %H:%M:%S" ),
                                                    typeName,
                                                    endTime.strftime( "%Y-%m-%d %H:%M:%S" )
                                                    )
    else:
      cmd += " WHERE `type%s`.`startTime` >= '%s' AND `type%s`.`startTime` <= '%s'" % (
                                                    typeName,
                                                    startTime.strftime( "%Y-%m-%d %H:%M:%S" ),
                                                    typeName,
                                                    endTime.strftime( "%Y-%m-%d %H:%M:%S" )
                                                    )
    sqlCondList = []
    for keyName in condDict:
      sqlORList = []
      if keyName in self.dbCatalog[ typeName ][ 'keys' ]:
        List.appendUnique( sqlLinkList, "`%s%s`.`%s` = `key%s`.`id`" % ( tableType,
                                                                         typeName,
                                                                         keyName,
                                                                         keyName ) )
      if type( condDict[ keyName ] ) not in ( types.ListType, types.TupleType ):
        condDict[ keyName ] = [ condDict[ keyName ] ]
      for keyValue in condDict[ keyName ]:
        retVal = self._escapeString( keyValue )
        if not retVal[ 'OK' ]:
          return retVal
        keyValue = retVal[ 'Value' ]
        if keyName in self.dbCatalog[ typeName ][ 'keys' ]:
          sqlORList.append( "`key%s`.`value` = %s" % ( keyName, keyValue ) )
        else:
          sqlORList.appen( "`%s%s`.`%s` = %s" % ( tableType, typeName, keyName, keyValue ) )
      sqlCondList.append( "( %s )" % " OR ".join( sqlORList ) )
    if sqlCondList:
      cmd += " AND %s" % " AND ".join( sqlCondList )
    sqlGroupList = []
    if groupFields:
      for field in groupFields:
        if field in self.dbCatalog[ typeName ][ 'keys' ]:
          List.appendUnique( sqlLinkList, "`%s%s`.`%s` = `key%s`.`id`" % ( tableType,
                                                                         typeName,
                                                                         field,
                                                                         field ) )
          sqlGroupList.append( "`key%s`.`value`" % field )
        else:
          sqlGroupList.append( "`%s%s`.`%s`" % ( tableType, typeName, field ) )
    if sqlLinkList:
      cmd += " AND %s" % " AND ".join( sqlLinkList )
    if sqlGroupList:
      cmd += " GROUP BY %s" % ", ".join( sqlGroupList )
    return self._query( cmd )