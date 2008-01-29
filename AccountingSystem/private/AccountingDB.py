# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/AccountingSystem/private/Attic/AccountingDB.py,v 1.7 2008/01/29 19:11:06 acasajus Exp $
__RCSID__ = "$Id: AccountingDB.py,v 1.7 2008/01/29 19:11:06 acasajus Exp $"

import datetime
import threading
import types
from DIRAC.Core.Base.DB import DB
from DIRAC import S_OK, S_ERROR, gLogger, gMonitor
from DIRAC.Core.Utilities import List, ThreadSafe, Time, DEncode

gSynchro = ThreadSafe.Synchronizer()

class AccountingDB(DB):

  def __init__( self, maxQueueSize = 10 ):
    DB.__init__( self, 'AccountingDB','Accounting/AccountingDB', maxQueueSize )
    self.maxBucketTime = 604800 #1 w
    self.dbCatalog = {}
    self.dbKeys = {}
    self.dbLocks = {}
    self.dbBucketsLength = {}
    self._createTables( { 'catalogTypes' : { 'Fields' : { 'name' : "VARCHAR(64) UNIQUE",
                                                          'keyFields' : "VARCHAR(256)",
                                                          'valueFields' : "VARCHAR(256)",
                                                          'bucketsLength' : "VARCHAR(256)",
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
    retVal = self._query( "SELECT name, keyFields, valueFields, bucketsLength FROM catalogTypes" )
    if not retVal[ 'OK' ]:
      raise Exception( retVal[ 'Message' ] )
    for typesEntry in retVal[ 'Value' ]:
      typeName = typesEntry[0]
      keyFields = List.fromChar( typesEntry[1], "," )
      valueFields = List.fromChar( typesEntry[2], "," )
      bucketsLength = DEncode.decode( typesEntry[3] )[0]
      self.__addToCatalog( typeName, keyFields, valueFields, bucketsLength )

  def __addToCatalog( self, typeName, keyFields, valueFields, bucketsLength ):
    """
    Add type to catalog
    """
    gLogger.verbose( "Adding to catalog type %s" % typeName, "with length %s" % str( bucketsLength ) )
    for key in keyFields:
      if key in self.dbKeys:
        self.dbKeys[ key ] += 1
      else:
        self.dbKeys[ key ] = 1
    self.dbCatalog[ typeName ] = { 'keys' : keyFields , 'values' : valueFields, 'typeFields' : [], 'bucketFields' : [] }
    self.dbCatalog[ typeName ][ 'typeFields' ].extend( keyFields )
    self.dbCatalog[ typeName ][ 'typeFields' ].extend( valueFields )
    self.dbCatalog[ typeName ][ 'bucketFields' ] = list( self.dbCatalog[ typeName ][ 'typeFields' ] )
    self.dbCatalog[ typeName ][ 'typeFields' ].extend( [ 'startTime', 'endTime' ] )
    self.dbCatalog[ typeName ][ 'bucketFields' ].extend( [ 'startTime', 'bucketLength' ] )
    self.dbLocks[ "bucket%s" % typeName ] = threading.Lock()
    self.dbBucketsLength[ typeName ] = bucketsLength
    for key in keyFields:
      if not key in self.dbLocks:
        self.dbLocks[ "key%s" % key ] = threading.Lock()

  @gSynchro
  def registerType( self, name, definitionKeyFields, definitionAccountingFields, bucketsLength ):
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
    for bucket in bucketsLength:
      if type( bucket ) != types.TupleType:
        return S_ERROR( "Length of buckets should be a list of tuples" )
      if len( bucket ) != 2:
        return S_ERROR( "Length of buckets should have 2d tuples" )
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
    bucketFieldsDict = {}
    indexesDict = {}
    for field in definitionKeyFields:
      indexesDict[ "%sIndex" % field[0] ] = [ field[0] ]
      fieldsDict[ field[0] ] = "INTEGER"
      bucketFieldsDict[ field[0] ] = "INTEGER"
    for field in definitionAccountingFields:
      fieldsDict[ field[0] ] = field[1]
      bucketFieldsDict[ field[0] ] = "FLOAT"
    fieldsDict[ 'startTime' ] = "DATETIME"
    fieldsDict[ 'endTime' ] = "DATETIME"
    bucketFieldsDict[ 'startTime' ] = "DATETIME"
    bucketFieldsDict[ 'bucketLength' ] = "INT"
    tables[ "bucket%s" % name ] = { 'Fields' : bucketFieldsDict,
                                    'Indexes' : indexesDict,
                                  }
    tables[ "type%s" % name ] = { 'Fields' : fieldsDict,
                                  'Indexes' : indexesDict,
                                }
    retVal = self._createTables( tables )

    if not retVal[ 'OK' ]:
      gLogger.error( "Can't create type %s: %s" % ( name, retVal[ 'Message' ] ) )
      return S_ERROR( "Can't create type %s: %s" % ( name, retVal[ 'Message' ] ) )
    bucketsLength.sort()
    print bucketsLength
    bucketsEncoding = DEncode.encode( bucketsLength )
    self._insert( 'catalogTypes',
                  [ 'name', 'keyFields', 'valueFields', 'bucketsLength' ],
                  [ name, ",".join( keyFieldsList ), ",".join( valueFieldsList ), bucketsEncoding ] )
    self.__addToCatalog( name, keyFieldsList, valueFieldsList, bucketsLength )
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
    tablesToDelete.insert( 0, "`type%s`" % typeName )
    tablesToDelete.insert( 0, "`bucket%s`" % typeName )
    retVal = self._query( "DROP TABLE %s" % ", ".join( tablesToDelete ) )
    if not retVal[ 'OK' ]:
      return retVal
    retVal = self._update( "DELETE FROM catalogTypes WHERE name='%s'" % typeName )
    del( self.dbCatalog[ typeName ] )
    return S_OK()

  def __getIdForKeyValue( self, keyName, keyValue, conn = False ):
    """
      Finds id number for value in a key table
    """
    retVal = self._query( "SELECT `id` FROM `key%s` WHERE `value`='%s'" % ( keyName, keyValue ), conn = conn )
    if not retVal[ 'OK' ]:
      return retVal
    if len( retVal[ 'Value' ] ) > 0:
      return S_OK( retVal[ 'Value' ][0][0] )
    return S_ERROR( "Key id %s for value %s does not exist although it shoud" % ( keyName, keyValue ) )

  def __addKeyValue( self, keyName, keyValue ):
    """
      Adds a key value to a key table if not existant
    """
    keyTable = "key%s" % keyName
    self.dbLocks[ keyTable ].acquire()
    try:
      if type( keyValue ) != types.StringType:
        keyValue = str( keyValue )
      retVal = self.__getIdForKeyValue( keyName, keyValue )
      if retVal[ 'OK' ]:
        return retVal
      else:
        retVal = self._getConnection()
        if not retVal[ 'OK' ]:
          return retVal
        connection = retVal[ 'Value' ]
        gLogger.info( "Value %s for key %s didn't exist, inserting" % ( keyValue, keyName ) )
        retVal = self._insert( keyTable, [ 'id', 'value' ], [ 0, keyValue ], connection )
        if not retVal[ 'OK' ]:
          return retVal
        return self.__getIdForKeyValue( keyName, keyValue, connection )
    finally:
      self.dbLocks[ keyTable ].release()
    return S_OK( keyId )

  def __getBucketTimeLength( self, typeName,now, when ):
    """
    Get the expected bucket time for a moment in time
    """
    dif = abs( now - when )
    for granuT in self.dbBucketsLength[ typeName ]:
      if dif < granuT[0]:
        return granuT[1]
    return self.maxBucketTime

  def __calculateBuckets( self, typeName, startTime, endTime ):
    """
    Magic function for calculating buckets between two times and
    the proportional part for each bucket
    """
    nowEpoch = int( Time.toEpoch( Time.dateTime() ) )
    startEpoch = int( Time.toEpoch( startTime ) )
    endEpoch = int( Time.toEpoch( endTime ) )
    bucketTimeLength = self.__getBucketTimeLength( typeName, nowEpoch, startEpoch )
    currentBucketStart = ( startEpoch / bucketTimeLength ) * bucketTimeLength
    if startEpoch == endEpoch:
      return [ ( Time.fromEpoch( currentBucketStart ),
                 1,
                 bucketTimeLength ) ]
    buckets = []
    totalLength = endEpoch - startEpoch
    while currentBucketStart < endEpoch:
      start = max( currentBucketStart, startEpoch )
      end = min( currentBucketStart + bucketTimeLength, endEpoch )
      proportion = float( end - start ) / totalLength
      buckets.append( ( Time.fromEpoch( currentBucketStart ),
                        proportion,
                        bucketTimeLength ) )
      currentBucketStart += bucketTimeLength
      bucketTimeLength = self.__getBucketTimeLength( typeName, nowEpoch, currentBucketStart )
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
    retVal = self._insert( "type%s" % typeName, self.dbCatalog[ typeName ][ 'typeFields' ], insertList )
    if not retVal[ 'OK' ]:
      return retVal
    return self.__splitInBuckets( typeName, startTime, endTime, valuesList )

  def __splitInBuckets( self, typeName, startTime, endTime, valuesList, connObj = False ):
    """
    Bucketize a record
    """
    buckets = self.__calculateBuckets( typeName, startTime, endTime )
    numKeys = len( self.dbCatalog[ typeName ][ 'keys' ] )
    keyValues = valuesList[ :numKeys ]
    valuesList = valuesList[ numKeys: ]
    for bucketInfo in buckets:
      self.dbLocks[ "bucket%s" % typeName ].acquire()
      try:
        bucketStartTime = bucketInfo[0]
        bucketLength = bucketInfo[2]
        retVal = self.__getBucketFromDB( typeName,
                                         bucketStartTime,
                                         bucketLength,
                                         keyValues, connObj = connObj )
        if not retVal[ 'OK' ]:
          return retVal
        proportionalValues = []
        for value in valuesList:
          proportionalValues.append( value * bucketInfo[1] )
        if len( retVal[ 'Value' ] ) == 0:
          retVal = self.__insertBucket( typeName,
                                        bucketStartTime,
                                        bucketLength,
                                        keyValues,
                                        proportionalValues, connObj = connObj )
          if not retVal[ 'OK' ]:
            return retVal
        else:
          bucketValues = retVal[ 'Value' ][0]
          for pos in range( len( bucketValues ) ):
            proportionalValues[ pos ] += bucketValues[ pos ]
          retVal = self.__updateBucket( typeName,
                                        bucketStartTime,
                                        bucketLength,
                                        keyValues,
                                        proportionalValues, connObj = connObj )
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

  def __getBucketFromDB( self, typeName, startTime, bucketLength, keyValues, connObj = False ):
    """
    Get a bucket from the DB
    """
    sqlFields = []
    for valueField in self.dbCatalog[ typeName ][ 'values' ]:
      sqlFields.append( "`bucket%s`.`%s`" % ( typeName, valueField ) )
    cmd = "SELECT %s FROM `bucket%s`" % ( ", ".join( sqlFields ), typeName )
    cmd += " WHERE `bucket%s`.`startTime`='%s' AND `bucket%s`.`bucketLength`='%s' AND " % (
                                                                              typeName,
                                                                              startTime,
                                                                              typeName,
                                                                              bucketLength )
    cmd += self.__generateSQLConditionForKeys( typeName, keyValues )
    return self._query( cmd, conn = connObj )

  def __updateBucket( self, typeName, startTime, bucketLength, keyValues, bucketValues, connObj = False ):
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
    cmd += " WHERE `bucket%s`.`startTime`='%s' AND `bucket%s`.`bucketLength`='%s' AND " % (
                                                                            typeName,
                                                                            startTime,
                                                                            typeName,
                                                                            bucketLength )
    cmd += self.__generateSQLConditionForKeys( typeName, keyValues )
    return self._update( cmd, conn = connObj )

  def __insertBucket( self, typeName, startTime, bucketLength, keyValues, bucketValues, connObj = False ):
    """
    Insert a bucket when coming from the raw insert
    """
    sqlFields = [ 'startTime', 'bucketLength' ]
    sqlValues = [ startTime.strftime( "%Y-%m-%d %H:%M:%S" ), bucketLength ]
    for keyPos in range( len( self.dbCatalog[ typeName ][ 'keys' ] ) ):
      sqlFields.append( self.dbCatalog[ typeName ][ 'keys' ][ keyPos ] )
      sqlValues.append( keyValues[ keyPos ] )
    for valPos in range( len( self.dbCatalog[ typeName ][ 'values' ] ) ):
      sqlFields.append( self.dbCatalog[ typeName ][ 'values' ][ valPos ] )
      sqlValues.append( bucketValues[ valPos ] )
    return self._insert( "bucket%s" % typeName, sqlFields, sqlValues, conn = connObj )

  def __checkFieldsExistsInType( self, typeName, fields, tableType ):
    """
    Check wether a list of fields exist for a given typeName
    """
    missing = []
    for key in fields:
      if key not in self.dbCatalog[ typeName ][ '%sFields' % tableType ]:
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
    print startTime
    nowEpoch = Time.toEpoch( Time.dateTime () )
    startEpoch = Time.toEpoch( startTime )
    bucketTimeLength = self.__getBucketTimeLength( typeName, nowEpoch , startEpoch )
    print bucketTimeLength
    print startEpoch
    startEpoch = int( startEpoch / bucketTimeLength ) * bucketTimeLength
    print startEpoch
    startTime = Time.fromEpoch( startEpoch )
    print startTime
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
    #Calculate fields to retrieve
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
    #Calculate tables needed
    keysInRequestedFields = [ value[0] for value in valueFields ]
    sqlFromList = [ "`%s%s`" % ( tableType, typeName ) ]
    for key in self.dbCatalog[ typeName ][ 'keys' ]:
      if key in condDict or key in groupFields or key in keysInRequestedFields:
        sqlFromList.append( "`key%s`" % key )
    cmd += " FROM %s" % ", ".join( sqlFromList )
    #Calculate time conditions
    sqlTimeCond = []
    if startTime:
      sqlTimeCond.append( "`%s%s`.`startTime` >= '%s'" % ( tableType,
                                               typeName,
                                               startTime.strftime( "%Y-%m-%d %H:%M:%S" )
                                               )
                                            )
    if endTime:
      if tableType == "bucket":
        endTimeSQLVar = "startTime"
      else:
        endTimeSQLVar = "endTime"
      sqlTimeCond.append( "`%s%s`.`%s` <= '%s'" % ( tableType,
                                               typeName,
                                               endTimeSQLVar,
                                               endTime.strftime( "%Y-%m-%d %H:%M:%S" )
                                               )
                                            )
    cmd += " WHERE %s" % " AND ".join( sqlTimeCond )
    #Calculate conditions
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
          sqlORList.append( "`%s%s`.`%s` = %s" % ( tableType, typeName, keyName, keyValue ) )
      sqlCondList.append( "( %s )" % " OR ".join( sqlORList ) )
    if sqlCondList:
      cmd += " AND %s" % " AND ".join( sqlCondList )
    #Calculate grouping
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

  def compactBuckets( self ):
    """
    Compact buckets for all defined types
    """
    for typeName in self.dbCatalog:
      self.__compactBucketsForType( typeName )
    return S_OK()

  def __selectForCompactBuckets(self, typeName, timeLimit, bucketLength, nextBucketLength, connObj = False ):
    """
    Nasty SQL query to get ideal buckets using grouping by date calculations and adding value contents
    """
    selectSQL = "SELECT "
    sqlSelectList = []
    for field in self.dbCatalog[ typeName ][ 'keys' ]:
      sqlSelectList.append( "`bucket%s`.`%s`" % ( typeName, field ) )
    for field in self.dbCatalog[ typeName ][ 'values' ]:
      sqlSelectList.append( "SUM( `bucket%s`.`%s` )" % ( typeName, field ) )
    sqlSelectList.append( "MIN( `bucket%s`.`startTime` )" % typeName)
    sqlSelectList.append( "MAX( `bucket%s`.`startTime` )" % typeName)
    selectSQL += ", ".join( sqlSelectList )
    selectSQL += " FROM `bucket%s`" % typeName
    selectSQL += " WHERE `bucket%s`.`startTime` <= '%s' AND" % ( typeName, timeLimit.strftime( "%Y-%m-%d %H:%M:%S" ) )
    selectSQL += " `bucket%s`.`bucketLength` = %s" % ( typeName, bucketLength )
    #HACK: Horrible constant to overcome the fact that MySQL defines epoch 0 as 13:00 and *nix define epoch as 01:00
    #43200 is half a day
    sqlGroupList = [ "CONVERT( ( UNIX_TIMESTAMP( `bucket%s`.`startTime` ) - 43200 )/%s, SIGNED )" % ( typeName, nextBucketLength ) ]
    for field in self.dbCatalog[ typeName ][ 'keys' ]:
      sqlGroupList.append( "`bucket%s`.`%s`" % ( typeName, field ) )
    selectSQL += " GROUP BY %s" % ", ".join( sqlGroupList )
    return self._query( selectSQL, conn = connObj )

  def __deleteForCompactBuckets( self, typeName, timeLimit, bucketLength, connObj = False ):
    """
    Delete compacted buckets
    """
    deleteSQL = "DELETE FROM `bucket%s` WHERE " % typeName
    deleteSQL += "`bucket%s`.`startTime` <= '%s' AND " % ( typeName, timeLimit.strftime( "%Y-%m-%d %H:%M:%S" ) )
    deleteSQL += "`bucket%s`.`bucketLength` = %s" % ( typeName, bucketLength )
    return self._update( deleteSQL, conn = connObj )

  def __compactBucketsForType( self, typeName ):
    """
    Compact all buckets for a given type
    """
    nowEpoch = Time.toEpoch()
    retVal = self._getConnection()
    if not retVal[ 'OK' ]:
      return retVal
    connObj = retVal[ 'Value' ]
    for bPos in range( len( self.dbBucketsLength[ typeName ] ) - 1 ):
      secondsLimit = self.dbBucketsLength[ typeName ][ bPos ][0]
      bucketLength = self.dbBucketsLength[ typeName ][ bPos ][1]
      nextBucketLength = self.dbBucketsLength[ typeName ][ bPos + 1 ][1]
      endEpoch = int( nowEpoch / bucketLength ) * bucketLength - secondsLimit
      timeLimit = Time.fromEpoch( endEpoch )
      #Retrieve the data
      self.dbLocks[ "bucket%s" % typeName ].acquire()
      try:
        retVal = self.__selectForCompactBuckets( typeName, timeLimit, bucketLength, nextBucketLength, connObj )
        if not retVal[ 'OK' ]:
          return retVal
        bucketsData = retVal[ 'Value' ]
        if len( bucketsData ) == 0:
          continue
        retVal = self.__deleteForCompactBuckets( typeName, timeLimit, bucketLength, connObj )
        if not retVal[ 'OK' ]:
          return retVal
      finally:
        self.dbLocks[ "bucket%s" % typeName ].release()
      gLogger.info( "Compacting %s records %s seconds size for %s" % ( len( bucketsData ), bucketLength, typeName ) )
      #Add data
      for record in bucketsData:
        startTime = record[-2]
        endTime = record[-1]
        valuesList = record[:-2]
        retVal = self.__splitInBuckets( typeName, startTime, endTime, valuesList, connObj )
        if not retVal[ 'OK' ]:
          gLogger.error( "Error while compacting data for record in %s: %s" % ( typeName, retVal[ 'Value' ] ) )


