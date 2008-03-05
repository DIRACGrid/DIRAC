# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/AccountingSystem/private/Attic/AccountingDB.py,v 1.14 2008/03/05 21:02:27 acasajus Exp $
__RCSID__ = "$Id: AccountingDB.py,v 1.14 2008/03/05 21:02:27 acasajus Exp $"

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
    self.catalogTableName = self.__getTableName( "catalog", "Types" )
    self._createTables( { self.catalogTableName : { 'Fields' : { 'name' : "VARCHAR(64) UNIQUE",
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
    retVal = self._query( "SELECT `name`, `keyFields`, `valueFields`, `bucketsLength` FROM `%s`" % self.catalogTableName )
    if not retVal[ 'OK' ]:
      raise Exception( retVal[ 'Message' ] )
    for typesEntry in retVal[ 'Value' ]:
      typeName = typesEntry[0]
      keyFields = List.fromChar( typesEntry[1], "," )
      valueFields = List.fromChar( typesEntry[2], "," )
      bucketsLength = DEncode.decode( typesEntry[3] )[0]
      self.__addToCatalog( typeName, keyFields, valueFields, bucketsLength )

  def __getTableName( self, tableType, name ):
    """
    Generate table name
    """
    return "ac_%s_%s" % ( tableType, name )

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
    self.dbCatalog[ typeName ][ 'bucketFields' ].extend( [  'entriesInBucket', 'startTime', 'bucketLength' ] )
    self.dbLocks[ self.__getTableName( "bucket", typeName ) ] = threading.Lock()
    self.dbBucketsLength[ typeName ] = bucketsLength
    #ADRI: TEST COMPACT BUCKETS
    #self.dbBucketsLength[ typeName ] = [ ( 86400, 3600 ), ( 15552000, 86400 ), ( 31104000, 604800 ) ]
    for key in keyFields:
      if not key in self.dbLocks:
        self.dbLocks[ self.__getTableName( "key", key ) ] = threading.Lock()

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
        tables[ self.__getTableName( "key", key[0] )  ] = { 'Fields' : { 'id' : 'INTEGER NOT NULL AUTO_INCREMENT',
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
    bucketFieldsDict[ 'entriesInBucket' ] = "FLOAT"
    bucketFieldsDict[ 'startTime' ] = "DATETIME"
    bucketFieldsDict[ 'bucketLength' ] = "INT"
    tables[ self.__getTableName( "bucket", name ) ] = { 'Fields' : bucketFieldsDict,
                                    'Indexes' : indexesDict,
                                  }
    tables[ self.__getTableName( "type", name ) ] = { 'Fields' : fieldsDict,
                                  'Indexes' : indexesDict,
                                }
    retVal = self._createTables( tables )

    if not retVal[ 'OK' ]:
      gLogger.error( "Can't create type %s: %s" % ( name, retVal[ 'Message' ] ) )
      return S_ERROR( "Can't create type %s: %s" % ( name, retVal[ 'Message' ] ) )
    bucketsLength.sort()
    bucketsEncoding = DEncode.encode( bucketsLength )
    self._insert( self.catalogTableName,
                  [ 'name', 'keyFields', 'valueFields', 'bucketsLength' ],
                  [ name, ",".join( keyFieldsList ), ",".join( valueFieldsList ), bucketsEncoding ] )
    self.__addToCatalog( name, keyFieldsList, valueFieldsList, bucketsLength )
    gLogger.info( "Registered type %s" % name )
    return S_OK()

  def getRegisteredTypes( self ):
    """
    Get list of registered types
    """
    retVal = self._query( "SELECT `name`, `keyFields`, `valueFields`, `bucketLength` FROM `%s`" % self.catalogTableName )
    if not retVal[ 'OK' ]:
      return retVal
    typesList = []
    for typeInfo in retVal[ 'Value' ]:
      typesList.append( [ typeInfo[0],
                          List.fromChar( typeInfo[1] ),
                          List.fromChar( typeInfo[2] ),
                          DEncode.decode( typeInfo[3] )
                        ]
                      )
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
        tablesToDelete.append( "`%s`" % self.__getTableName( "key", keyField ) )
        gLogger.info( "Deleting key table", keyField )
        del( self.dbKeys[ keyField ] )
    tablesToDelete.insert( 0, "`%s`" % self.__getTableName( "type", typeName ) )
    tablesToDelete.insert( 0, "`%s`" % self.__getTableName( "bucket", typeName ) )
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
    retVal = self._query( "SELECT `id` FROM `%s` WHERE `value`='%s'" % ( self.__getTableName( "key", keyName ),
                                                                         keyValue ), conn = conn )
    if not retVal[ 'OK' ]:
      return retVal
    if len( retVal[ 'Value' ] ) > 0:
      return S_OK( retVal[ 'Value' ][0][0] )
    return S_ERROR( "Key id %s for value %s does not exist although it shoud" % ( keyName, keyValue ) )

  def __addKeyValue( self, keyName, keyValue ):
    """
      Adds a key value to a key table if not existant
    """
    keyTable = self.__getTableName( "key", keyName )
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

  def calculateBucketLengthForTime( self, typeName, now, when ):
    """
    Get the expected bucket time for a moment in time
    """
    dif = abs( now - when )
    for granuT in self.dbBucketsLength[ typeName ]:
      if dif < granuT[0]:
        return granuT[1]
    return self.maxBucketTime

  def calculateBuckets( self, typeName, startTime, endTime ):
    """
    Magic function for calculating buckets between two times and
    the proportional part for each bucket
    """
    nowEpoch = int( Time.toEpoch( Time.dateTime() ) )
    startEpoch = int( Time.toEpoch( startTime ) )
    endEpoch = int( Time.toEpoch( endTime ) )
    bucketTimeLength = self.calculateBucketLengthForTime( typeName, nowEpoch, startEpoch )
    currentBucketStart = startEpoch - startEpoch % bucketTimeLength
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
      bucketTimeLength = self.calculateBucketLengthForTime( typeName, nowEpoch, currentBucketStart )
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
    retVal = self._insert( self.__getTableName( "type", typeName ),
                           self.dbCatalog[ typeName ][ 'typeFields' ],
                           insertList
                         )
    if not retVal[ 'OK' ]:
      return retVal
    #HACK: One more record to split in the buckets
    valuesList.append(1)
    return self.__splitInBuckets( typeName, startTime, endTime, valuesList )

  def __splitInBuckets( self, typeName, startTime, endTime, valuesList, connObj = False ):
    """
    Bucketize a record
    """
    #Calculate amount of buckets
    buckets = self.calculateBuckets( typeName, startTime, endTime )
    #Separate key values from normal values
    numKeys = len( self.dbCatalog[ typeName ][ 'keys' ] )
    keyValues = valuesList[ :numKeys ]
    valuesList = valuesList[ numKeys: ]
    for bucketInfo in buckets:
      self.dbLocks[ self.__getTableName( "bucket", typeName ) ].acquire()
      try:
        bucketStartTime = bucketInfo[0]
        bucketLength = bucketInfo[2]
        #Discover if bucket existed
        retVal = self.__getBucketFromDB( typeName,
                                         bucketStartTime,
                                         bucketLength,
                                         keyValues, connObj = connObj )
        if not retVal[ 'OK' ]:
          return retVal
        #Calculate proportional values
        proportionalValues = []
        for value in valuesList:
          proportionalValues.append( value * bucketInfo[1] )
        #If no previous bucket, insert this
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
          #Add previous bucket values to the new one and update
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
        self.dbLocks[ self.__getTableName( "bucket", typeName ) ].release()
    return S_OK()

  def getBucketsDef( self, typeName ):
    return self.dbBucketsLength[ typeName ]

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
      realCondList.append( "`%s`.`%s` = %s" % ( self.__getTableName( "bucket", typeName ), keyField, keyValue ) )
    return " AND ".join( realCondList )

  def __getBucketFromDB( self, typeName, startTime, bucketLength, keyValues, connObj = False ):
    """
    Get a bucket from the DB
    """
    tableName = self.__getTableName( "bucket", typeName )
    sqlFields = []
    for valueField in self.dbCatalog[ typeName ][ 'values' ]:
      sqlFields.append( "`%s`.`%s`" % ( tableName, valueField ) )
    sqlFields.append( "`%s`.`entriesInBucket`" % ( tableName ) )
    cmd = "SELECT %s FROM `%s`" % ( ", ".join( sqlFields ), self.__getTableName( "bucket", typeName ) )
    cmd += " WHERE `%s`.`startTime`='%s' AND `%s`.`bucketLength`='%s' AND " % (
                                                                              tableName,
                                                                              startTime,
                                                                              tableName,
                                                                              bucketLength )
    cmd += self.__generateSQLConditionForKeys( typeName, keyValues )
    return self._query( cmd, conn = connObj )

  def __updateBucket( self, typeName, startTime, bucketLength, keyValues, bucketValues, connObj = False ):
    """
    Update a bucket when coming from the raw insert
    """
    tableName = self.__getTableName( "bucket", typeName )
    cmd = "UPDATE `%s` SET " % tableName
    sqlValList = []
    for pos in range( len( self.dbCatalog[ typeName ][ 'values' ] ) ):
      valueField = self.dbCatalog[ typeName ][ 'values' ][ pos ]
      value = bucketValues[ pos ]
      sqlValList.append( "`%s`.`%s`=%s" % ( tableName, valueField, value ) )
    sqlValList.append( "`%s`.`entriesInBucket`=%s" % ( tableName, bucketValues[-1] ) )
    cmd += ", ".join( sqlValList )
    cmd += " WHERE `%s`.`startTime`='%s' AND `%s`.`bucketLength`='%s' AND " % (
                                                                            tableName,
                                                                            startTime,
                                                                            tableName,
                                                                            bucketLength )
    cmd += self.__generateSQLConditionForKeys( typeName, keyValues )
    return self._update( cmd, conn = connObj )

  def __insertBucket( self, typeName, startTime, bucketLength, keyValues, bucketValues, connObj = False ):
    """
    Insert a bucket when coming from the raw insert
    """
    sqlFields = [ 'startTime', 'bucketLength', 'entriesInBucket' ]
    sqlValues = [ startTime.strftime( "%Y-%m-%d %H:%M:%S" ), bucketLength, bucketValues[-1] ]
    for keyPos in range( len( self.dbCatalog[ typeName ][ 'keys' ] ) ):
      sqlFields.append( self.dbCatalog[ typeName ][ 'keys' ][ keyPos ] )
      sqlValues.append( keyValues[ keyPos ] )
    for valPos in range( len( self.dbCatalog[ typeName ][ 'values' ] ) ):
      sqlFields.append( self.dbCatalog[ typeName ][ 'values' ][ valPos ] )
      sqlValues.append( bucketValues[ valPos ] )
    return self._insert( self.__getTableName( "bucket", typeName ), sqlFields, sqlValues, conn = connObj )

  def __checkFieldsExistsInType( self, typeName, fields, tableType ):
    """
    Check wether a list of fields exist for a given typeName
    """
    missing = []
    tableFields = self.dbCatalog[ typeName ][ '%sFields' % tableType ]
    for key in fields:
      if key not in tableFields:
        missing.append( key )
    return missing

  def __checkIncomingFieldsForQuery( self, typeName, selectFields, condDict, groupFields, orderFields, tableType ):
    missing = self.__checkFieldsExistsInType( typeName, selectFields[1], tableType )
    if missing:
      return S_ERROR( "Value keys %s are not defined" % ", ".join( missing ) )
    missing = self.__checkFieldsExistsInType( typeName, condDict, tableType )
    if missing:
      return S_ERROR( "Condition keys %s are not defined" % ", ".join( missing ) )
    missing = self.__checkFieldsExistsInType( typeName, groupFields, tableType )
    if missing:
      return S_ERROR( "Group fields %s are not defined" % ", ".join( missing ) )
    missing = self.__checkFieldsExistsInType( typeName,  orderFields, tableType )
    if missing:
      return S_ERROR( "Order fields %s are not defined" % ", ".join( missing ) )
    return S_OK()


  def retrieveBucketedData( self, typeName, startTime, endTime, selectFields, condDict, groupFields, orderFields ):
    """
    Get data from the DB
      Parameters:
        typeName -> typeName
        startTime & endTime -> datetime objects. Do I need to explain the meaning?
        selectFields -> tuple containing a string and a list of fields:
                        ( "SUM(%s), %s/%s", ( "field1name", "field2name", "field3name" ) )
        condDict -> conditions for the query
                    key -> name of the field
                    value -> list of possible values
        groupFields -> list of fields to group by
    """
    if typeName not in self.dbCatalog:
      return S_ERROR( "Type %s is not defined" % typeName )
    if len( selectFields ) < 2:
      return S_ERROR( "selectFields has to be a list containing a string and a list of fields" )
    retVal = self.__checkIncomingFieldsForQuery( typeName, selectFields, condDict, groupFields, orderFields, "bucket" )
    if not retVal[ 'OK' ]:
      return retVal
    nowEpoch = Time.toEpoch( Time.dateTime () )
    startEpoch = Time.toEpoch( startTime )
    bucketTimeLength = self.calculateBucketLengthForTime( typeName, nowEpoch , startEpoch )
    startEpoch = int( startEpoch / bucketTimeLength ) * bucketTimeLength
    startTime = Time.fromEpoch( startEpoch )
    return self.__queryType( typeName,
                             startTime,
                             endTime,
                             selectFields,
                             condDict,
                             groupFields,
                             orderFields,
                             "bucket" )

  def __queryType( self, typeName, startTime, endTime, selectFields, condDict, groupFields, orderFields, tableType ):
    """
    Execute a query over a main table
    """
    tableName = self.__getTableName( tableType, typeName )
    cmd = "SELECT"
    sqlLinkList = []
    #Calculate fields to retrieve
    realFieldList = []
    for rawFieldName in selectFields[1]:
      keyTable = self.__getTableName( "key", rawFieldName )
      if rawFieldName in self.dbCatalog[ typeName ][ 'keys' ]:
        realFieldList.append( "`%s`.`value`" % keyTable )
        List.appendUnique( sqlLinkList, "`%s`.`%s` = `%s`.`id`" % ( tableName,
                                                                    rawFieldName,
                                                                    keyTable ) )
      else:
        realFieldList.append( "`%s`.`%s`" % ( tableName, rawFieldName ) )
    try:
      cmd += " %s" % selectFields[0] % tuple( realFieldList )
    except Exception, e:
      return S_ERROR( "Error generating select fields string: %s" % str(e) )
    #Calculate tables needed
    sqlFromList = [ "`%s`" % tableName ]
    for key in self.dbCatalog[ typeName ][ 'keys' ]:
      if key in condDict or key in groupFields or key in selectFields[1]:
        sqlFromList.append( "`%s`" % self.__getTableName( "key", key ) )
    cmd += " FROM %s" % ", ".join( sqlFromList )
    #Calculate time conditions
    sqlTimeCond = []
    if startTime:
      sqlTimeCond.append( "`%s`.`startTime` >= '%s'" % ( tableName,
                                               startTime.strftime( "%Y-%m-%d %H:%M:%S" )
                                               )
                                            )
    if endTime:
      if tableType == "bucket":
        endTimeSQLVar = "startTime"
      else:
        endTimeSQLVar = "endTime"
      sqlTimeCond.append( "`%s`.`%s` <= '%s'" % ( tableName,
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
        List.appendUnique( sqlLinkList, "`%s`.`%s` = `%s`.`id`" % ( tableName,
                                                                    keyName,
                                                                    self.__getTableName( "key", keyName )
                                                                    ) )
      if type( condDict[ keyName ] ) not in ( types.ListType, types.TupleType ):
        condDict[ keyName ] = [ condDict[ keyName ] ]
      for keyValue in condDict[ keyName ]:
        retVal = self._escapeString( keyValue )
        if not retVal[ 'OK' ]:
          return retVal
        keyValue = retVal[ 'Value' ]
        if keyName in self.dbCatalog[ typeName ][ 'keys' ]:
          sqlORList.append( "`%s`.`value` = %s" % ( self.__getTableName( "key", keyName ), keyValue ) )
        else:
          sqlORList.append( "`%s`.`%s` = %s" % ( tableName, keyName, keyValue ) )
      sqlCondList.append( "( %s )" % " OR ".join( sqlORList ) )
    if sqlCondList:
      cmd += " AND %s" % " AND ".join( sqlCondList )
    #Calculate grouping
    sqlGroupList = []
    if groupFields:
      for field in groupFields:
        if field in self.dbCatalog[ typeName ][ 'keys' ]:
          List.appendUnique( sqlLinkList, "`%s`.`%s` = `%s`.`id`" % ( tableName,
                                                                      field,
                                                                      self.__getTableName( "key", field )
                                                                    ) )
          sqlGroupList.append( "`%s`.`value`" % self.__getTableName( "key", field ) )
        else:
          sqlGroupList.append( "`%s`.`%s`" % ( tableName, field ) )
    #Calculate ordering
    sqlOrderList = []
    if orderFields:
      for field in orderFields:
        if field in self.dbCatalog[ typeName ][ 'keys' ]:
          List.appendUnique( sqlLinkList, "`%s`.`%s` = `%s`.`id`" % ( tableName,
                                                                      field,
                                                                      self.__getTableName( "key", field )
                                                                    ) )
          sqlOrderList.append( "`%s`.`value`" % self.__getTableName( "key", field ) )
        else:
          sqlOrderList.append( "`%s`.`%s`" % ( tableName, field ) )
    if sqlLinkList:
      cmd += " AND %s" % " AND ".join( sqlLinkList )
    if sqlGroupList:
      cmd += " GROUP BY %s" % ", ".join( sqlGroupList )
    if sqlOrderList:
      cmd += " ORDER BY %s" % ", ".join( sqlOrderList )
    return self._query( cmd )

  @gSynchro
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
    tableName = self.__getTableName( "bucket", typeName )
    selectSQL = "SELECT "
    sqlSelectList = []
    for field in self.dbCatalog[ typeName ][ 'keys' ]:
      sqlSelectList.append( "`%s`.`%s`" % ( tableName, field ) )
    for field in self.dbCatalog[ typeName ][ 'values' ]:
      sqlSelectList.append( "SUM( `%s`.`%s` )" % ( tableName, field ) )
    sqlSelectList.append( "SUM( `%s`.`entriesInBucket` )" % ( tableName ) )
    sqlSelectList.append( "MIN( `%s`.`startTime` )" % tableName )
    sqlSelectList.append( "MAX( `%s`.`startTime` )" % tableName )
    selectSQL += ", ".join( sqlSelectList )
    selectSQL += " FROM `%s`" % tableName
    selectSQL += " WHERE `%s`.`startTime` <= '%s' AND" % ( tableName, timeLimit.strftime( "%Y-%m-%d %H:%M:%S" ) )
    selectSQL += " `%s`.`bucketLength` = %s" % ( tableName, bucketLength )
    #HACK: Horrible constant to overcome the fact that MySQL defines epoch 0 as 13:00 and *nix define epoch as 01:00
    #43200 is half a day
    sqlGroupList = [ "CONVERT( ( UNIX_TIMESTAMP( `%s`.`startTime` ) - 43200 )/%s, SIGNED )" % ( tableName, nextBucketLength ) ]
    for field in self.dbCatalog[ typeName ][ 'keys' ]:
      sqlGroupList.append( "`%s`.`%s`" % ( tableName, field ) )
    selectSQL += " GROUP BY %s" % ", ".join( sqlGroupList )
    return self._query( selectSQL, conn = connObj )

  def __deleteForCompactBuckets( self, typeName, timeLimit, bucketLength, connObj = False ):
    """
    Delete compacted buckets
    """
    tableName = self.__getTableName( "bucket", typeName )
    deleteSQL = "DELETE FROM `%s` WHERE " % tableName
    deleteSQL += "`%s`.`startTime` <= '%s' AND " % ( tableName, timeLimit.strftime( "%Y-%m-%d %H:%M:%S" ) )
    deleteSQL += "`%s`.`bucketLength` = %s" % ( tableName, bucketLength )
    return self._update( deleteSQL, conn = connObj )

  def __compactBucketsForType( self, typeName ):
    """
    Compact all buckets for a given type
    """
    tableName = self.__getTableName( "bucket", typeName )
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
      self.dbLocks[ tableName ].acquire()
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
        self.dbLocks[ tableName ].release()
      gLogger.info( "Compacting %s records %s seconds size for %s" % ( len( bucketsData ), bucketLength, typeName ) )
      #Add data
      for record in bucketsData:
        startTime = record[-2]
        endTime = record[-1]
        valuesList = record[:-2]
        retVal = self.__splitInBuckets( typeName, startTime, endTime, valuesList, connObj )
        if not retVal[ 'OK' ]:
          gLogger.error( "Error while compacting data for record in %s: %s" % ( typeName, retVal[ 'Value' ] ) )


