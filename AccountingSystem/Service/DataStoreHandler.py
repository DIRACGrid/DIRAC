# $HeadURL$
__RCSID__ = "$Id$"
import types
from DIRAC import S_OK, S_ERROR, gConfig
from DIRAC.AccountingSystem.DB.MultiAccountingDB import MultiAccountingDB
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities import Time
from DIRAC.Core.Utilities.ThreadScheduler import gThreadScheduler

class DataStoreHandler( RequestHandler ):

  __acDB = False

  @classmethod
  def initializeHandler( cls, svcInfoDict ):
    cls.__acDB = MultiAccountingDB( svcInfoDict[ 'serviceSectionPath' ] )
    cls.__acDB.autoCompactDB()
    result = cls.__acDB.markAllPendingRecordsAsNotTaken()
    if not result[ 'OK' ]:
      return result
    gThreadScheduler.addPeriodicTask( 60, cls.__acDB.loadPendingRecords )
    return S_OK()

  types_registerType = [ types.StringType, types.ListType, types.ListType, types.ListType ]
  def export_registerType( self, typeName, definitionKeyFields, definitionAccountingFields, bucketsLength ):
    """
      Register a new type. (Only for all powerful admins)
      (Bow before me for I am admin! :)
    """
    retVal = gConfig.getSections( "/DIRAC/Setups" )
    if not retVal[ 'OK' ]:
      return retVal
    errorsList = []
    for setup in retVal[ 'Value' ]:
      retVal = self.__acDB.registerType( setup, typeName, definitionKeyFields, definitionAccountingFields, bucketsLength )
      if not retVal[ 'OK' ]:
        errorsList.append( retVal[ 'Message' ] )
    if errorsList:
      return S_ERROR( "Error while registering type:\n %s" % "\n ".join( errorsList ) )
    return S_OK()

  types_setBucketsLength = [ types.StringType, types.ListType ]
  def export_setBucketsLength( self, typeName, bucketsLength ):
    """
      Change the buckets Length. (Only for all powerful admins)
      (Bow before me for I am admin! :)
    """
    retVal = gConfig.getSections( "/DIRAC/Setups" )
    if not retVal[ 'OK' ]:
      return retVal
    errorsList = []
    for setup in retVal[ 'Value' ]:
      retVal = self.__acDB.changeBucketsLength( setup, typeName, bucketsLength )
      if not retVal[ 'OK' ]:
        errorsList.append( retVal[ 'Message' ] )
    if errorsList:
      return S_ERROR( "Error while changing bucketsLength type:\n %s" % "\n ".join( errorsList ) )
    return S_OK()

  types_regenerateBuckets = [ types.StringType ]
  def export_regenerateBuckets( self, typeName ):
    """
      Recalculate buckets. (Only for all powerful admins)
      (Bow before me for I am admin! :)
    """
    retVal = gConfig.getSections( "/DIRAC/Setups" )
    if not retVal[ 'OK' ]:
      return retVal
    errorsList = []
    for setup in retVal[ 'Value' ]:
      retVal = self.__acDB.regenerateBuckets( setup, typeName )
      if not retVal[ 'OK' ]:
        errorsList.append( retVal[ 'Message' ] )
    if errorsList:
      return S_ERROR( "Error while recalculating buckets for type:\n %s" % "\n ".join( errorsList ) )
    return S_OK()

  types_getRegisteredTypes = []
  def export_getRegisteredTypes( self ):
    """
      Get a list of registered types (Only for all powerful admins)
      (Bow before me for I am admin! :)
    """
    return self.__acDB.getRegisteredTypes()

  types_deleteType = [ types.StringType ]
  def export_deleteType( self, typeName ):
    """
      Delete accounting type and ALL its contents. VERY DANGEROUS! (Only for all powerful admins)
      (Bow before me for I am admin! :)
    """
    retVal = gConfig.getSections( "/DIRAC/Setups" )
    if not retVal[ 'OK' ]:
      return retVal
    errorsList = []
    for setup in retVal[ 'Value' ]:
      retVal = self.__acDB.deleteType( setup, typeName )
      if not retVal[ 'OK' ]:
        errorsList.append( retVal[ 'Message' ] )
    if errorsList:
      return S_ERROR( "Error while deleting type:\n %s" % "\n ".join( errorsList ) )
    return S_OK()

  types_commit = [ types.StringType, Time._dateTimeType, Time._dateTimeType, types.ListType ]
  def export_commit( self, typeName, startTime, endTime, valuesList ):
    """
      Add a record for a type
    """
    setup = self.serviceInfoDict[ 'clientSetup' ]
    startTime = int( Time.toEpoch( startTime ) )
    endTime = int( Time.toEpoch( endTime ) )
    return self.__acDB.insertRecordThroughQueue( setup, typeName, startTime, endTime, valuesList )

  types_commitRegisters = [ types.ListType ]
  def export_commitRegisters( self, entriesList ):
    """
      Add a record for a type
    """
    setup = self.serviceInfoDict[ 'clientSetup' ]
    expectedTypes = [ types.StringType, Time._dateTimeType, Time._dateTimeType, types.ListType ]
    for entry in entriesList:
      if len( entry ) != 4:
        return S_ERROR( "Invalid records" )
      for i in range( len( entry ) ):
        if type( entry[i] ) != expectedTypes[i]:
          return S_ERROR( "%s field in the records should be %s" % ( i, expectedTypes[i] ) )
    records = []
    for entry in entriesList:
      startTime = int( Time.toEpoch( entry[1] ) )
      endTime = int( Time.toEpoch( entry[2] ) )
      records.append( ( setup, entry[0], startTime, endTime, entry[3] ) )
    return self.__acDB.insertRecordBundleThroughQueue( records )


  types_compactDB = []
  def export_compactDB( self ):
    """
    Compact the db by grouping buckets
    """
    return self.__acDB.compactBuckets()

  types_remove = [ types.StringType, Time._dateTimeType, Time._dateTimeType, types.ListType ]
  def export_remove( self, typeName, startTime, endTime, valuesList ):
    """
      Remove a record for a type
    """
    setup = self.serviceInfoDict[ 'clientSetup' ]
    startTime = int( Time.toEpoch( startTime ) )
    endTime = int( Time.toEpoch( endTime ) )
    return self.__acDB.deleteRecord( setup, typeName, startTime, endTime, valuesList )

  types_removeRegisters = [ types.ListType ]
  def export_removeRegisters( self, entriesList ):
    """
      Remove a record for a type
    """
    setup = self.serviceInfoDict[ 'clientSetup' ]
    expectedTypes = [ types.StringType, Time._dateTimeType, Time._dateTimeType, types.ListType ]
    for entry in entriesList:
      if len( entry ) != 4:
        return S_ERROR( "Invalid records" )
      for i in range( len( entry ) ):
        if type( entry[i] ) != expectedTypes[i]:
          return S_ERROR( "%s field in the records should be %s" % ( i, expectedTypes[i] ) )
    ok = 0
    for entry in entriesList:
      startTime = int( Time.toEpoch( entry[1] ) )
      endTime = int( Time.toEpoch( entry[2] ) )
      record = entry[3]
      result = self.__acDB.deleteRecord( setup, entry[0], startTime, endTime, record )
      if not result[ 'OK' ]:
        return S_OK( ok )
      ok += 1

    return S_OK( ok )
