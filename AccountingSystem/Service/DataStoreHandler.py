# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/AccountingSystem/Service/DataStoreHandler.py,v 1.7 2009/02/18 14:37:30 acasajus Exp $
__RCSID__ = "$Id: DataStoreHandler.py,v 1.7 2009/02/18 14:37:30 acasajus Exp $"
import types
from DIRAC import S_OK, S_ERROR, gConfig
from DIRAC.AccountingSystem.DB.AccountingDB import AccountingDB
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities import Time

gAccountingDB = False

def initializeDataStoreHandler( serviceInfo ):
  global gAccountingDB
  gAccountingDB = AccountingDB()
  gAccountingDB.autoCompactDB()
  return S_OK()

class DataStoreHandler( RequestHandler ):

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
      setupTypeName = "%s_%s" % ( setup, typeName )
      retVal = gAccountingDB.registerType( setupTypeName, definitionKeyFields, definitionAccountingFields, bucketsLength )
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
      setupTypeName = "%s_%s" % ( setup, typeName )
      retVal = gAccountingDB.changeBucketsLength( setupTypeName, bucketsLength )
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
      setupTypeName = "%s_%s" % ( setup, typeName )
      retVal = gAccountingDB.regenerateBuckets( setupTypeName )
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
    return gAccountingDB.getRegisteredTypes()

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
      setupTypeName = "%s_%s" % ( setup, typeName )
      retVal = gAccountingDB.deleteType( setupTypeName )
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
    typeName = "%s_%s" % ( setup, typeName )
    startTime = int( Time.toEpoch( startTime ) )
    endTime = int( Time.toEpoch( endTime ) )
    return gAccountingDB.insertRecordThroughQueue( typeName, startTime, endTime, valuesList )

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
          return S_ERROR( "%s field in the records should be %s" % ( i, expectedType[i] ) )
    for entry in entriesList:
      typeName = "%s_%s" % ( setup, entry[0] )
      startTime = int( Time.toEpoch( entry[1] ) )
      endTime = int( Time.toEpoch( entry[2] ) )
      retVal = gAccountingDB.insertRecordThroughQueue( typeName, startTime, endTime, entry[3] )
      if not retVal[ 'OK' ]:
        return retVal
    return S_OK()

  types_compactDB = []
  def export_compactDB( self ):
    """
    Compact the db by grouping buckets
    """
    return gAccountingDB.compactBuckets()