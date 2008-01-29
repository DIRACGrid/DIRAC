# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/AccountingSystem/Service/Attic/AccountingHandler.py,v 1.5 2008/01/29 19:11:06 acasajus Exp $
__RCSID__ = "$Id: AccountingHandler.py,v 1.5 2008/01/29 19:11:06 acasajus Exp $"
import types
from DIRAC import S_OK, S_ERROR
from DIRAC.AccountingSystem.private.AccountingDB import AccountingDB
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities import Time

gAccountingDB = False

def initializeAccountingHandler( serviceInfo ):
  global gAccountingDB
  gAccountingDB = AccountingDB()
  return S_OK()

class AccountingHandler( RequestHandler ):

  types_registerType = [ types.StringType, types.ListType, types.ListType, types.ListType ]
  def export_registerType( self, typeName, definitionKeyFields, definitionAccountingFields, bucketsLength ):
    """
      Register a new type. (Only for all powerful admins)
      (Bow before me for I am admin! :)
    """
    setup = self.serviceInfoDict[ 'clientSetup' ]
    typeName = "%s_%s" % ( typeName, setup )
    return gAccountingDB.registerType( typeName, definitionKeyFields, definitionAccountingFields, bucketsLength )

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
    setup = self.serviceInfoDict[ 'clientSetup' ]
    typeName = "%s_%s" % ( typeName, setup )
    return gAccountingDB.deleteType( typeName )

  types_commit = [ types.StringType, Time._dateTimeType, Time._dateTimeType, types.ListType ]
  def export_commit( self, typeName, startTime, endTime, valuesList ):
    """
      Add a record for a type
    """
    setup = self.serviceInfoDict[ 'clientSetup' ]
    typeName = "%s_%s" % ( typeName, setup )
    return gAccountingDB.addEntry( typeName, startTime, endTime, valuesList )

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
      typeName = "%s_%s" % ( entry[0], setup )
      retVal = gAccountingDB.addEntry( typeName, entry[1], entry[2], entry[3] )
      if not retVal[ 'OK' ]:
        return retVal
    return S_OK()

  types_retrieveBucketedData = [ types.StringType, Time._dateTimeType, Time._dateTimeType, types.DictType, types.ListType, types.ListType ]
  def export_retrieveBucketedData( self, typeName, startTime, endTime, condDict, returnList, groupFields ):
    """
    Get data from the DB
      Parameters:
        typeName -> typeName
        startTime & endTime -> datetime objects. Do I need to explain the meaning?
        condDict -> conditions for the query
                    key -> name of the key field
                    value -> list of possible values
        returnList -> list of value fields to retrieve. Has to contain tuples with:
                        ( <name of value field>, <function to apply> )
        groupFields -> list of fields to group by
    """
    setup = self.serviceInfoDict[ 'clientSetup' ]
    typeName = "%s_%s" % ( typeName, setup )
    return gAccountingDB.retrieveBucketedData( typeName, startTime, endTime, condDict, returnList, groupFields )

  types_compactDB = []
  def export_compactDB( self ):
    """
    Compact the db by grouping buckets
    """
    return gAccountingDB.compactBuckets()