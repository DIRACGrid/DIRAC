# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/AccountingSystem/Service/Attic/AccountingHandler.py,v 1.4 2008/01/29 15:34:03 acasajus Exp $
__RCSID__ = "$Id: AccountingHandler.py,v 1.4 2008/01/29 15:34:03 acasajus Exp $"
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

  types_registerType = [ types.StringType, types.ListType, types.ListType ]
  def export_registerType( self, typeName, definitionKeyFields, definitionAccountingFields ):
    """
      Register a new type. (Only for all powerful admins)
      (Bow before me for I am admin! :)
    """
    setup = self.serviceInfoDict[ 'clientSetup' ]
    typeName = "%s_%s" % ( typeName, setup )
    return gAccountingDB.registerType( typeName, definitionKeyFields, definitionAccountingFields )

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

  types_addEntryToType = [ types.StringType, Time._dateTimeType, Time._dateTimeType, types.ListType ]
  def export_addEntryToType( self, typeName, startTime, endTime, valuesList ):
    """
      Add a record for a type
    """
    setup = self.serviceInfoDict[ 'clientSetup' ]
    typeName = "%s_%s" % ( typeName, setup )
    return gAccountingDB.addEntry( typeName, startTime, endTime, valuesList )

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