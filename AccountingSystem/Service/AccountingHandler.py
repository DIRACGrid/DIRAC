# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/AccountingSystem/Service/Attic/AccountingHandler.py,v 1.2 2008/01/23 18:41:07 acasajus Exp $
__RCSID__ = "$Id: AccountingHandler.py,v 1.2 2008/01/23 18:41:07 acasajus Exp $"
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
  def export_registerType( self, name, keyFields, valuesFields ):
    """
      Register a new type. (Only for all powerful admins)
      (Bow before me for I am admin! :)
    """
    return gAccountingDB.registerType( name, keyFields, valuesFields )

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
    return gAccountingDB.deleteType( typeName )

  types_addEntryToType = [ types.StringType, Time._dateTimeType, Time._dateTimeType, types.ListType ]
  def export_addEntryToType( self, typeName, startTime, endTime, valuesList ):
    """
      Add a record for a type
    """
    return gAccountingDB.addEntry( typeName, startTime, endTime, valuesList )