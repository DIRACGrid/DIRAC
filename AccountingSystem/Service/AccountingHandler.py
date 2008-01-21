# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/AccountingSystem/Service/Attic/AccountingHandler.py,v 1.1 2008/01/21 14:31:02 acasajus Exp $
__RCSID__ = "$Id: AccountingHandler.py,v 1.1 2008/01/21 14:31:02 acasajus Exp $"
import types
from DIRAC.ConfigurationSystem.private.ServiceInterface import ServiceInterface
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR

gServiceInterface = False

def initializeAccountingHandler( serviceInfo ):
  global gServiceInterface
  gServiceInterface = ServiceInterface( serviceInfo[ 'URL' ] )
  return S_OK()

class AccountingHandler( RequestHandler ):
