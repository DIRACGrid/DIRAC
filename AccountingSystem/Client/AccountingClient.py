# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/AccountingSystem/Client/Attic/AccountingClient.py,v 1.1 2008/01/24 11:03:33 acasajus Exp $
__RCSID__ = "$Id: AccountingClient.py,v 1.1 2008/01/24 11:03:33 acasajus Exp $"

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.DISET.RPCClient import RPCClient

def checkBaseType( obj ):
  for parent in obj.__bases__:
    if parent.__name__ == "BaseAccountingType":
      return True
    if self.__checkHandler( parent ):
      return True
  return False

def commitAccountingRegister( register ):
  if not checkBaseType( register.__class__ ):
    return S_ERROR( "register is not a valid type (has to inherit from BaseAccountingType" )
  retVal = register.checkValues()
  if not retVal[ 'OK' ]:
    return retVal
  rpcClient = RPCClient( "Accounting/Server" )
  return rpcClient.addEntryToType( *register.getValues() )
