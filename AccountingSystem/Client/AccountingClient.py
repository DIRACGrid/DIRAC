# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/AccountingSystem/Client/Attic/AccountingClient.py,v 1.3 2008/04/01 17:40:34 acasajus Exp $
__RCSID__ = "$Id: AccountingClient.py,v 1.3 2008/04/01 17:40:34 acasajus Exp $"

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities.ThreadSafe import Synchronizer

gAccountingSynchro = Synchronizer()

class AccountingClient:

  __registersList = []

  def __checkBaseType( self, obj ):
    """
    Check to find that the class inherits from the Base Type
    """
    for parent in obj.__bases__:
      if parent.__name__ == "BaseAccountingType":
        return True
      if self.__checkHandler( parent ):
        return True
    return False

  @gAccountingSynchro
  def addRegister( self, register ):
    """
    Add a register to the list to be sent
    """
    if not self.__checkBaseType( register.__class__ ):
      return S_ERROR( "register is not a valid type (has to inherit from BaseAccountingType" )
    retVal = register.checkValues()
    if not retVal[ 'OK' ]:
      return retVal
    self.__registersList.append( register.getValues() )
    return S_OK()

  @gAccountingSynchro
  def commit( self ):
    """
    Send the registers in a bundle mode
    """
    rpcClient = RPCClient( "Accounting/DataStore" )
    while len( self.__registersList ) > 0:
      retVal = rpcClient.commitRegisters( self.__registersList[ :50 ] )
      if not retVal[ 'OK' ]:
        return retVal
      del( self.__registersList[ :50 ] )
    return S_OK()

gAccounting = AccountingClient()
