# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/AccountingSystem/Client/Attic/AccountingClient.py,v 1.2 2008/01/29 19:11:06 acasajus Exp $
__RCSID__ = "$Id: AccountingClient.py,v 1.2 2008/01/29 19:11:06 acasajus Exp $"

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.DISET.RPCClient import RPCClient

class AccountingClient:

  def __init__( self ):
    self.registersList = []

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

  def addRegister( self, register ):
    """
    Add a register to the list to be sent
    """
    if not self.__checkBaseType( register.__class__ ):
      return S_ERROR( "register is not a valid type (has to inherit from BaseAccountingType" )
    retVal = register.checkValues()
    if not retVal[ 'OK' ]:
      return retVal
    self.registersList.append( register.getValues() )
    return S_OK()

  def commit( self ):
    """
    Send the registers in a bundle mode
    """
    rpcClient = RPCClient( "Accounting/Server" )
    while len( self.registersList ) > 0:
      retVal = rpcClient.commitRegisters( self.registersList[ :50 ] )
      if not retVal[ 'OK' ]:
        return retVal
      del( self.registersList[ :50 ] )
    return S_OK()

gAccounting = AccountingClient()
