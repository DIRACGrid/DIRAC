# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/AccountingSystem/Client/DataStoreClient.py,v 1.6 2008/11/10 15:23:13 acasajus Exp $
__RCSID__ = "$Id: DataStoreClient.py,v 1.6 2008/11/10 15:23:13 acasajus Exp $"

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities.ThreadSafe import Synchronizer

gAccountingSynchro = Synchronizer()

class DataStoreClient:


  def __init__( self, setup = False ):
    self.__setup = setup
    self.__maxRecordsInABundle = 100
    self.__registersList = []

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
    if self.__setup:
      rpcClient = RPCClient( "Accounting/DataStore", setup = self.__setup )
    else:
      rpcClient = RPCClient( "Accounting/DataStore" )
    sent = 0
    while len( self.__registersList ) > 0:
      registersToSend = self.__registersList[ :self.__maxRecordsInABundle ]
      retVal = rpcClient.commitRegisters( registersToSend )
      if not retVal[ 'OK' ]:
        return retVal
      sent += len( registersToSend )
      del( self.__registersList[ :self.__maxRecordsInABundle ] )
    return S_OK( sent )

gDataStoreClient = DataStoreClient()
