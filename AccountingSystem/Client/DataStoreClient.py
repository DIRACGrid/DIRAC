# $HeadURL$
__RCSID__ = "$Id$"

import time, random, copy
from DIRAC import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.Core.DISET.RPCClient                     import RPCClient
from DIRAC.Core.Utilities.ThreadSafe                import Synchronizer
from DIRAC.Core.Utilities                           import DEncode
from DIRAC.RequestManagementSystem.Client.Request   import Request
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient


gAccountingSynchro = Synchronizer()
random.seed()

class DataStoreClient:
  """
    Class providing front end access to DIRAC Accounting DataStore Service
     - It allows to reduce the interactions with the server by building and list of
    pending Registers to be sent that are sent in a bundle using the commit method.
     - In case the DataStore is down Registers are sent as DISET requests.
  """
  def __init__( self, setup = False, retryGraceTime = 0 ):
    self.__setup = setup
    self.__maxRecordsInABundle = 5000
    self.__registersList = []
    self.__maxTimeRetrying = retryGraceTime
    self.__lastSuccessfulCommit = time.time()
    self.__failoverEnabled = not gConfig.getValue( '/LocalSite/DisableFailover', False )

  def setRetryGraceTime( self, retryGraceTime ):
    """
    Set Timeout to send failing records to Failover if enabled
    """
    self.__maxTimeRetrying = retryGraceTime

  def __checkBaseType( self, obj ):
    """
    Check to find that the class inherits from the Base Type
    """
    for parent in obj.__bases__:
      if parent.__name__ == "BaseAccountingType":
        return True
      if self.__checkBaseType( parent ):
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
    if gConfig.getValue( '/LocalSite/DisableAccounting', False ):
      return S_OK()
    self.__registersList.append( copy.deepcopy( register.getValues() ) )
    return S_OK()

  def disableFailover( self ):
    self.__failoverEnabled = False

  def __getRPCClient( self ):
    if self.__setup:
      return RPCClient( "Accounting/DataStore", setup = self.__setup, timeout = 3600 )
    return RPCClient( "Accounting/DataStore", timeout = 3600 )

  @gAccountingSynchro
  def commit( self ):
    """
    Send the registers in a bundle mode
    """
    rpcClient = self.__getRPCClient()
    sent = 0
    while len( self.__registersList ) > 0:
      registersToSend = self.__registersList[ :self.__maxRecordsInABundle ]
      retVal = rpcClient.commitRegisters( registersToSend )
      if retVal[ 'OK' ]:
        self.__lastSuccessfulCommit = time.time()
      else:
        if self.__failoverEnabled and time.time() - self.__lastSuccessfulCommit > self.__maxTimeRetrying:
          gLogger.verbose( "Sending accounting records to failover" )
          result = _sendToFailover( retVal[ 'rpcStub' ] )
          if not result[ 'OK' ]:
            return result
        else:
          return S_ERROR( "Cannot commit data to DataStore service" )
      sent += len( registersToSend )
      del( self.__registersList[ :self.__maxRecordsInABundle ] )
    return S_OK( sent )

  def remove( self, register ):
    """
    Remove a Register from the Accounting DataStore
    """
    if not self.__checkBaseType( register.__class__ ):
      return S_ERROR( "register is not a valid type (has to inherit from BaseAccountingType" )
    retVal = register.checkValues()
    if not retVal[ 'OK' ]:
      return retVal
    if gConfig.getValue( '/LocalSite/DisableAccounting', False ):
      return S_OK()
    return self.__getRPCClient().remove( *register.getValues() )

  def ping( self ):
    """
    Ping the DataStore service
    """
    return self.__getRPCClient().ping()

def _sendToFailover( rpcStub ):
  """ Create a ForwardDISET operation for failover
  """
  request = Request()
  request.RequestName = "Accounting.DataStore.%s.%s" % ( time.time(), random.random() )
  forwardDISETOp = Operation()
  forwardDISETOp.Type = "ForwardDISET"
  forwardDISETOp.Arguments = DEncode.encode( rpcStub )
  request.addOperation( forwardDISETOp )

  return ReqClient().putRequest( request )


gDataStoreClient = DataStoreClient()
