"""
  DIRAC class to execute consumers

  All DIRAC consumers must inherit from the basic class ConsumerModule

  The class is created based on the AgentReactor example.
  In the most common case, DIRAC consumers are executed using the dirac-consumer command.
  dirac-consumer accepts a list positional arguments. These arguments have the form:
  [DIRAC System Name]/[DIRAC Consumer Name]
  dirac-consumer then:
  - produces a instance of ConsumerReactor
  - loads the required module using the ConsumerReactor.loadModule method
  - starts the consumer itself using the ConsumerReactor.go method

"""

from DIRAC.Core.Base.ConsumerModule import ConsumerModule
from DIRAC.Core.Base.private.ModuleLoader import ModuleLoader
from DIRAC.WorkloadManagementSystem.Consumer.ConsumerTools import getConsumerSection
from DIRAC import S_OK, DError
import errno

def loadConsumerModule( consumerModuleName, hideExceptions = False ):
  """Loads the consumerModule.
  """
  loader = ModuleLoader( "Consumer", getConsumerSection, ConsumerModule )
  #The function loadModules takes as the first argument, the list
  #of modules to load. Even that we actually have only one consumer
  #module we must transform it to a list.
  moduleList = [ consumerModuleName ]
  result = loader.loadModules( moduleList, hideExceptions = hideExceptions )
  if not result[ 'OK' ]:
    return result
  modules = loader.getModules()
  return S_OK( modules[consumerModuleName] )

class ConsumerReactor(object):
  """
    Main interface to DIRAC consumers.
  """

  def __init__( self, systemConsumerModuleName ):
    self.consumerModule = None
    self.system_ConsumerModuleName = systemConsumerModuleName

  def go( self ):
    """Creates an instance of a consumer module and
       initializes it.
    """
    if not self.consumerModule['classObj']:
      return DError(errno.EPERM, 'Consumer module class is not loaded')
    instanceObj = self.consumerModule['classObj']()
    instanceObj.initialize( systemConsumerModuleName =  self.system_ConsumerModuleName )
    return S_OK()
