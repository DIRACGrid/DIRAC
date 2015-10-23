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
  - starts the execution loop using the ConsumerReactor.go method

"""

from DIRAC.Core.Base.ConsumerModule import ConsumerModule
from DIRAC.Core.Base.private.ModuleLoader import ModuleLoader
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC import S_OK


class ConsumerReactor(object):
  """
    Main interface to DIRAC consumers.
  """

  def __init__( self ):
    self.__loader = ModuleLoader( "Consumer", PathFinder.getConsumerSection, ConsumerModule )
    self.__consumerModule = None

  def go( self ):
    """
      Main method to control the execution of the consumer.
    """
    instanceObj = self.__consumerModule['classObj']()
    print "im here"
    instanceObj.initialize()
    print "well will never be here"
    return S_OK()


  def loadModule( self, consumerModuleName, hideExceptions = False):
    """Loads the consumerModule.
    """
    #The function loadModules takes as the first argument, the list
    #of modules to load. Even that we actually have only one consumer
    #module we must transform it to a list.
    moduleList = [ consumerModuleName ]
    result = self.__loader.loadModules( moduleList, hideExceptions = hideExceptions )
    if not result[ 'OK' ]:
      return result
    modules = self.__loader.getModules()
    self.__consumerModule = modules[consumerModuleName]
    return S_OK()
