"""
  DIRAC class to execute consumers

  All DIRAC consumers must inherit from the basic class ConsumerModule

  The class is created based on the AgentReactor example.
  In the most common case, DIRAC consumers are executed using the dirac-consumer command.
  dirac-consumer accepts a list positional arguments. These arguments have the form:
  [DIRAC System Name]/[DIRAC Consumer Name]
  dirac-consumer then:
  - produces a instance of ConsumerReactor
  - loads the required modules using the ConsumerReactor.loadConsumerModules method
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
    self.__consumerModules={}

  def go( self ):
    """
      Main method to control the execution of all configured consumers
    """
    for name in self.__consumerModules:
      instanceObj = self.__consumerModules[name]['classObj' ]()
      instanceObj.execute()
    return S_OK()


  def loadModules( self, modulesList, hideExceptions = False):
    """
      Return all modules required in moduleList
    """
    result = self.__loader.loadModules( modulesList, hideExceptions = hideExceptions )
    if not result[ 'OK' ]:
      return result
    self.__consumerModules = self.__loader.getModules()
    return S_OK()
