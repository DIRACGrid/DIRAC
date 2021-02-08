"""
Logging Root
"""

from __future__ import print_function
__RCSID__ = "$Id$"

import logging
import time
import sys

from DIRAC.FrameworkSystem.private.standardLogging.LogLevels import LogLevels
from DIRAC.FrameworkSystem.private.standardLogging.Logging import Logging
from DIRAC.Resources.LogBackends.StdoutBackend import StdoutBackend
from DIRAC.Core.Utilities import DIRACSingleton


class LoggingRoot(Logging):
  """
  LoggingRoot is a Logging object and it is particular because it is the first parent of the chain.
  In this context, it has more possibilities because it is the one that initializes the logger of the
  standard logging library and it configures it with the configuration.

  There is a difference between the parent Logging and the other because the parent defines the behaviour
  of all the Logging objects, so it needs a specific class.

  LoggingRoot has to be unique, because we want one and only one parent on the top of the chain: that is why
  we created a singleton to keep it unique.
  """
  __metaclass__ = DIRACSingleton.DIRACSingleton

  # Boolean preventing that the LoggingRoot be configured more than one time
  __configuredLogging = False

  def __init__(self):
    """
    Initialization of the LoggingRoot object.
    LoggingRoot :
    - initialize the UTC time
    - set the correct level defines by the user, or the default
    - add the custom level to logging: verbose, notice, always
    - register a default backend: stdout : all messages will be displayed here
    - update the format according to the command line argument
    """
    super(LoggingRoot, self).__init__()

    # this line removes some useless information from log records and improves
    # the performances
    logging._srcfile = None  # pylint: disable=protected-access

    # initialize the root logger
    # actually a child of the root logger to avoid conflicts with other
    # libraries which used 'logging'
    self._logger = logging.getLogger('dirac')
    # prevent propagation to the root logger to avoid conflicts with external libraries
    # which want to use the root logger
    self._logger.propagate = False

    # here we redefine the custom name to the empty string to remove the "\"
    # in the display
    self._customName = ""

    # this level is not the Logging level, it is only used to send all log messages to the central logging system
    # to do such an operation, we need to let pass all log messages to the root logger, so all logger needs to be
    # at debug. Then, all the backends have a level associated to a Logging level, which can be changed with the
    # setLevel method of Logging, and these backends will choose to send the
    # log messages or not.
    self._logger.setLevel(LogLevels.DEBUG)

    # initialization of the UTC time
    # Actually, time.gmtime is equal to UTC time because it has its DST flag to 0
    # which means there is no clock advance
    logging.Formatter.converter = time.gmtime

    # initialization of levels
    levels = LogLevels.getLevels()
    for level in levels:
      logging.addLevelName(levels[level], level)

    # initialization of the default backend
    self._setLevel(LogLevels.NOTICE)
    # use the StdoutBackend directly to avoid dependancy loop with ObjectLoader
    self._addBackend(StdoutBackend())

    # configuration of the level and update of the format
    self.__configureLevel()
    self._generateBackendFormat()

  def initialize(self, systemName, cfgPath, forceInit=False):
    """
    Configure the root Logging.
    It can be possible to :
    - attach it some backends : LogBackends = stdout,stderr,file,server
    - attach backend options : BackendOptions { FileName = /tmp/file.log }
    - add colors and the path of the call : LogColor = True, LogShowLine = True
    - precise a level : LogLevel = DEBUG

    :params systemName: string represented as "system name/component name"
    :params cfgPath: string of the configuration path
    :params forceInit: Force the initialization even if it had already happened.
                       This should not be used !! The only case is LocalConfiguration.enableCS
                       In order to take into account extensions' backends
    """
    # we have to put the import line here to avoid a dependancy loop
    from DIRAC import gConfig

    self._lockConfig.acquire()
    try:
      if not LoggingRoot.__configuredLogging or forceInit:
        Logging._componentName = systemName

        # Prepare to remove all the backends from the root Logging as in the old gLogger.
        # store them in a list handlersToRemove.
        # we will remove them later, because some components as ObjectLoader need a backend.
        # this can be useful to have logs only in a file for instance.
        handlersToRemove = []
        for backend in self._backendsList:
          handlersToRemove.append(backend.getHandler())
        del self._backendsList[:]

        # get the backends, the backend options and add them to the root
        # Logging
        desiredBackends = self.__getBackendsFromCFG(cfgPath)
        for backend in desiredBackends:
          desiredOptions = self.__getBackendOptionsFromCFG(cfgPath, backend)
          self.registerBackend(desiredOptions.get(
              'Plugin', backend), desiredOptions)

        # Format options
        self._options['Color'] = gConfig.getValue(
            "%s/LogColor" % cfgPath, False)

        # Remove the old backends
        for handler in handlersToRemove:
          self._logger.removeHandler(handler)

        levelName = gConfig.getValue("%s/LogLevel" % cfgPath, None)
        if levelName is not None:
          self.setLevel(levelName)

        LoggingRoot.__configuredLogging = True
    finally:
      self._lockConfig.release()

  def __getBackendsFromCFG(self, cfgPath):
    """
    Get backends from the configuration and register them in LoggingRoot.
    This is the new way to get the backends providing a general configuration.

    :params cfgPath: string of the configuration path
    """
    # We have to put the import line here to avoid a dependancy loop
    from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
    from DIRAC import gConfig

    # get the second last string representing the component type in the configuration
    # example : 'Agents', 'Services'
    component = cfgPath.split("/")[-2]
    operation = Operations()

    # Search desired backends in the component
    desiredBackends = gConfig.getValue("%s/%s" % (cfgPath, 'LogBackends'), [])
    if not desiredBackends:
      # Search desired backends in the operation section according to the
      # component type
      desiredBackends = operation.getValue(
          "Logging/Default%sBackends" % component, [])
      if not desiredBackends:
        # Search desired backends in the operation section
        desiredBackends = operation.getValue("Logging/DefaultBackends", [])
        if not desiredBackends:
          # Default value
          desiredBackends = ['stdout']

    return desiredBackends

  def __getBackendOptionsFromCFG(self, cfgPath, backend):
    """
    Get backend options from the configuration.

    :params cfgPath: string of the configuration path
    :params backend: string representing a backend identifier: stdout, file, f04
    """
    # We have to put the import lines here to avoid a dependancy loop
    from DIRAC import gConfig
    from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getBackendConfig

    backendOptions = {}
    # Search backends config in the resources section
    retDictRessources = getBackendConfig(backend)
    if retDictRessources['OK']:
      backendOptions = retDictRessources['Value']

    # Search backends config in the component to update some options
    retDictConfig = gConfig.getOptionsDict(
        "%s/%s/%s" % (cfgPath, 'LogBackendsConfig', backend))
    if retDictConfig['OK']:
      backendOptions.update(retDictConfig['Value'])
    else:
      # Search backends config in the component with the old option
      # 'BackendsOptions'
      retDictOptions = gConfig.getOptionsDict("%s/BackendsOptions" % cfgPath)
      if retDictOptions['OK']:
        # We have to write the deprecated message with the print method because we are changing
        # the backends, so we can not be sure of the display using a log.
        print("WARNING: Use of a deprecated cfg section: BackendsOptions. Please replace it by BackendConfig.")
        backendOptions.update(retDictOptions['Value'])

    return backendOptions

  def __configureLevel(self):
    """
    Configure the log level of the root Logging according to the argv parameter
    It can be : -d, -dd, -ddd
    Work only for clients, scripts and tests
    Configuration/Client/LocalConfiguration manages services,agents and executors
    """
    debLevs = 0
    for arg in sys.argv:
      if arg.find("-d") == 0:
        debLevs += arg.count("d")
    if debLevs == 1:
      self._setLevel(LogLevels.VERBOSE)
    elif debLevs == 2:
      self._setLevel(LogLevels.VERBOSE)
      self.showHeaders(True)
    elif debLevs >= 3:
      self._setLevel(LogLevels.DEBUG)
      self.showHeaders(True)
      self.showThreadIDs(True)

  def enableLogsFromExternalLibs(self):
    """
    Enable the display of the logs coming from external libraries
    """
    self.__enableLogsFromExternalLibs()

  def disableLogsFromExternalLibs(self):
    """
    Disable the display of the logs coming from external libraries
    """
    self.__enableLogsFromExternalLibs(False)

  @staticmethod
  def __enableLogsFromExternalLibs(isEnabled=True):
    """
    Configure the root logger from 'logging' for an external library use.
    By default the root logger is configured with:
    - debug level,
    - stderr output
    - custom format close to the DIRAC format

    :params isEnabled: boolean value. True allows the logs in the external lib,
                       False do not.
    """
    rootLogger = logging.getLogger()
    rootLogger.handlers = []
    if isEnabled:
      logging.basicConfig(level=logging.DEBUG,
                          format='%(asctime)s UTC ExternalLibrary/%(name)s %(levelname)s: %(message)s',
                          datefmt='%Y-%m-%d %H:%M:%S')
    else:
      rootLogger.addHandler(logging.NullHandler())
