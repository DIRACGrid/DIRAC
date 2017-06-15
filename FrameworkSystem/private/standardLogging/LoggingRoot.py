"""
Logging Root
"""

__RCSID__ = "$Id$"

import logging
import time
import sys

from DIRAC.FrameworkSystem.private.standardLogging.LogLevels import LogLevels
from DIRAC.FrameworkSystem.private.standardLogging.Logging import Logging


class LoggingRoot(Logging):
  """
  LoggingRoot is a Logging object and it is particular because it is the first parent of the chain.
  In this context, it has more possibilities because it is the one that initializes the logger of the 
  standard logging library and it configures it with the cfg file.

  There is a difference between the parent Logging and the other because the parent defines the behaviour
  of all the Logging objects, so it needs a specific class.  

  LoggingRoot has to be unique, because we want one and only one parent on the top of the chain: that is why 
  we created a singleton to keep it unique. 
  """

  # Boolean preventing that the LoggingRoot be configured more than one time
  __configuredLogging = False

  # The unique instance of Logging Root, initialized at None at the beginning, then will take the LoggingRoot as value.
  __instance = None

  def __new__(cls):
    """
    Initialization of the singleton to keep LoggingRoot unique.
    """
    if LoggingRoot.__instance is None:
      LoggingRoot.__instance = object.__new__(cls)
    return LoggingRoot.__instance

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
    # initialize the root logger
    self._logger = logging.getLogger('')

    # here we redefine the custom name to the empty string to remove the "\" in the display
    self.customName = ""

    # this level is not the Logging level, it is only used to send all log messages to the central logging system
    # to do such an operation, we need to let pass all log messages to the root logger, so all logger needs to be
    # at debug. Then, all the backends have a level associated to a Logging level, which can be changed with the
    # setLevel method of Logging, and these backends will choose to send the log messages or not.
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
    self.registerBackends(['stdout'])

    # configuration of the level and update of the format
    self.__configureLevel()
    self._generateBackendFormat()

  def initialize(self, systemName, cfgPath):
    """
    Configure the root Logging with a cfg file.
    It can be possible to :
    - attach it some backends : LogBackends = stdout,stderr,file,server 
    - attach backend options : BackendOptions { FileName = /tmp/file.log }
    - add colors and the path of the call : LogColor = True, LogShowLine = True
    - precise a level : LogLevel = DEBUG

    :params systemName: string represented as "system name/component name"
    :params cfgPath: string of the cfg file path
    """
    # we have to put the import line here to avoid a dependancy loop
    from DIRAC.ConfigurationSystem.Client.Config import gConfig

    if not LoggingRoot.__configuredLogging:
      backends = (None, None)
      Logging._componentName = systemName

      # Remove all the backends from the root Logging as in the old gLogger.
      # this can be useful to have logs only in a file for instance.
      for backend in self._backendsList:
        self._logger.removeHandler(backend.getHandler())
      del self._backendsList[:]

      # Backend options
      desiredBackends = gConfig.getValue("%s/LogBackends" % cfgPath, ['stdout'])

      retDict = gConfig.getOptionsDict("%s/BackendsOptions" % cfgPath)
      if retDict['OK']:
        backends = (desiredBackends, retDict['Value'])
      else:
        backends = (desiredBackends, None)

      # Format options
      self._options['Color'] = gConfig.getValue("%s/LogColor" % cfgPath, False)

      desiredBackends, backendOptions = backends
      self.registerBackends(desiredBackends, backendOptions)

      levelName = gConfig.getValue("%s/LogLevel" % cfgPath, None)
      if levelName is not None:
        self.setLevel(levelName)

      LoggingRoot.__configuredLogging = True

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
