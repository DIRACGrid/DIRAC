"""
Logging Root
"""

__RCSID__ = "$Id$"

import logging
import time
import sys

from DIRAC.FrameworkSystem.private.standardLogging.LogLevels import LogLevels
from DIRAC.FrameworkSystem.private.standardLogging.Logging import Logging
from DIRAC.FrameworkSystem.private.standardLogging.Backend.StdoutBackend import StdoutBackend
from DIRAC.Core.Utilities import DIRACSingleton


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

    # this line removes some useless information from log records and improves the performances
    logging._srcfile = None

    # initialize the root logger
    # actually a child of the root logger to avoid conflicts with other libraries which used 'logging'
    self._logger = logging.getLogger('dirac')

    # here we redefine the custom name to the empty string to remove the "\" in the display
    self._customName = ""

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
    # use the StdoutBackend directly to avoid dependancy loop with ObjectLoader
    self._addBackend(StdoutBackend())

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

    self._lockConfig.acquire()
    try:
      if not LoggingRoot.__configuredLogging:
        backends = (None, None)
        Logging._componentName = systemName

        # Prepare to remove all the backends from the root Logging as in the old gLogger.
        # store them in a list handlersToRemove.
        # we will remove them later, because some components as ObjectLoader need a backend.
        # this can be useful to have logs only in a file for instance.
        handlersToRemove = []
        for backend in self._backendsList:
          handlersToRemove.append(backend.getHandler())
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

        # Remove the old backends
        for handler in handlersToRemove:
          self._logger.removeHandler(handler)

        levelName = gConfig.getValue("%s/LogLevel" % cfgPath, None)
        if levelName is not None:
          self.setLevel(levelName)

        LoggingRoot.__configuredLogging = True
    finally:
      self._lockConfig.release()

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
