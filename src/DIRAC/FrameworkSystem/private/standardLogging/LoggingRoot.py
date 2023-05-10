"""
Logging Root
"""

import logging
import time
import sys

from DIRAC.FrameworkSystem.private.standardLogging.LogLevels import LogLevels
from DIRAC.FrameworkSystem.private.standardLogging.Logging import Logging
from DIRAC.Resources.LogBackends.StdoutBackend import StdoutBackend
from DIRAC.Core.Utilities.DIRACSingleton import DIRACSingleton


class LoggingRoot(Logging, metaclass=DIRACSingleton):
    """
    LoggingRoot is a particular Logging object: the first parent of the chain.

    - It is the one that initializes the root logger of the standard logging library and it configures it
    - As it defines the default behaviour of the Logging objects, it needs a specific class
    - It is unique, there is one and only one parent at the top of the chain: this justifies the usage of a Singleton
    """

    # Boolean preventing the LoggingRoot to be configured once more
    __configuredLogging = False

    def __init__(self):
        """
        Initialization of the LoggingRoot object.
        LoggingRoot :

        - initialize the UTC time
        - set the correct level defines by the user, or the default
        - add the custom level to logging: verbose, notice, always
        - register a default backend (stdout): all messages will be displayed here
        - update the format according to the command line argument

        """
        # initialize the DIRAC root logger, which turns out to be a child of root
        # it is strongly advised to use a logger with a unique and identifiable name:
        # https://docs.python.org/3/howto/logging.html#configuring-logging-for-a-library
        super().__init__(name="dirac")

        # this line removes some useless information from log records and improves the performances
        logging._srcfile = None  # pylint: disable=protected-access

        # disable propagation to avoid any conflicts with external libs that would use "logging" too
        self._logger.propagate = False

        # initialization of levels
        levels = LogLevels.getLevels()
        for level in levels:
            logging.addLevelName(levels[level], level)

        # root Logger level is set to INFO by default
        self._logger.setLevel(LogLevels.INFO)

        # initialization of the default backend
        # use the StdoutBackend directly to avoid dependancy loop with ObjectLoader
        self._addBackend(StdoutBackend)

        # configuration of the level and update the format
        self.__configureLevel()

    def initialize(self, systemName: str, cfgPath: str, forceInit: bool = False):
        """
        Configure the root Logging.
        It can be possible to:

        - attach some backends to it: LogBackends = stdout,stderr,file,server
        - attach backend options: BackendOptions { FileName = /tmp/file.log }
        - attach filters to it and to some backends: see LogFilters
        - add colors: LogColor = True
        - precise a level: LogLevel = DEBUG

        :param str systemName: <system name>/<component name>
        :param str cfgPath: configuration path
        :param bool forceInit: Force the initialization even if it had already happened.
                               This should not be used !! The only case is LocalConfiguration.enableCS
                               In order to take into account extensions' backends
        """
        # we have to put the import line here to avoid a dependancy loop
        from DIRAC import gConfig

        self._lockConfig.acquire()
        try:
            if not LoggingRoot.__configuredLogging or forceInit:
                Logging._componentName = systemName

                # prepare to remove all the backends from the root Logging as in the old gLogger.
                # store them in a list handlersToRemove.
                # we will remove them later, because some components as ObjectLoader need a backend.
                # this can be useful to have logs only in a file for instance.
                handlersToRemove = []
                for backend in self._backendsList:
                    handlersToRemove.append(backend.getHandler())
                del self._backendsList[:]

                # get the backends, the backend options and add them to the root Logging
                backends = self.__getBackendsFromCFG(cfgPath)
                for backend in backends:
                    backendOptions = self.__getBackendOptionsFromCFG(cfgPath, backend)
                    backendFilters = self.__getFilterList(backendOptions)
                    filters = {}
                    for backendFilter in backendFilters:
                        filters[backendFilter] = self.__getFilterOptionsFromCFG(backendFilter)

                    self.registerBackend(backendOptions.get("Plugin", backend), backendOptions, filters)

                # Format options
                self._options["color"] = gConfig.getValue(f"{cfgPath}/LogColor", False)

                # Remove the old backends
                for handler in handlersToRemove:
                    self._logger.removeHandler(handler)

                levelName = self.__getLogLevelFromCFG(cfgPath)
                if levelName is not None:
                    self.setLevel(levelName)

                LoggingRoot.__configuredLogging = True
        finally:
            self._lockConfig.release()

    def __getLogLevelFromCFG(self, cfgPath: str) -> str:
        """
        Get the logging level from the cfg, following this order:

        * cfgPath/LogLevel
        * Operations/<setup/vo>/Logging/Default<Agents/Services>LogLevel
        * Operations/<setup/vo>/Logging/DefaultLogLevel

        :param str cfgPath: configuration path
        """
        # We have to put the import line here to avoid a dependancy loop
        from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
        from DIRAC import gConfig

        # Search desired log level in the component
        logLevel = gConfig.getValue(f"{cfgPath}/LogLevel")
        if not logLevel:
            # get the second last string representing the component type in the configuration
            # example : 'Agents', 'Services'
            component = cfgPath.split("/")[-2]

            operation = Operations()
            # Search desired logLevel in the operation section according to the
            # component type
            logLevel = operation.getValue(f"Logging/Default{component}LogLevel")
            if not logLevel:
                # Search desired logLevel in the operation section
                logLevel = operation.getValue("Logging/DefaultLogLevel")

        return logLevel

    def __getBackendsFromCFG(self, cfgPath: str) -> list[str]:
        """
        Get backends from the configuration and register them in LoggingRoot.
        This is the new way to get the backends providing a general configuration.

        :param str cfgPath: configuration path
        """
        # We have to put the import line here to avoid a dependancy loop
        from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
        from DIRAC import gConfig

        # Search desired backends in the component
        backends = gConfig.getValue(f"{cfgPath}/LogBackends", [])
        if not backends:
            # get the second last string representing the component type in the configuration
            # example : 'Agents', 'Services'
            component = cfgPath.split("/")[-2]

            operation = Operations()
            # Search desired backends in the operation section according to the
            # component type
            backends = operation.getValue(f"Logging/Default{component}Backends", [])
            if not backends:
                # Search desired backends in the operation section
                backends = operation.getValue("Logging/DefaultBackends", [])
                if not backends:
                    # Default value
                    backends = ["stdout"]

        return backends

    def __getBackendOptionsFromCFG(self, cfgPath: str, backend: str) -> dict:
        """
        Get backend options from the configuration.

        :param cfgPath: configuration path
        :param backend: backend identifier: stdout, file, f04
        """
        # We have to put the import lines here to avoid a dependancy loop
        from DIRAC import gConfig
        from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getBackendConfig

        backendOptions = {}
        # Search backends config in the resources section
        retDictRessources = getBackendConfig(backend)
        if retDictRessources["OK"]:
            backendOptions = retDictRessources["Value"]

        # Search backends config in the component to update some options
        retDictConfig = gConfig.getOptionsDict(f"{cfgPath}/LogBackendsConfig/{backend}")
        if retDictConfig["OK"]:
            backendOptions.update(retDictConfig["Value"])

        return backendOptions

    def __getFilterList(self, backendOptions: dict) -> list[str]:
        """
        Return list of defined filters.

        :param dict backendOptions: backend options
        """
        if not (isinstance(backendOptions, dict) and "Filter" in backendOptions):
            return []
        return [fil.strip() for fil in backendOptions["Filter"].split(",") if fil.strip()]

    def __getFilterOptionsFromCFG(self, logFilter: str) -> dict:
        """Get filter options from the configuration..

        :param logFilter: filter identifier
        """
        # We have to put the import lines here to avoid a dependancy loop
        from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getFilterConfig

        # Search filters config in the resources section
        retDictRessources = getFilterConfig(logFilter)
        if retDictRessources["OK"]:
            return retDictRessources["Value"]

        return {}

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
            self.setLevel("verbose")
        elif debLevs == 2:
            self.setLevel("verbose")
            self.showHeaders(True)
        elif debLevs >= 3:
            self.setLevel("debug")
            self.showHeaders(True)
            self.showThreadIDs(True)

    def enableLogsFromExternalLibs(self):
        """
        Enable the display of the logs coming from external libraries

        .. warning::

          This method should only be used for debugging purposes.
        """
        self.__enableLogsFromExternalLibs()

    def disableLogsFromExternalLibs(self):
        """
        Disable the display of the logs coming from external libraries

        .. warning::

          This method should only be used for debugging purposes.
        """
        self.__enableLogsFromExternalLibs(False)

    @staticmethod
    def __enableLogsFromExternalLibs(isEnabled: bool = True):
        """
        Configure the root logger from 'logging' for an external library use.
        By default the root logger is configured with:

        - debug level,
        - stderr output
        - custom format close to the DIRAC format

        :param bool isEnabled: allows logs from external libs
        """
        rootLogger = logging.getLogger()
        rootLogger.handlers = []
        if isEnabled:
            logging.basicConfig(
                level=logging.DEBUG,
                format="%(asctime)s,%(msecs)03dZ ExternalLibrary/%(name)s %(levelname)s: %(message)s",
                datefmt="%Y-%m-%dT%H:%M:%S",
            )
            rootLogger.handlers[0].formatter.converter = time.gmtime
        else:
            rootLogger.addHandler(logging.NullHandler())
