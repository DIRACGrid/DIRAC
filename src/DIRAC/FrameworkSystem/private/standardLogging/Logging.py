"""
Logging
"""
from __future__ import annotations
import logging
import os

from DIRAC import S_ERROR
from DIRAC.Core.Utilities.LockRing import LockRing
from DIRAC.FrameworkSystem.private.standardLogging.LogLevels import LogLevels, LogLevel
from DIRAC.Resources.LogFilters.SensitiveDataFilter import SensitiveDataFilter


class Logging:
    """
    - Logging is a wrapper of the logger object from the standard "logging" library which integrates some DIRAC concepts.
    - It aimed at seamlessly replacing the previous gLogger implementation and thus provides the same interface.
    - Logging is generally used to create log records, that are then sent to pre-determined backends.

    Each Logging embeds a logger of "logging". Logging can instanciate "children" logging objects and
    all Logging objects inherit from the configuration of LoggingRoot, the first Logging object to be instanciated.
    """

    # componentName is a class variable: the component name is the same for every Logging objects
    # its default value is "Framework" but it can be configured in initialize() in LoggingRoot
    # it can be composed by the system name and the component name. For instance: "Monitoring/Atom"
    _componentName = "Framework"
    # use the lockRing singleton to save the Logging object
    _lockRing = LockRing()
    # lock the configuration of the Logging
    _lockConfig = _lockRing.getLock("config")

    def __init__(self, father: Logging | None = None, name: str = ""):
        """
        Initialization of the Logging object. By default, 'name' is empty,
        because getChild only accepts string and the first empty string corresponds to the root logger.
        Example:
        >>> logging.getLogger('') == logging.getLogger('root') # root logger
        >>> logging.getLogger('root').getChild('log') == logging.getLogger('log') # log child of root

        :param Logging father: father of this new Logging.
        :param str name: name of the logger in the chain.
        """

        # Logging chain
        self._children = {}
        self._parent = father
        # name of the Logging
        self.name = str(name)

        if self._parent is not None:
            # initialize display options and level with the ones of the Logging parent
            self._options = self._parent.getDisplayOptions()
            # initialize the logging.Logger instance (<parent name>.<name of sublogger>)
            self._logger = logging.getLogger(self._parent._logger.name).getChild(self.name)
            # update the custom name of the Logging adding the new Logging name in the entire path
            self._customName = os.path.join("/", self._parent._customName, self.name)
        else:
            # default value for Logging with no parent
            self._options = {
                "headerIsShown": True,
                "timeStampIsShown": True,
                "contextIsShown": True,
                "threadIDIsShown": False,
                "color": False,
            }
            # default logging.logger
            self._logger = logging.getLogger(self.name)
            self._customName = ""

        # dictionary of the options modifications: give the same behaviour that the "logging" level
        # - propagation from the parent to the children when their levels are not set by the developer
        # - stop the propagation when a developer set a level to a child
        self._optionsModified = {
            "headerIsShown": False,
            "timeStampIsShown": False,
            "contextIsShown": False,
            "threadIDIsShown": False,
        }

        self._backendsList = []

        # add a filter to remove sensitive data from the logs
        self._logger.addFilter(SensitiveDataFilter())

        # Locks to make Logging thread-safe
        # we use RLock to prevent blocking in the Logging
        # lockInit to protect the initialization of a sublogger
        self._lockInit = self._lockRing.getLock("init")
        # lockOptions to protect the option modifications and the backendsList
        self._lockOptions = self._lockRing.getLock("options", recursive=True)
        # lockLevel to protect the level
        self._lockLevel = self._lockRing.getLock("level", recursive=True)
        # lockObjectLoader to protect the ObjectLoader singleton
        self._lockObjectLoader = self._lockRing.getLock("objectLoader")

    def showHeaders(self, yesno: bool = True):
        """
        Depending on the value, display or not the prefix of the message.

        :param bool yesno: determine the log record format
        """
        self._setOption("headerIsShown", yesno)

    def showThreadIDs(self, yesno: bool = True):
        """
        Depending on the value, display or not the thread ID.
        Make sure to enable the headers: showHeaders(True) before

        :param bool yesno: determe the log record format
        """
        self._setOption("threadIDIsShown", yesno)

    def showTimeStamps(self, yesno: bool = True):
        """
        Depending on the value, display or not the timestamp of the message.
        Make sure to enable the headers: showHeaders(True) before

        :param bool yesno: determine the log record format
        """
        self._setOption("timeStampIsShown", yesno)

    def showContexts(self, yesno: bool = True):
        """
        Depending on the value, display or not the context of the message.
        Make sure to enable the headers: showHeaders(True) before

        :param bool yesno: determine the log record format
        """
        self._setOption("contextIsShown", yesno)

    def _setOption(self, optionName: str, value: bool, directCall: bool = True):
        """
        Depending on the value, modify the value of the option and propagate the option to the children.
        The options of the children will be updated if they were not modified before by a developer.

        :param str optionName: name of the option to modify
        :param bool value: value of the option to set
        :param bool directCall: indicate whether the call is performed by a developer
        """
        # lock to prevent that two threads change the options at the same time
        self._lockOptions.acquire()
        try:
            if self._optionsModified.get(optionName) and not directCall:
                return

            if directCall:
                self._optionsModified[optionName] = True

            # update option
            self._options[optionName] = value

            # propagate in the children
            for child in self._children.values():
                child._setOption(optionName, value, directCall=False)  # pylint: disable=protected-access
        finally:
            self._lockOptions.release()

    def registerBackend(
        self, desiredBackend: str, backendOptions: dict | None = None, backendFilters: dict | None = None
    ) -> bool:
        """
        Attach a backend to the Logging object.
        Convert backend name to backend class name to a Backend object and add it to the Logging object

        :param desiredBackend: a name attaching to a backend type. List of possible values: ['stdout', 'stderr', 'file']
        :param backendOptions: dictionary of different backend options. Example: FileName='/tmp/log.txt'
        :param backendFilters: dictionary of different backend filters. Example: {'ModuleFilter': {'dirac': 'ERROR'}}

        :returns: Success or failure of registration
        :rtype: bool
        """
        # Remove white space and capitalize the first letter
        desiredBackend = desiredBackend.strip()
        desiredBackend = desiredBackend[0].upper() + desiredBackend[1:]
        _class = self.__loadLogClass(f"Resources.LogBackends.{desiredBackend}Backend")
        if not _class["OK"]:
            self.warn(f"{desiredBackend} is not a valid backend name.")
            return False

        filterInstances = []
        if backendFilters:
            for filterName, filterOptions in backendFilters.items():
                filterInstances.append(self._generateFilter(filterOptions.get("Plugin", filterName), filterOptions))

        # add the backend instance to the Logging
        self._addBackend(_class["Value"], backendOptions, filterInstances)
        return True

    def _addBackend(self, backendType, backendOptions: dict | None = None, backendFilters: list | None = None):
        """
        Attach a Backend object to the Logging object.

        :param Backend backend: Backend object that has to be added
        :param backendOptions: a dictionary of different backend options. Example: {'FileName': '/tmp/log.txt'}
        :param backendFilters: list of different instances of backend filters.
        """
        # lock to prevent that the level change before adding the new backend in the backendsList
        # and to prevent a change of the backendsList during the reading of the
        # list
        self._lockLevel.acquire()
        self._lockOptions.acquire()
        try:
            backend = backendType(backendOptions, backendFilters)
            self._logger.addHandler(backend.getHandler())
            self._backendsList.append(backend)
        finally:
            self._lockLevel.release()
            self._lockOptions.release()

    def _generateFilter(self, filterType: str, filterOptions: dict | None = None):
        """
        Create a filter and add it to the handler of the backend.

        :param str filterType: type of logging Filter
        :param dict filterOptions: parameters of the filter
        """
        _class = self.__loadLogClass(f"Resources.LogFilters.{filterType}")
        if not _class["OK"]:
            self.warn(f"{filterType!r} is not a valid Filter type.")
            return None
        return _class["Value"](filterOptions)

    def setLevel(self, levelName: str) -> bool:
        """
        Check if the level name exists and set it.

        :param levelName: string representing the level to give to the logger
        :return: boolean representing if the setting is done or not
        """
        if isinstance(levelName, LogLevel):
            self._logger.setLevel(levelName)
            return True
        elif isinstance(levelName, str):
            if levelName.upper() in LogLevels.getLevelNames():
                self._logger.setLevel(LogLevels.getLevelValue(levelName))
            return True
        return False

    def getLevel(self) -> str:
        """
        :return: the name of the level
        """
        return LogLevels.getLevel(self._logger.getEffectiveLevel())

    def shown(self, levelName: str) -> bool:
        """
        Determine whether messages with a certain level will be displayed.

        :param levelName: string representing the level to analyse

        :return: boolean which give the answer
        """
        # lock to prevent a level change
        self._lockLevel.acquire()
        try:
            result = False
            if levelName.upper() in LogLevels.getLevelNames():
                result = LogLevels.getLevelValue(self.getLevel()) <= LogLevels.getLevelValue(levelName)
            return result
        finally:
            self._lockLevel.release()

    @classmethod
    def getName(cls) -> str:
        """
        :return: "system name/component name"
        """
        return cls._componentName

    def getSubName(self) -> str:
        """
        :return: the name of the logger
        """
        return self.name

    def getDisplayOptions(self) -> dict[str, bool]:
        """
        :return: the dictionary of the display options and their values. Must not be redefined
        """
        # lock to save the options which can be modified
        self._lockOptions.acquire()
        try:
            # copy the dictionary to avoid that every Logging has the same
            options = self._options.copy()
            return options
        finally:
            self._lockOptions.release()

    def __loadLogClass(self, modulePath: str):
        """Load class thread-safe."""
        # import ObjectLoader here to avoid a dependancy loop
        from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader

        objLoader = ObjectLoader()
        # lock to avoid problem in ObjectLoader which is a singleton not
        # thread-safe
        self._lockObjectLoader.acquire()
        try:
            # load the Backend class
            return objLoader.loadObject(modulePath)
        finally:
            self._lockObjectLoader.release()

    @staticmethod
    def getAllPossibleLevels() -> list[str]:
        """
        :return: a list of all levels available
        """
        return LogLevels.getLevelNames()

    def always(self, sMsg: str, sVarMsg: str = "") -> bool:
        """
        Always level
        """
        return self._createLogRecord(LogLevels.ALWAYS, sMsg, sVarMsg)

    def notice(self, sMsg: str, sVarMsg: str = "") -> bool:
        """
        Notice level
        """
        return self._createLogRecord(LogLevels.NOTICE, sMsg, sVarMsg)

    def info(self, sMsg: str, sVarMsg: str = "") -> bool:
        """
        Info level
        """
        return self._createLogRecord(LogLevels.INFO, sMsg, sVarMsg)

    def verbose(self, sMsg: str, sVarMsg: str = "") -> bool:
        """
        Verbose level
        """
        return self._createLogRecord(LogLevels.VERBOSE, sMsg, sVarMsg)

    def debug(self, sMsg: str, sVarMsg: str = "") -> bool:
        """
        Debug level
        """
        return self._createLogRecord(LogLevels.DEBUG, sMsg, sVarMsg)

    def warn(self, sMsg: str, sVarMsg: str = "") -> bool:
        """
        Warn
        """
        return self._createLogRecord(LogLevels.WARN, sMsg, sVarMsg)

    def error(self, sMsg: str, sVarMsg: str = "") -> bool:
        """
        Error level
        """
        return self._createLogRecord(LogLevels.ERROR, sMsg, sVarMsg)

    def exception(self, sMsg: str = "", sVarMsg: str = "", lException: bool = False, lExcInfo: bool = False) -> bool:
        """
        Exception level
        """
        _ = lException  # Make pylint happy
        _ = lExcInfo
        return self._createLogRecord(LogLevels.ERROR, sMsg, sVarMsg, exc_info=True)

    def fatal(self, sMsg: str, sVarMsg: str = "") -> bool:
        """
        Fatal level
        """
        return self._createLogRecord(LogLevels.FATAL, sMsg, sVarMsg)

    def _createLogRecord(
        self, level: int, sMsg: str, sVarMsg: str, exc_info: bool = False, local_context: dict | None = None
    ) -> bool:
        """
        Create a log record according to the level of the message.

        - The log record is always sent to the different backends
        - Backends have their own levels and may manage the display of the log record

        :param int level: level of the log record
        :param str sMsg: message
        :param str sVarMsg: additional message
        :param bool exc_info: indicates whether the stacktrace has to appear in the log record
        :param dict local_context: Extra information propagated as extra to the formater.
                                   It is meant to be used only by the LocalSubLogger

        :return: boolean representing the result of the log record creation
        """

        # lock to prevent a level change after that the log is sent.
        self._lockLevel.acquire()
        try:
            # exc_info is only for exception to add the stack trace

            # extra is a way to add extra attributes to the log record:
            # - 'componentname': the system/component name
            # - 'varmessage': the variable message
            # - 'customname' : the name of the logger for the DIRAC usage: without 'root' and separated with '/'
            # as log records, extras attributes are not camel case
            extra = {
                "componentname": self._componentName,
                "varmessage": str(sVarMsg),
                "spacer": "" if not sVarMsg else " ",
                "customname": self._customName,
            }

            # options such as headers and threadIDs also depend on the logger, we have to add them to extra
            extra.update(self._options)

            # This typically contains local custom names
            if local_context:
                extra.update(local_context)

            self._logger.log(level, "%s", sMsg, exc_info=exc_info, extra=extra)
            # check whether the message is displayed
            isSent = LogLevels.getLevelValue(self.getLevel()) <= level
            return isSent
        finally:
            self._lockLevel.release()

    def showStack(self) -> bool:
        """
        Display a debug message without any content.

        :return: boolean, True if the message is sent, else False
        """
        return self.debug("")

    def getSubLogger(self, subName: str) -> Logging:
        """
        Create a new Logging object, child of this Logging, if it does not exists.

        .. warning::

           For very short lived sub logger, consider :py:meth:`getLocalSubLogger` instead

        :param str subName: name of the child Logging
        """

        # lock to prevent that the method initializes two Logging for the same 'logging' logger
        # and to erase the existing _children[subName]
        self._lockInit.acquire()
        try:
            # Check if the object has a child with "subName".
            result = self._children.get(subName)
            if result is not None:
                return result
            # create a new child Logging
            childLogging = Logging(self, subName)

            self._children[subName] = childLogging
            return childLogging
        finally:
            self._lockInit.release()

    class LocalSubLogger:
        """
        This is inspired from the standard LoggingAdapter.
        The idea is to provide an interface which looks like a Logger,
        but does not implement all the features.
        You can basically just create it, and log messages.
        You cannot create subLogger from it

        This is to be used for very short lived sub logger. It allows to
        give context information (like a jobID) without creating a new logging
        object, which ends up eating all the memory
        (see https://github.com/DIRACGrid/DIRAC/issues/5280)
        """

        def __init__(self, logger: Logging, extra: dict):
            """
            :param logger: :py:class:`Logging` object on which to be based
            :param extra: dictionary of extra information to be passed
            """

            self.logger = logger
            self.extra = extra

        def always(self, sMsg: str, sVarMsg: str = "") -> bool:
            """
            Always level
            """
            return self.logger._createLogRecord(  # pylint: disable=protected-access
                LogLevels.ALWAYS, sMsg, sVarMsg, local_context=self.extra
            )

        def notice(self, sMsg: str, sVarMsg: str = "") -> bool:
            """
            Notice level
            """
            return self.logger._createLogRecord(  # pylint: disable=protected-access
                LogLevels.NOTICE, sMsg, sVarMsg, local_context=self.extra
            )

        def info(self, sMsg: str, sVarMsg: str = "") -> bool:
            """
            Info level
            """
            return self.logger._createLogRecord(  # pylint: disable=protected-access
                LogLevels.INFO, sMsg, sVarMsg, local_context=self.extra
            )

        def verbose(self, sMsg: str, sVarMsg: str = "") -> bool:
            """
            Verbose level
            """
            return self.logger._createLogRecord(  # pylint: disable=protected-access
                LogLevels.VERBOSE, sMsg, sVarMsg, local_context=self.extra
            )

        def debug(self, sMsg: str, sVarMsg: str = "") -> bool:
            """
            Debug level
            """
            return self.logger._createLogRecord(  # pylint: disable=protected-access
                LogLevels.DEBUG, sMsg, sVarMsg, local_context=self.extra
            )

        def warn(self, sMsg: str, sVarMsg: str = "") -> bool:
            """
            Warn
            """
            return self.logger._createLogRecord(  # pylint: disable=protected-access
                LogLevels.WARN, sMsg, sVarMsg, local_context=self.extra
            )

        def error(self, sMsg: str, sVarMsg: str = "") -> bool:
            """
            Error level
            """
            return self.logger._createLogRecord(  # pylint: disable=protected-access
                LogLevels.ERROR, sMsg, sVarMsg, local_context=self.extra
            )

        def exception(
            self, sMsg: str = "", sVarMsg: str = "", lException: bool = False, lExcInfo: bool = False
        ) -> bool:
            """
            Exception level
            """
            _ = lException  # Make pylint happy
            _ = lExcInfo
            return self.logger._createLogRecord(  # pylint: disable=protected-access
                LogLevels.ERROR, sMsg, sVarMsg, exc_info=True, local_context=self.extra
            )

        def fatal(self, sMsg: str, sVarMsg: str = "") -> bool:
            """
            Fatal level
            """
            return self.logger._createLogRecord(  # pylint: disable=protected-access
                LogLevels.FATAL, sMsg, sVarMsg, local_context=self.extra
            )

    def getLocalSubLogger(self, subName: str) -> Logging.LocalSubLogger:
        """
        Create a subLogger which is meant to have very short lifetime,
        (e.g. when you want to add the jobID in the name)

        .. warning::
          This is a light version of a logger, read the documentation of
          :py:class:`LocalSubLogger` carefully

        :param str subName: name of the child Logging
        """

        return Logging.LocalSubLogger(self, dict(local_name=subName))
