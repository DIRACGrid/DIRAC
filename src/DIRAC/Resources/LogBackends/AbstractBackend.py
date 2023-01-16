"""
Backend wrapper
"""
from DIRAC.FrameworkSystem.private.standardLogging.LogLevels import LogLevels, LogLevel


class AbstractBackend:
    """
    AbstractBackend is used to create an abstraction of handler and formatter concepts from 'logging'.
    It corresponds to the backend concept of the old gLogger.
    It is useful for different reasons:

    - to gather handler and formatter,
      in DIRAC, each handler must be bind with a specific formatter so
      it is more simple and more clear to create an object for this job.

    - each backend can get its options and
      format them to give them to the handler,
      in DIRAC, it is possible to add backend options in the cfgfile.
      For example, for a file, you can give the filename that you want to write log inside.
      The backend allows to each handler to get its own options as parameters. Again, it is more clear
      and more simple to have a specific object for this job.

    In this way, we have an object composed by one handler and one formatter name.
    The purpose of the object is to get cfg options to give them to the handler,
    and to set the format of the handler when the display must be changed.
    """

    def __init__(self, handlerType, formatterType, backendParams=None, backendFilters=None, level="debug"):
        """
        Initialization of the backend.
        _handler and _formatter can be custom objects. If it is the case, you can find them
        in FrameworkSystem/private/standardLogging/Formatter or Handler.

        :param handlerType: handler object from 'logging'. Ex: StreamHandler(), FileHandler()...
        :param formatterType: the name of a formatter object from logging. Ex: BaseFormatter
        :param dict backendParams: parameters to set up the backend
        :param list backendFilters: list of different instances of 'logging.Filter'.
        :param str level: level of the handler
        """
        # get handler parameters from the backendParams and instantiate the handler
        self._handlerParams = {}
        self._setHandlerParameters(backendParams)
        self._setHandler(handlerType)

        # get formatter parameters from the backendParams, instantiate and attach the formatter to the handler
        self._formatterParams = {}
        self._setFormatterParameters(backendParams)
        self._setFormatter(formatterType)

        # attach filter instances to the handler
        if backendFilters:
            for backendFilter in backendFilters:
                self._addFilter(backendFilter)

        # set the level: can also be defined in the backendParams
        if backendParams:
            level = backendParams.get("LogLevel", level)
        self.setLevel(level)

    def getHandler(self):
        """
        :return: the handler
        """
        return self._handler

    def _setHandler(self, handlerType):
        """
        Instantiate a handler from the given handlerType.

        :param str handlerType: the handler type (e.g. logging.StreamHandler)
        """
        self._handler = handlerType(**self._handlerParams)

    def _setHandlerParameters(self, backendParams=None):
        """
        Get the handler parameters from the backendParams.
        The keys of handlerParams should correspond to the parameter names of the associated handler.
        The method should be overridden in every backend that needs handler parameters.
        The method should be called before creating the handler object.

        :param dict parameters: parameters of the backend. ex: {'FileName': file.log}
        """
        pass

    def _setFormatter(self, formatterType):
        """
        Instantiate a formatter from the given formatterType and attach it to the handler.

        :param str formatterType: the formatter type (e.g. BaseFormatter)
        """
        self._handler.setFormatter(formatterType(**self._formatterParams))

    def _setFormatterParameters(self, backendParams=None):
        """
        Get the formatting option from the backendParams

        :param dict backendParams: parameters of the backend
        """
        # Default values
        self._formatterParams["fmt"] = None
        self._formatterParams["datefmt"] = None

        # Get values from formatterParameters
        if backendParams is not None:
            self._formatterParams["fmt"] = backendParams.get("Format")
            self._formatterParams["datefmt"] = backendParams.get("DateFormat")

    def _addFilter(self, filterInstance):
        """
        Attach a filter instance to the handler.

        :param logging.Filter filterInstance: a logging filter
        """
        self._handler.addFilter(filterInstance)

    def setLevel(self, levelName):
        """
        Configure the level of the handler associated to the backend.
        Make sure the handler has been created before calling the method.

        :param int level: a level
        """
        if isinstance(levelName, LogLevel):
            self._handler.setLevel(levelName)
            return True
        elif isinstance(levelName, str):
            if levelName.upper() in LogLevels.getLevelNames():
                self._handler.setLevel(LogLevels.getLevelValue(levelName))
            return True
        return False
