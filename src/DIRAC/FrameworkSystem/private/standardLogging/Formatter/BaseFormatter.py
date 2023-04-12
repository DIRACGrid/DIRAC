"""
BaseFormatter
"""
import logging
import time


class BaseFormatter(logging.Formatter):
    """
    BaseFormatter is used to format log record to create a string representing a DIRAC log message.
    It is based on the Formatter object of the standard 'logging' library.

    The purpose of this Formatter is only to add a new parameter in the Formatter constructor: options.
    Indeed, all the custom Formatter objects of DIRAC will inherit from it to have this attribute because
    it is a dictionary containing DIRAC specific options useful to create new format.
    """

    def __init__(self, fmt=None, datefmt=None):
        """
        Initialize the formatter without using parameters.
        They are then modified in format()

        :param str fmt: log format: "%(asctime)s UTC %(name)s %(levelname)s: %(message)"
        :param str datefmt: date format: "%Y-%m-%d %H:%M:%S"
        """
        # Initialization of the UTC time
        # Actually, time.gmtime is equal to UTC time: it has its DST flag to 0 which means there is no clock advance
        self.converter = time.gmtime
        super().__init__()

    def format(self, record):
        """
        Overriding.
        format is the main method of the Formatter object because it is the method which transforms
        a log record into a string.
        The format and the dateformat are hardcoded to return a DIRAC log format

        :param record: the log record containing all the information about the log message: name, level, threadid...
        """
        # Generate DIRAC base format
        fmt = ""
        timeStamp = ""
        contextComponent = ""
        thread = ""
        level = ""

        if record.headerIsShown:
            if record.timeStampIsShown:
                timeStamp = "%(asctime)s "
            if record.contextIsShown:
                contextComponentList = ["%(componentname)s%(customname)s"]

                # The local_name is normally only provided when using a
                # local sub logger relying on
                # :py:class:`DIRAC.FrameworkSystem.private.standardLogging.Logging.Logging.LocalSubLogger`
                if hasattr(record, "local_name"):
                    contextComponentList += ["/%(local_name)s"]

                contextComponentList += [" "]
                contextComponent = "".join(contextComponentList)
            if record.threadIDIsShown:
                thread = "[%(thread)d] "
            level = "%(levelname)s"
            fmt += f"{timeStamp}{contextComponent}{thread}{level}: "

        fmt += "%(message)s%(spacer)s%(varmessage)s"

        self._style._fmt = fmt  # pylint: disable=no-member
        return super().format(record)

    def formatTime(self, record, datefmt=None):
        """:py:meth:`logging.Formatter.formatTime` with microsecond precision by default"""
        ct = self.converter(record.created)
        if datefmt:
            s = time.strftime(datefmt, ct)
        else:
            t = time.strftime("%Y-%m-%dT%H:%M:%S", ct)
            s = "%s,%06dZ" % (t, (record.created - int(record.created)) * 1e6)
        return s
