import time
from pythonjsonlogger.jsonlogger import JsonFormatter, RESERVED_ATTRS


class MicrosecondJsonFormatter(JsonFormatter):
    """Standard JSON formater with microsecond precision"""

    def __init__(self, *args, **kwargs):
        """
        Add to the list of attributes we don't want to see
        all the DIRAC spefic log formating instructions
        """
        # Initialization of the UTC time
        # Actually, time.gmtime is equal to UTC time: it has its DST flag to 0 which means there is no clock advance
        self.converter = time.gmtime

        if "reserved_attrs" not in kwargs:
            kwargs["reserved_attrs"] = RESERVED_ATTRS + (
                "spacer",
                "headerIsShown",
                "timeStampIsShown",
                "contextIsShown",
                "threadIDIsShown",
                "color",
            )
        super().__init__(*args, **kwargs)

    def formatTime(self, record, datefmt=None):
        """:py:meth:`logging.Formatter.formatTime` with microsecond precision by default"""
        ct = self.converter(record.created)
        if datefmt:
            s = time.strftime(datefmt, ct)
        else:
            t = time.strftime("%Y-%m-%dT%H:%M:%S", ct)
            s = "%s,%06dZ" % (t, (record.created - int(record.created)) * 1e6)
        return s
