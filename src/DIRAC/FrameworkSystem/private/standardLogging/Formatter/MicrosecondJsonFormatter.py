import time
from pythonjsonlogger.jsonlogger import JsonFormatter


class MicrosecondJsonFormatter(JsonFormatter):
    """Standard JSON formater with microsecond precision"""

    def formatTime(self, record, datefmt=None):
        """:py:meth:`logging.Formatter.formatTime` with microsecond precision by default"""
        ct = self.converter(record.created)
        if datefmt:
            s = time.strftime(datefmt, ct)
        else:
            t = time.strftime("%Y-%m-%d %H:%M:%S", ct)
            s = "%s,%06d" % (t, (record.created - int(record.created)) * 1e6)
        return s
