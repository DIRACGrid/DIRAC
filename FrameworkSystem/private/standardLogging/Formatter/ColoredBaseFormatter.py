"""
ColoredBaseFormatter
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import sys

from DIRAC.FrameworkSystem.private.standardLogging.Formatter.BaseFormatter import BaseFormatter


class ColoredBaseFormatter(BaseFormatter):
  """
  ColoredBaseFormatter is used to format log record to create a string representing a log message.
  It is based on the BaseFormatter object which is based on the of the standard logging library.

  This custom formatter is useful for format messages to correspond with the gLogger format.
  It adds color on all messages which come from StdoutBackend and StderrBackend
  and color them according to their levels.
  """
  COLOR_MAP = {
      'black': 0,
      'red': 1,
      'green': 2,
      'yellow': 3,
      'blue': 4,
      'magenta': 5,
      'cyan': 6,
      'white': 7
  }

  LEVEL_MAP = {
      'ALWAYS': ('black', 'white', False),
      'NOTICE': (None, 'magenta', False),
      'INFO': (None, 'green', False),
      'VERBOSE': (None, 'cyan', False),
      'DEBUG': (None, 'blue', False),
      'WARN': (None, 'yellow', False),
      'ERROR': (None, 'red', False),
      'FATAL': ('red', 'black', False)
  }

  def __init__(self, fmt=None, datefmt=None):
    """
    Initialize the formatter without using parameters.
    They are then modified in format()

    :param str fmt: log format: "%(asctime)s UTC %(name)s %(levelname)s: %(message)"
    :param str datefmt: date format: "%Y-%m-%d %H:%M:%S"
    """
    super(ColoredBaseFormatter, self).__init__()

  def format(self, record):
    """
    Overriding.
    format is the main method of the Formatter object because it is the method which transforms
    a log record into a string with colors.
    According to the level, the method get colors from LEVEL_MAP to add them to the message.

    :param record: the log record containing all the information about the log message: name, level, threadid...
    """

    stringRecord = super(ColoredBaseFormatter, self).format(record)

    # post treatment
    if record.color and sys.stdout.isatty() and sys.stderr.isatty():
      params = []
      bg, fg, bold = self.LEVEL_MAP[record.levelname]
      if bg in self.COLOR_MAP:
        params.append(str(self.COLOR_MAP[bg] + 40))
      if fg in self.COLOR_MAP:
        params.append(str(self.COLOR_MAP[fg] + 30))
      if bold:
        params.append('1')
      stringRecord = ("".join(('\x1b[', ";".join(params), 'm', stringRecord, '\x1b[0m')))

    return stringRecord
