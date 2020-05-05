"""
Formatter as JSON structure
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from pythonjsonlogger.jsonlogger import JsonFormatter as libJsonFormatter

from DIRAC.FrameworkSystem.private.standardLogging.Formatter.BaseFormatter import BaseFormatter

__RCSID__ = "$Id$"


class JsonFormatter(libJsonFormatter, BaseFormatter):
  """ This class will format the log messages as a JSON structure.
      It is just a wrapper around jsonlogger, however we need it because
      the constructor of BaseFormatter takes different arguments
  """

  def __init__(self, fmt, datefmt, options):
    """
        Constructor that just fowards the arguments to the two parents classes
    """

    libJsonFormatter.__init__(self, fmt, datefmt)
    BaseFormatter.__init__(self, fmt, datefmt, options)
