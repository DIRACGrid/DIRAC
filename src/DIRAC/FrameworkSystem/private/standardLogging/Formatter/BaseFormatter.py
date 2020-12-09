"""
BaseFormatter
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import logging
import sys


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
    super(BaseFormatter, self).__init__()

  def format(self, record):
    """
    Overriding.
    format is the main method of the Formatter object because it is the method which transforms
    a log record into a string.
    The format and the dateformat are hardcoded to return a DIRAC log format

    :param record: the log record containing all the information about the log message: name, level, threadid...
    """
    # Generate DIRAC base format
    fmt = ''
    timeStamp = ''
    contextComponent = ''
    thread = ''
    level = ''

    if record.headerIsShown:
      if record.timeStampIsShown:
        timeStamp = '%(asctime)s UTC '
      if record.contextIsShown:
        contextComponent = '%(componentname)s%(customname)s '
      if record.threadIDIsShown:
        thread = '[%(thread)d] '
      level = '%(levelname)s'
      fmt += '%s%s%s%s: ' % (timeStamp, contextComponent, thread, level)

    fmt += '%(message)s%(spacer)s%(varmessage)s'

    # Formatter class does not have the same arguments between Python2 and 3
    # To prevent breaking the Formatter during the migration to Python3, we check the version
    if sys.version_info < (3,):
      self._fmt = fmt
    else:
      self._style._fmt = fmt  # pylint: disable=no-member
    self.datefmt = '%Y-%m-%d %H:%M:%S'
    return super(BaseFormatter, self).format(record)
