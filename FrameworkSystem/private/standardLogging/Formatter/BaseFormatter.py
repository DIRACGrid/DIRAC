"""
BaseFormatter
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import logging


class BaseFormatter(logging.Formatter):
  """
  BaseFormatter is used to format log record to create a string representing a log message.
  It is based on the Formatter object of the standard 'logging' library.

  The purpose of this Formatter is only to add a new parameter in the Formatter constructor: options.
  Indeed, all the custom Formatter objects of DIRAC will inherit from it to have this attribute because
  it is a dictionary containing DIRAC specific options useful to create new format.
  """

  def __init__(self, fmt, datefmt, options):
    """
    Initialize the formatter with new arguments.

    :params fmt: string representing the format: "%(asctime)s UTC %(name)s %(levelname)s: %(message)"
    :params datefmt: string representing the date format: "%Y-%m-%d %H:%M:%S"
    :params options: dictionary of logging DIRAC options
    """
    super(BaseFormatter, self).__init__(fmt, datefmt)
    self._options = options
