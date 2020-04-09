"""
StderrBackend wrapper
"""

__RCSID__ = "$Id$"

import logging
import sys

from DIRAC.Resources.LogBackends.AbstractBackend import AbstractBackend
from DIRAC.FrameworkSystem.private.standardLogging.Formatter.ColoredBaseFormatter import ColoredBaseFormatter


class StderrBackend(AbstractBackend):
  """
  StderrBackend is used to create an abstraction of the handler and the formatter concepts from logging.
  Here, we gather a StreamHandler object and a BaseFormatter.

  - StreamHandler is from the standard logging library: it is used to write log messages in a desired stream
    so it needs a name: here it is stderr.

  - ColorBaseFormatter is a custom Formatter object, created for DIRAC in order to get the appropriate display
    with color.
    You can find it in FrameworkSystem/private/standardLogging/Formatter
  """

  def __init__(self):
    super(StderrBackend, self).__init__(None, ColoredBaseFormatter)

  def createHandler(self, parameters=None):
    """
    Each backend can initialize its attributes and create its handler with them.

    :params parameters: dictionary of parameters. ex: {'FileName': file.log}
    """
    self._handler = logging.StreamHandler(sys.stderr)
