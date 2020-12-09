"""
FileBackend wrapper
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import logging
from os import getpid

from DIRAC.Resources.LogBackends.AbstractBackend import AbstractBackend
from DIRAC.FrameworkSystem.private.standardLogging.Formatter.BaseFormatter import BaseFormatter


class FileBackend(AbstractBackend):
  """
  FileBackend is used to create an abstraction of the handler and the formatter concepts from logging.
  Here, we gather a FileHandler object and a BaseFormatter.

  - FileHandler is from the standard logging library: it is used to write log messages in a desired file
    so it needs a filename.

  - BaseFormatter is a custom Formatter object, created for DIRAC in order to get the appropriate display.
    You can find it in FrameworkSystem/private/standardLogging/Formatter
  """

  def __init__(self, backendParams=None):
    super(FileBackend, self).__init__(logging.FileHandler, BaseFormatter, backendParams)

  def _setHandlerParameters(self, backendParams=None):
    """
    Get the handler parameters from the backendParams.
    The keys of handlerParams should correspond to the parameter names of the associated handler.
    The method should be overridden in every backend that needs handler parameters.
    The method should be called before creating the handler object.

    :param dict parameters: parameters of the backend. ex: {'FileName': file.log}
    """
    # default values
    self._handlerParams['filename'] = 'Dirac-log_%s.log' % getpid()

    if backendParams is not None:
      self._handlerParams['filename'] = backendParams.get('FileName', self._handlerParams['filename'])
