"""
Backend wrapper
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"


class AbstractBackend(object):
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

  def __init__(self, handler, formatter):
    """
    Initialization of the backend.
    _handler and _formatter can be custom objects. If it is the case, you can find them
    in FrameworkSystem/private/standardLogging/Formatter or Handler.

    :params _handler: handler object from 'logging'. Ex: StreamHandler(), FileHandler()...
    :params _formatter: the name of a formatter object from logging. Ex: BaseFormatter
    """
    self._handler = handler
    self._formatter = formatter

  def createHandler(self, parameters=None):
    """
    Each backend can initialize its attributes and create its handler with them.

    :params parameters: dictionary of parameters. ex: {'FileName': file.log}
    """
    raise NotImplementedError("setParameter not implemented")

  def getHandler(self):
    """
    :return: the handler
    """
    return self._handler

  def setFormat(self, fmt, datefmt, options):
    """
    Each backend give a format to their formatters and attach them to their handlers.

    :params fmt: string representing the log format
    :params datefmt: string representing the date format
    :params component: string represented as "system/component"
    :params options: dictionary of logging options. ex: {'Color': True}
    """
    self._handler.setFormatter(self._formatter(fmt, datefmt, options))

  def setLevel(self, level):
    """
    Configure the level of the handler associated to the backend.

    :params level: integer representing a level
    """
    self._handler.setLevel(level)

  @staticmethod
  def createFormat(options):
    """
    Create a format from the options given in parameters.

    :params options: dictionary of options of the Logging which wants a new format
    :params level: integer representing the level of the Logging object which wants a new format
    :return: tuple containing two strings: a format and a date format
    """
    fmt = ''
    datefmt = '%Y-%m-%d %H:%M:%S'
    if options['headerIsShown']:
      fmt += '%(asctime)s UTC %(componentname)s%(customname)s'
      if options['threadIDIsShown']:
        fmt += ' [%(thread)d]'
      fmt += ' %(levelname)s: '
    fmt += '%(message)s%(spacer)s%(varmessage)s'
    return (datefmt, fmt)
