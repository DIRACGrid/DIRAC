"""
Logging
"""

__RCSID__ = "$Id$"

import logging
import os

from DIRAC.FrameworkSystem.private.standardLogging.LogLevels import LogLevels

from DIRAC.FrameworkSystem.private.standardLogging.Backend.AbstractBackend import AbstractBackend
from DIRAC.FrameworkSystem.private.standardLogging.Backend.StdoutBackend import StdoutBackend
from DIRAC.FrameworkSystem.private.standardLogging.Backend.StderrBackend import StderrBackend
from DIRAC.FrameworkSystem.private.standardLogging.Backend.FileBackend import FileBackend
from DIRAC.FrameworkSystem.private.standardLogging.Backend.RemoteBackend import RemoteBackend


class Logging(object):
  """
  Logging is a wrapper of the logger object from the standard "logging" library which integrate
  some DIRAC concepts. It is the equivalent to the old gLogger object.

  It is used like an interface to use the logger object of the "logging" library. 
  Its purpose is to replace transparently the old gLogger object in the existing code in order to 
  minimize the changes. 

  In this way, each Logging embed a logger of "logging". It is possible to create sublogger,
  set and get the level of the embedded logger and create log messages with it.

  Logging could delegate the initialization and the configuration to a factory of the root logger be it can not
  because it has to wrap the old gLogger.  

  Logging should not be instancied directly. It is LoggingRoot which is instancied and which instantiates Logging
  objects.
  """

  # componentName is a class variable because the component name is the same for every Logging objects
  # its default value is "Framework" but it can be configured in initialize() in LoggingRoot
  # it can be composed by the system name and the component name. For instance: "Monitoring/Atom"
  _componentName = "Framework"

  # all the different backends
  _BACKENDSDICT = {'stdout': StdoutBackend,
                   'stderr': StderrBackend,
                   'file': FileBackend,
                   'server': RemoteBackend}

  def __init__(self, father=None, fatherName='', name='', customName=''):
    """
    Initialization of the Logging object.
    :params father: Logging, father of this new Logging.
    :params fatherName: string representing the name of the father logger in the chain.
    :params name: string representing the name of the logger in the chain. 
    :params customName: string representing the name of the logger in the chain: 
                        - "root" does not appear at the beginning of the chain
                        - hierarchy "." are replaced by "\"
                        useful for the display of the Logging name
    By default, 'fatherName' and 'name' are empty, because getChild accepts only string and the first empty
    string corresponds to the root logger. 

    Example: 
    logging.getLogger('') == logging.getLogger('root') == root logger
    logging.getLogger('root').getChild('log') == root.log == log child of root
    """

    # Logging chain
    self._children = {}
    self._parent = father

    # initialize display options and level with the ones of the Logging parent
    if self._parent is not None:
      self._options = self._parent.getDisplayOptions()
      self._level = LogLevels.getLevelValue(father.getLevel())
    else:
      self._options = {'headerIsShown': True, 'threadIDIsShown': False, 'Color': False}
      # the native level is not used because it has to be to debug to send all messages to the log central
      self._level = None

    # dictionary of the option state, modified by the user or not
    # this is to give to the options the same behaviour that the "logging" level:
    # - propagation from the parent to the children when their levels are not set by the developer himself
    # - stop the propagation when a developer set a level to a child
    self._optionsModified = {'headerIsShown': False, 'threadIDIsShown': False}
    self._levelModified = False

    self._backendsList = []

    self._logger = logging.getLogger(fatherName).getChild(name)

    # name of the Logging
    self.name = name

    # update the custom name of the Logging adding the new Logging name in the entire path
    self.customName = os.path.join("/", customName, name)

  def showHeaders(self, yesno=True):
    """
    Depending on the value, display or not the prefix of the message.
    :params yesno: boolean determining the behaviour of the display
    """
    self._setOption('headerIsShown', yesno)

  def showThreadIDs(self, yesno=True):
    """
    Depending on the value, display or not the thread ID.
    :params yesno: boolean determining the behaviour of the display
    """
    self._setOption('threadIDIsShown', yesno)

  def _setOption(self, optionName, value, directCall=True):
    """
    Depending on the value, modify the value of the option.
    Propagate the option to the children.
    The options of the children will be updated if they were not modified before by a developer
    :params optionName: string representing the name of the option to modify
    :params value: boolean to give to the option  
    :params directCall: boolean indicating if it is a call by the user or not
    """
    if self._optionsModified[optionName] and not directCall:
      return 

    if directCall:
      self._optionsModified[optionName] = True

    # update option  
    self._options[optionName] = value
    self._optionsModified[optionName] = True
    
    #propagate in the children
    for child in self._children.itervalues():
      child._setOption(optionName, value, directCall=False)
    # update the format to apply the option change
    self._generateBackendFormat()

  def registerBackends(self, desiredBackends, backendOptions=None):
    """
    Attach a list of backends to the Logging object.
    :params desiredBackends: a list of different names attaching to differents backends.
                             these names must be the same as in the _BACKENDSDICT
                             list of the possible values: ['stdout', 'stderr', 'file', 'server']
    :params backendOptions: a dictionary of different backend options. 
                            example: {'FileName': '/tmp/log.txt'}
    """
    for backendName in desiredBackends:
      backendName = backendName.strip().lower()

      # check if the name is correct
      if backendName in Logging._BACKENDSDICT:
        backend = Logging._BACKENDSDICT[backendName]()

        backend.createHandler(backendOptions)

        # update the level of the new backend to respect the Logging level
        backend.setLevel(self._level)
        self._logger.addHandler(backend.getHandler())
        self._backendsList.append(backend)
        self._generateBackendFormat()
      else:
        self._generateBackendFormat()
        self.warn("%s is not a valid backend name.", backendName)

  def setLevel(self, levelName):
    """
    Check if the level name exists and get the integer value before setting it.
    :params levelName: string representing the level to give to the logger
    :return: boolean representing if the setting is done or not
    """
    result = False
    if levelName.upper() in LogLevels.getLevelNames():
      self._setLevel(LogLevels.getLevelValue(levelName)) 
      result = True
    return result

  def _setLevel(self, level, directCall=True):
    """
    Set a level to the backends attached to this Logging.
    Set the level of the Logging too.
    Propagate the level to its children.
    :params level: integer representing the level to give to the logger
    :params directCall: boolean indicating if it is a call by the user or not
    """    
    # if the level logging level was previously modified by the developer
    # and it is not a direct call from him, then we return in order to stop the propagation
    if self._levelModified and not directCall: 
      return

    if directCall:
      self._levelModified = True

    # update Logging level
    self._level = level
    # update backend levels
    for backend in self._backendsList:
      backend.setLevel(self._level)

    # propagate in the children
    for child in self._children.itervalues():
      child._setLevel(level, directCall=False)  

  def getLevel(self):
    """
    :return: the name of the level
    """
    return LogLevels.getLevel(self._level)

  def shown(self, levelName):
    """
    Determine if messages with a certain level will be displayed or not.
    :params levelName: string representing the level to analyse
    :return: boolean which give the answer
    """
    result = False
    if levelName.upper() in LogLevels.getLevelNames():
      result = self._level <= LogLevels.getLevelValue(levelName)
    return result

  @classmethod
  def getName(cls):
    """
    :return: "system name/component name"
    """
    return cls._componentName

  def getSubName(self):
    """
    :return: the name of the logger
    """
    return self.name

  def getDisplayOptions(self):
    """
    :return: the dictionary of the display options and their values. Must not be redefined
    """
    return self._options    

  @staticmethod
  def getAllPossibleLevels():
    """
    :return: a list of all levels available
    """
    return LogLevels.getLevelNames()

  def always(self, sMsg, sVarMsg=''):
    """
    Always level
    """
    return self._createLogRecord(LogLevels.ALWAYS, sMsg, sVarMsg)

  def notice(self, sMsg, sVarMsg=''):
    """
    Notice level
    """
    return self._createLogRecord(LogLevels.NOTICE, sMsg, sVarMsg)

  def info(self, sMsg, sVarMsg=''):
    """
    Info level
    """
    return self._createLogRecord(LogLevels.INFO, sMsg, sVarMsg)

  def verbose(self, sMsg, sVarMsg=''):
    """
    Verbose level
    """
    return self._createLogRecord(LogLevels.VERBOSE, sMsg, sVarMsg)

  def debug(self, sMsg, sVarMsg=''):
    """
    Debug level
    """
    return self._createLogRecord(LogLevels.DEBUG, sMsg, sVarMsg)

  def warn(self, sMsg, sVarMsg=''):
    """
    Warn
    """
    return self._createLogRecord(LogLevels.WARN, sMsg, sVarMsg)

  def error(self, sMsg, sVarMsg=''):
    """
    Error level
    """
    return self._createLogRecord(LogLevels.ERROR, sMsg, sVarMsg)

  def exception(self, sMsg="", sVarMsg='', lException=False, lExcInfo=False):
    """
    Exception level
    """
    return self._createLogRecord(LogLevels.ERROR, sMsg, sVarMsg, exc_info=True)

  def fatal(self, sMsg, sVarMsg=''):
    """
    Critical level
    """
    return self._createLogRecord(LogLevels.FATAL, sMsg, sVarMsg)

  def _createLogRecord(self, level, sMsg, sVarMsg, exc_info=False):
    """
    Create a log record according to the level of the message. The log record is always sent to the different backends
    Backends have their own levels and can manage the display of the message or not according to the level. 
    Nevertheless, backends and the logger have the same level value, 
    so we can test if the message will be displayed or not. 
    :params level: positive integer representing the level of the log record
    :params sMsg: string representing the message
    :params sVarMsg: string representing an optional message
    :params exc_info: boolean representing the stacktrace for the exception

    :return: boolean representing the result of the log record creation
    """
    # exc_info is only for exception to add the stack trace
    # extra is a way to add extra attributes to the log record:
    # - 'componentname': the system/component name
    # - 'varmessage': the variable message
    # - 'customname' : the name of the logger for the DIRAC usage: without 'root' and separated with '/'
    # extras attributes are not camel case because log record attributes are not either.
    extra = {'componentname': self._componentName,
             'varmessage': sVarMsg,
             'customname': self.customName}
    self._logger.log(level, "%s", sMsg, exc_info=exc_info, extra=extra)
    # test to know if the message is displayed or not
    return self._level <= level

  def showStack(self):
    """
    Display a debug message without any content.
    :return: boolean, True if the message is sent, else False
    """
    return self.debug('')

  def _generateBackendFormat(self):
    """
    Generate the Backends format according to the options
    """
    # give options and level to AbstractBackend to receive the new format for the backends list
    datefmt, fmt = AbstractBackend.createFormat(self._options)

    for backend in self._backendsList:
      backend.setFormat(fmt, datefmt, self._options)

  def getSubLogger(self, subName, child=True):
    """
    Create a new Logging object, child of this Logging, if it does not exists.
    :params subName: the name of the child Logging
    """
    #  Check if the object has a child with "subName".
    result = self._children.get(subName)
    if result is not None:
      return result
    # create a new child Logging
    childLogging = Logging(self, self._logger.name, subName, self.customName)
    self._children[subName] = childLogging
    return childLogging

  def initialized(self):
    """
    initialized: Deleted method. Do not use it.
    """
    return True

  def processMessage(self, messageObject):
    """
    processMessage: Deleted method. Do not use it.
    """
    return False

  def flushAllMessages(self, exitCode=0):
    """
    flushAllMessages: Deleted method. Do not use it.
    """
    pass
