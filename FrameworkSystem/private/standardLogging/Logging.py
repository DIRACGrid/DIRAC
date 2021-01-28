"""
Logging
"""

__RCSID__ = "$Id$"

import logging
import os

from DIRAC import S_ERROR
from DIRAC.FrameworkSystem.private.standardLogging.LogLevels import LogLevels
from DIRAC.Core.Utilities.Decorators import deprecated
from DIRAC.Core.Utilities.LockRing import LockRing
from DIRAC.Resources.LogBackends.AbstractBackend import AbstractBackend


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
  # it can be composed by the system name and the component name. For
  # instance: "Monitoring/Atom"
  _componentName = "Framework"
  # use the lockRing singleton to save the Logging object
  _lockRing = LockRing()
  # lock the configuration of the Logging
  _lockConfig = _lockRing.getLock("config")

  def __init__(self, father=None, fatherName='', name='', customName=''):
    """
    Initialization of the Logging object.
    By default, 'fatherName' and 'name' are empty, because getChild accepts only string and the first empty
    string corresponds to the root logger.
    Example:
    logging.getLogger('') == logging.getLogger('root') == root logger
    logging.getLogger('root').getChild('log') == root.log == log child of root

    :params father: Logging, father of this new Logging.
    :params fatherName: string representing the name of the father logger in the chain.
    :params name: string representing the name of the logger in the chain.
    :params customName: string representing the name of the logger in the chain:
                        - "root" does not appear at the beginning of the chain
                        - hierarchy "." are replaced by "\"
                        useful for the display of the Logging name
    """

    # Logging chain
    self._children = {}
    self._parent = father

    # initialize display options and level with the ones of the Logging parent
    if self._parent is not None:
      self._options = self._parent.getDisplayOptions()
      self._level = LogLevels.getLevelValue(father.getLevel())
    else:
      self._options = {'headerIsShown': True,
                       'threadIDIsShown': False, 'Color': False}
      # the native level is not used because it has to be to debug to send all
      # messages to the log central
      self._level = None

    # dictionary of the option state, modified by the user or not
    # this is to give to the options the same behaviour that the "logging" level:
    # - propagation from the parent to the children when their levels are not set by the developer himself
    # - stop the propagation when a developer set a level to a child
    self._optionsModified = {'headerIsShown': False, 'threadIDIsShown': False}
    self._levelModified = False

    self._backendsList = []

    # name of the Logging
    self.name = str(name)
    self._logger = logging.getLogger(fatherName).getChild(self.name)
    # update the custom name of the Logging adding the new Logging name in the
    # entire path
    self._customName = os.path.join("/", customName, self.name)

    # Locks to make Logging thread-safe
    # we use RLock to prevent blocking in the Logging
    # lockInit to protect the initialization of a sublogger
    self._lockInit = self._lockRing.getLock("init")
    # lockOptions to protect the option modifications and the backendsList
    self._lockOptions = self._lockRing.getLock("options", recursive=True)
    # lockLevel to protect the level
    self._lockLevel = self._lockRing.getLock("level", recursive=True)
    # lockObjectLoader to protect the ObjectLoader singleton
    self._lockObjectLoader = self._lockRing.getLock("objectLoader")

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
    The options of the children will be updated if they were not modified before by a developer.

    :params optionName: string representing the name of the option to modify
    :params value: boolean to give to the option
    :params directCall: boolean indicating if it is a call by the user or not
    """
    # lock to prevent that two threads change the options at the same time
    self._lockOptions.acquire()
    try:
      if self._optionsModified[optionName] and not directCall:
        return

      if directCall:
        self._optionsModified[optionName] = True

      # update option
      self._options[optionName] = value

      # propagate in the children
      for child in self._children.itervalues():
        child._setOption(optionName, value, directCall=False)  # pylint: disable=protected-access
      # update the format to apply the option change
      self._generateBackendFormat()
    finally:
      self._lockOptions.release()

  def registerBackends(self, desiredBackends, backendOptions=None):
    """
    Attach a list of backends to the Logging object.
    Convert backend name to backend class name to a Backend object and add it to the Logging object

    :params desiredBackends: a list of different names attaching to differents backends.
                             list of the possible values: ['stdout', 'stderr', 'file', 'server']
    :params backendOptions: dictionary of different backend options.
                            example: FileName='/tmp/log.txt'
    """
    for backendName in desiredBackends:
      self.registerBackend(backendName, backendOptions)

  def registerBackend(self, desiredBackend, backendOptions=None):
    """
    Attach a backend to the Logging object.
    Convert backend name to backend class name to a Backend object and add it to the Logging object

    :params desiredBackend: a name attaching to a backend type.
                            list of the possible values: ['stdout', 'stderr', 'file', 'server']
    :params backendOptions: dictionary of different backend options.
                            example: FileName='/tmp/log.txt'
    """
    # Remove white space and capitalize the first letter
    desiredBackend = desiredBackend.strip()
    desiredBackend = desiredBackend[0].upper() + desiredBackend[1:]
    _class = self.__loadLogClass('Resources.LogBackends.%sBackend' % desiredBackend)
    if _class['OK']:
      # add the backend instance to the Logging
      self._addBackend(_class['Value'](), backendOptions)
      self._generateBackendFormat()
    else:
      self._generateBackendFormat()
      self.warn("%s is not a valid backend name." % desiredBackend)

  def _addBackend(self, backend, backendOptions=None):
    """
    Attach a Backend object to the Logging object.

    :params backend: Backend object that has to be added
    :params backendOptions: a dictionary of different backend options.
                            example: {'FileName': '/tmp/log.txt'}
    """
    backend.createHandler(backendOptions)

    # lock to prevent that the level change before adding the new backend in the backendsList
    # and to prevent a change of the backendsList during the reading of the
    # list
    self._lockLevel.acquire()
    self._lockOptions.acquire()
    try:
      # update the level of the new backend to respect the Logging level
      backend.setLevel(self._level)
      self._logger.addHandler(backend.getHandler())
      self._addFilter(backend, backendOptions)
      self._backendsList.append(backend)
    finally:
      self._lockLevel.release()
      self._lockOptions.release()

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
    # lock to prevent that two threads change the level at the same time
    self._lockLevel.acquire()
    try:
      # if the level logging level was previously modified by the developer
      # and it is not a direct call from him, then we return in order to stop
      # the propagation
      if self._levelModified and not directCall:
        return

      if directCall:
        self._levelModified = True

      # update Logging level
      self._level = level

      # lock to prevent a modification of the backendsList
      self._lockOptions.acquire()
      try:
        # update backend levels
        for backend in self._backendsList:
          backend.setLevel(self._level)
      finally:
        self._lockOptions.release()

      # propagate in the children
      for child in self._children.itervalues():
        child._setLevel(level, directCall=False)  # pylint: disable=protected-access
    finally:
      self._lockLevel.release()

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
    # lock to prevent a level change
    self._lockLevel.acquire()
    try:
      result = False
      if levelName.upper() in LogLevels.getLevelNames():
        result = self._level <= LogLevels.getLevelValue(levelName)
      return result
    finally:
      self._lockLevel.release()

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
    # lock to save the options which can be modified
    self._lockOptions.acquire()
    try:
      # copy the dictionary to avoid that every Logging has the same
      options = self._options.copy()
      return options
    finally:
      self._lockOptions.release()

  def __loadLogClass(self, modulePath):
    """Load class thread-safe."""
    # import ObjectLoader here to avoid a dependancy loop
    from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
    objLoader = ObjectLoader()
    # lock to avoid problem in ObjectLoader which is a singleton not
    # thread-safe
    self._lockObjectLoader.acquire()
    try:
      # load the Backend class
      return objLoader.loadObject(modulePath)
    finally:
      self._lockObjectLoader.release()
    return S_ERROR()

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
    _ = lException  # Make pylint happy
    _ = lExcInfo
    return self._createLogRecord(LogLevels.ERROR, sMsg, sVarMsg, exc_info=True)

  def fatal(self, sMsg, sVarMsg=''):
    """
    Fatal level
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

    # lock to prevent a level change after that the log is sent.
    self._lockLevel.acquire()
    try:
      # exc_info is only for exception to add the stack trace
      # extra is a way to add extra attributes to the log record:
      # - 'componentname': the system/component name
      # - 'varmessage': the variable message
      # - 'customname' : the name of the logger for the DIRAC usage: without 'root' and separated with '/'
      # extras attributes are not camel case because log record attributes are
      # not either.
      extra = {'componentname': self._componentName,
               'varmessage': str(sVarMsg),
               'spacer': '' if not sVarMsg else ' ',
               'customname': self._customName}
      self._logger.log(level, "%s", sMsg, exc_info=exc_info, extra=extra)
      # test to know if the message is displayed or not
      isSent = self._level <= level
      return isSent
    finally:
      self._lockLevel.release()

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
    # lock to prevent the modification of the options during this code block
    # and to prevent a modification of the backendsList
    self._lockOptions.acquire()
    try:
      # give options and level to AbstractBackend to receive the new format for
      # the backends list
      datefmt, fmt = AbstractBackend.createFormat(self._options)

      for backend in self._backendsList:
        backend.setFormat(fmt, datefmt, self._options)
    finally:
      self._lockOptions.release()

  def _addFilter(self, backend, backendOptions):
    """Create a filter and add it to the handler of the backend."""
    for filterName in self.__getFilterList(backendOptions):
      options = self.__getFilterOptionsFromCFG(filterName)
      _class = self.__loadLogClass('Resources.LogFilters.%s' % options.get('Plugin'))
      if _class['OK']:
        # add the backend instance to the Logging
        backend.getHandler().addFilter(_class['Value'](options))
      else:
        self.warn("%r is not a valid Filter name." % filterName)

  def __getFilterList(self, backendOptions):
    """Return list of defined filters."""
    if not (isinstance(backendOptions, dict) and 'Filter' in backendOptions):
      return []
    return [fil.strip() for fil in backendOptions['Filter'].split(',') if fil.strip()]

  def __getFilterOptionsFromCFG(self, logFilter):
    """Get filter options from the configuration..

    :params logFilter: string representing a filter identifier: stdout, file, f04
    """
    # We have to put the import lines here to avoid a dependancy loop
    from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getFilterConfig

    # Search filters config in the resources section
    retDictRessources = getFilterConfig(logFilter)
    if retDictRessources['OK']:
      return retDictRessources['Value']
    return {}

  def getSubLogger(self, subName, child=True):
    """
    Create a new Logging object, child of this Logging, if it does not exists.

    :params subName: the name of the child Logging
    """
    _ = child  # make pylint happy
    # lock to prevent that the method initializes two Logging for the same 'logging' logger
    # and to erase the existing _children[subName]
    self._lockInit.acquire()
    try:
      # Check if the object has a child with "subName".
      result = self._children.get(subName)
      if result is not None:
        return result
      # create a new child Logging
      childLogging = Logging(self, self._logger.name,
                             subName, self._customName)
      self._children[subName] = childLogging
      return childLogging
    finally:
      self._lockInit.release()

  @deprecated("No longer does anything", onlyOnce=True)
  def initialized(self):  # pylint: disable=no-self-use
    """
    initialized: Deleted method. Do not use it.
    """
    return True

  @deprecated("No longer does anything", onlyOnce=True)
  def processMessage(self, messageObject):  # pylint: disable=no-self-use
    """
    processMessage: Deleted method. Do not use it.
    """
    _ = messageObject  # make pylint happy
    return False

  @deprecated("No longer does anything", onlyOnce=True)
  def flushAllMessages(self, exitCode=0):
    """
    flushAllMessages: Deleted method. Do not use it.
    """
    pass
