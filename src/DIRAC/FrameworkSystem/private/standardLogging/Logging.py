"""
Logging
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import logging
import os

from DIRAC import S_ERROR
from DIRAC.FrameworkSystem.private.standardLogging.LogLevels import LogLevels
from DIRAC.Core.Utilities.LockRing import LockRing


class Logging(object):
  """
  - Logging is a wrapper of the logger object from the standard "logging" library which integrates some DIRAC concepts.
  - It aimed at seamlessly replacing the previous gLogger implementation and thus provides the same interface.
  - Logging is generally used to create log records, that are then sent to pre-determined backends.

  Each Logging embeds a logger of "logging". Logging can instanciate "children" logging objects and
  all Logging objects inherit from the configuration of LoggingRoot, the first Logging object to be instanciated.
  """

  # componentName is a class variable: the component name is the same for every Logging objects
  # its default value is "Framework" but it can be configured in initialize() in LoggingRoot
  # it can be composed by the system name and the component name. For instance: "Monitoring/Atom"
  _componentName = "Framework"
  # use the lockRing singleton to save the Logging object
  _lockRing = LockRing()
  # lock the configuration of the Logging
  _lockConfig = _lockRing.getLock("config")

  def __init__(self, father=None, fatherName='', name='', customName=''):
    """
    Initialization of the Logging object. By default, 'fatherName' and 'name' are empty,
    because getChild only accepts string and the first empty string corresponds to the root logger.
    Example:
    >>> logging.getLogger('') == logging.getLogger('root') # root logger
    >>> logging.getLogger('root').getChild('log') == logging.getLogger('log') # log child of root

    :param Logging father: father of this new Logging.
    :param str fatherName: name of the father logger in the chain.
    :param str name: name of the logger in the chain.
    :param str customName: name of the logger in the chain:
                            - "root" does not appear at the beginning of the chain
                            - hierarchy "." are replaced by "\"
    """

    # Logging chain
    self._children = {}
    self._parent = father

    # initialize display options and level with the ones of the Logging parent
    if self._parent is not None:
      self._options = self._parent.getDisplayOptions()
    else:
      self._options = {'headerIsShown': True,
                       'timeStampIsShown': True,
                       'contextIsShown': True,
                       'threadIDIsShown': False,
                       'color': False}

    # dictionary of the options modifications: give the same behaviour that the "logging" level
    # - propagation from the parent to the children when their levels are not set by the developer
    # - stop the propagation when a developer set a level to a child
    self._optionsModified = {'headerIsShown': False,
                             'timeStampIsShown': False,
                             'contextIsShown': False,
                             'threadIDIsShown': False}

    self._backendsList = []

    # name of the Logging
    self.name = str(name)
    self._logger = logging.getLogger(fatherName).getChild(self.name)

    # update the custom name of the Logging adding the new Logging name in the entire path
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

    :param bool yesno: determine the log record format
    """
    self._setOption('headerIsShown', yesno)

  def showThreadIDs(self, yesno=True):
    """
    Depending on the value, display or not the thread ID.
    Make sure to enable the headers: showHeaders(True) before

    :param bool yesno: determe the log record format
    """
    self._setOption('threadIDIsShown', yesno)

  def showTimeStamps(self, yesno=True):
    """
    Depending on the value, display or not the timestamp of the message.
    Make sure to enable the headers: showHeaders(True) before

    :param bool yesno: determine the log record format
    """
    self._setOption('timeStampIsShown', yesno)

  def showContexts(self, yesno=True):
    """
    Depending on the value, display or not the context of the message.
    Make sure to enable the headers: showHeaders(True) before

    :param bool yesno: determine the log record format
    """
    self._setOption('contextIsShown', yesno)

  def _setOption(self, optionName, value, directCall=True):
    """
    Depending on the value, modify the value of the option and propagate the option to the children.
    The options of the children will be updated if they were not modified before by a developer.

    :param str optionName: name of the option to modify
    :param bool value: value of the option to set
    :param bool directCall: indicate whether the call is performed by a developer
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
      for child in self._children.values():
        child._setOption(optionName, value, directCall=False)  # pylint: disable=protected-access
    finally:
      self._lockOptions.release()

  def registerBackends(self, desiredBackends, backendOptions=None):
    """
    Attach a list of backends to the Logging object.
    Convert backend names to backend class names to Backend objects and add them to the Logging object

    :param desiredBackends: list of different names attaching to differents backends.
                             list of the possible values: ['stdout', 'stderr', 'file']
    :param backendOptions: dictionary of different backend options. Example: FileName='/tmp/log.txt'
    """
    for backendName in desiredBackends:
      self.registerBackend(backendName, backendOptions)

  def registerBackend(self, desiredBackend, backendOptions=None):
    """
    Attach a backend to the Logging object.
    Convert backend name to backend class name to a Backend object and add it to the Logging object

    :param desiredBackend: a name attaching to a backend type. List of possible values: ['stdout', 'stderr', 'file']
    :param backendOptions: dictionary of different backend options. Example: FileName='/tmp/log.txt'
    """
    # Remove white space and capitalize the first letter
    desiredBackend = desiredBackend.strip()
    desiredBackend = desiredBackend[0].upper() + desiredBackend[1:]
    _class = self.__loadLogClass('Resources.LogBackends.%sBackend' % desiredBackend)
    if _class['OK']:
      # add the backend instance to the Logging
      self._addBackend(_class['Value'], backendOptions)
    else:
      self.warn("%s is not a valid backend name." % desiredBackend)

  def _addBackend(self, backendType, backendOptions=None):
    """
    Attach a Backend object to the Logging object.

    :param Backend backend: Backend object that has to be added
    :param backendOptions: a dictionary of different backend options. Example: {'FileName': '/tmp/log.txt'}
    """
    # lock to prevent that the level change before adding the new backend in the backendsList
    # and to prevent a change of the backendsList during the reading of the
    # list
    self._lockLevel.acquire()
    self._lockOptions.acquire()
    try:
      backend = backendType(backendOptions)
      self._logger.addHandler(backend.getHandler())
      self._addFilter(backend, backendOptions)
      self._backendsList.append(backend)
    finally:
      self._lockLevel.release()
      self._lockOptions.release()

  def _addFilter(self, backend, backendOptions):
    """
    Create a filter and add it to the handler of the backend.
    """
    for filterName in self.__getFilterList(backendOptions):
      options = self.__getFilterOptionsFromCFG(filterName)
      _class = self.__loadLogClass('Resources.LogFilters.%s' % options.get('Plugin'))
      if _class['OK']:
        # add the backend instance to the Logging
        backend.getHandler().addFilter(_class['Value'](options))
      else:
        self.warn("%r is not a valid Filter name." % filterName)

  def __getFilterList(self, backendOptions):
    """
    Return list of defined filters.
    """
    if not (isinstance(backendOptions, dict) and 'Filter' in backendOptions):
      return []
    return [fil.strip() for fil in backendOptions['Filter'].split(',') if fil.strip()]

  def __getFilterOptionsFromCFG(self, logFilter):
    """Get filter options from the configuration..

    :param logFilter: filter identifier: stdout, file, f04
    """
    # We have to put the import lines here to avoid a dependancy loop
    from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getFilterConfig

    # Search filters config in the resources section
    retDictRessources = getFilterConfig(logFilter)
    if retDictRessources['OK']:
      return retDictRessources['Value']
    return {}

  def setLevel(self, levelName):
    """
    Check if the level name exists and set it.

    :param levelName: string representing the level to give to the logger
    :return: boolean representing if the setting is done or not
    """
    result = False
    if levelName.upper() in LogLevels.getLevelNames():
      self._logger.setLevel(LogLevels.getLevelValue(levelName))
      result = True
    return result

  def getLevel(self):
    """
    :return: the name of the level
    """
    return LogLevels.getLevel(self._logger.getEffectiveLevel())

  def shown(self, levelName):
    """
    Determine whether messages with a certain level will be displayed.

    :param levelName: string representing the level to analyse

    :return: boolean which give the answer
    """
    # lock to prevent a level change
    self._lockLevel.acquire()
    try:
      result = False
      if levelName.upper() in LogLevels.getLevelNames():
        result = LogLevels.getLevelValue(self.getLevel()) <= LogLevels.getLevelValue(levelName)
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
    Create a log record according to the level of the message.

    - The log record is always sent to the different backends
    - Backends have their own levels and may manage the display of the log record

    :param int level: level of the log record
    :param str sMsg: message
    :param str sVarMsg: additional message
    :param bool exc_info: indicates whether the stacktrace has to appear in the log record

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
      # as log records, extras attributes are not camel case
      extra = {'componentname': self._componentName,
               'varmessage': str(sVarMsg),
               'spacer': '' if not sVarMsg else ' ',
               'customname': self._customName}

      # options such as headers and threadIDs also depend on the logger, we have to add them to extra
      extra.update(self._options)

      self._logger.log(level, "%s", sMsg, exc_info=exc_info, extra=extra)
      # check whether the message is displayed
      isSent = LogLevels.getLevelValue(self.getLevel()) <= level
      return isSent
    finally:
      self._lockLevel.release()

  def showStack(self):
    """
    Display a debug message without any content.

    :return: boolean, True if the message is sent, else False
    """
    return self.debug('')

  def getSubLogger(self, subName, child=True):
    """
    Create a new Logging object, child of this Logging, if it does not exists.

    :param str subName: name of the child Logging
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
      childLogging = Logging(self, self._logger.name, subName, self._customName)
      self._children[subName] = childLogging
      return childLogging
    finally:
      self._lockInit.release()
