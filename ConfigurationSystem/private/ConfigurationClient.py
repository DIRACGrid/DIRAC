""" Basic functions for interacting with CS objects
"""


import os

import DIRAC
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
from DIRAC.Core.Utilities import List
from DIRAC.ConfigurationSystem.Client.Helpers.Path import cfgPath
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
from DIRAC.ConfigurationSystem.private.Refresher import gRefresher

__RCSID__ = "$Id$"


class ConfigurationClient(object):

  def __init__(self, fileToLoadList=None):
    self.diracConfigFilePath = os.path.join(DIRAC.rootPath, "etc", "dirac.cfg")
    if fileToLoadList and isinstance(fileToLoadList, list):
      for fileName in fileToLoadList:
        gConfigurationData.loadFile(fileName)

  def loadFile(self, fileName):
    return gConfigurationData.loadFile(fileName)

  def loadCFG(self, cfg):
    return gConfigurationData.mergeWithLocal(cfg)

  def forceRefresh(self, fromMaster=False):
    return gRefresher.forceRefresh(fromMaster=fromMaster)

  def dumpLocalCFGToFile(self, fileName):
    return gConfigurationData.dumpLocalCFGToFile(fileName)

  def dumpRemoteCFGToFile(self, fileName):
    return gConfigurationData.dumpRemoteCFGToFile(fileName)

  def addListenerToNewVersionEvent(self, functor):
    gRefresher.addListenerToNewVersionEvent(functor)

  def dumpCFGAsLocalCache(self, fileName=None, raw=False):
    cfg = gConfigurationData.mergedCFG.clone()
    try:
      if not raw and cfg.isSection('DIRAC'):
        diracSec = cfg['DIRAC']
        if diracSec.isSection('Configuration'):  # pylint: disable=no-member
          confSec = diracSec['Configuration']  # pylint: disable=unsubscriptable-object
          for opt in ('Servers', 'MasterServer'):
            if confSec.isOption(opt):
              confSec.deleteKey(opt)
      strData = str(cfg)
      if fileName:
        with open(fileName, "w") as fd:
          fd.write(strData)
    except Exception as e:
      return S_ERROR("Can't write to file %s: %s" % (fileName, str(e)))
    return S_OK(strData)

  def getServersList(self):
    return gConfigurationData.getServers()

  def useServerCertificate(self):
    return gConfigurationData.useServerCertificate()

  def getValue(self, optionPath, defaultValue=None):
    retVal = self.getOption(optionPath, defaultValue)
    return retVal['Value'] if retVal['OK'] else defaultValue

  def getOption(self, optionPath, typeValue=None):
    gRefresher.refreshConfigurationIfNeeded()
    optionValue = gConfigurationData.extractOptionFromCFG(optionPath)

    if optionValue is None:
      return S_ERROR("Path %s does not exist or it's not an option" % optionPath)

    # Value has been returned from the configuration
    if typeValue is None:
      return S_OK(optionValue)

    # Casting to typeValue's type
    if not isinstance(typeValue, type):
      # typeValue is not a type but a default object
      requestedType = type(typeValue)
    else:
      requestedType = typeValue

    if requestedType in (list, tuple, set):
      try:
        return S_OK(requestedType(List.fromChar(optionValue, ',')))
      except Exception as e:
        return S_ERROR("Can't convert value (%s) to comma separated list \n%s" % (str(optionValue),
                                                                                  repr(e)))
    elif requestedType == bool:
      try:
        return S_OK(optionValue.lower() in ("y", "yes", "true", "1"))
      except Exception as e:
        return S_ERROR("Can't convert value (%s) to Boolean \n%s" % (str(optionValue),
                                                                     repr(e)))
    elif requestedType == dict:
      try:
        splitOption = List.fromChar(optionValue, ',')
        value = {}
        for opt in splitOption:
          keyVal = [x.strip() for x in opt.split(':')]
          if len(keyVal) == 1:
            keyVal.append(True)
          value[keyVal[0]] = keyVal[1]
        return S_OK(value)
      except Exception as e:
        return S_ERROR("Can't convert value (%s) to Dict \n%s" % (str(optionValue),
                                                                  repr(e)))
    else:
      try:
        return S_OK(requestedType(optionValue))
      except Exception as e:
        return S_ERROR(
            "Type mismatch between default (%s) and configured value (%s) \n%s" %
            (str(typeValue), optionValue, repr(e)))

  def getSections(self, sectionPath, listOrdered=True):
    gRefresher.refreshConfigurationIfNeeded()
    sectionList = gConfigurationData.getSectionsFromCFG(sectionPath, ordered=listOrdered)
    if isinstance(sectionList, list):
      return S_OK(sectionList)
    else:
      return S_ERROR("Path %s does not exist or it's not a section" % sectionPath)

  def getOptions(self, sectionPath, listOrdered=True):
    gRefresher.refreshConfigurationIfNeeded()
    optionList = gConfigurationData.getOptionsFromCFG(sectionPath, ordered=listOrdered)
    if isinstance(optionList, list):
      return S_OK(optionList)
    else:
      return S_ERROR("Path %s does not exist or it's not a section" % sectionPath)

  def getOptionsDict(self, sectionPath):
    gRefresher.refreshConfigurationIfNeeded()
    optionsDict = {}
    optionList = gConfigurationData.getOptionsFromCFG(sectionPath)
    if isinstance(optionList, list):
      for option in optionList:
        optionsDict[option] = gConfigurationData.extractOptionFromCFG("%s/%s" % (sectionPath, option))
      return S_OK(optionsDict)
    else:
      return S_ERROR("Path %s does not exist or it's not a section" % sectionPath)

  def getConfigurationTree(self, root='', *filters):
    """
    Create a dictionary with all sections, subsections and options
    starting from given root. Result can be filtered.

    :param str root: Starting point in the configuration tree.
    :param filters: Select results that contain given substrings (check full path, i.e. with option name)
    :type filters: str or python:list[str]
    :return: Return a dictionary where keys are paths taken from
             the configuration (e.g. /Systems/Configuration/...).
             Value is "None" when path points to a section
             or not "None" if path points to an option.
    """

    log = DIRAC.gLogger.getSubLogger('getConfigurationTree')

    # check if root is an option (special case)
    option = self.getOption(root)
    if option['OK']:
      result = {root: option['Value']}

    else:
      result = {root: None}
      for substr in filters:
        if substr not in root:
          result = {}
          break

      # remove slashes at the end
      root = root.rstrip('/')

      # get options of current root
      options = self.getOptionsDict(root)
      if not options['OK']:
        log.error("getOptionsDict() failed with message: %s" % options['Message'])
        return S_ERROR('Invalid root path provided')

      for key, value in options['Value'].iteritems():
        path = cfgPath(root, key)
        addOption = True
        for substr in filters:
          if substr not in path:
            addOption = False
            break

        if addOption:
          result[path] = value

      # get subsections of the root
      sections = self.getSections(root)
      if not sections['OK']:
        log.error("getSections() failed with message: %s" % sections['Message'])
        return S_ERROR('Invalid root path provided')

      # recursively go through subsections and get their subsections
      for section in sections['Value']:
        subtree = self.getConfigurationTree("%s/%s" % (root, section), *filters)
        if not subtree['OK']:
          log.error("getConfigurationTree() failed with message: %s" % sections['Message'])
          return S_ERROR('Configuration was altered during the operation')
        result.update(subtree['Value'])

    return S_OK(result)

  def setOptionValue(self, optionPath, value):
    """
    Set a value in the local configuration
    """
    gConfigurationData.setOptionInCFG(optionPath, value)
