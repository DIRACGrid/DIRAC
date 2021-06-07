""" Basic functions for interacting with CS objects
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


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
    """ C'or

        :param list fileToLoadList: files to load
    """
    self.diracConfigFilePath = os.path.join(DIRAC.rootPath, "etc", "dirac.cfg")
    if fileToLoadList and isinstance(fileToLoadList, list):
      for fileName in fileToLoadList:
        gConfigurationData.loadFile(fileName)

  def loadFile(self, fileName):
    """ Load file

        :param str fileName: file name

        :return: S_OK()/S_ERROR()
    """
    return gConfigurationData.loadFile(fileName)

  def loadCFG(self, cfg):
    """ Load CFG

       :param CFG() cfg: CFG object

       :return: S_OK()/S_ERROR()
    """
    return gConfigurationData.mergeWithLocal(cfg)

  def forceRefresh(self, fromMaster=False):
    """ Force refresh

        :param bool fromMaster: refresh from master

        :return: S_OK()/S_ERROR()
    """
    return gRefresher.forceRefresh(fromMaster=fromMaster)

  def dumpLocalCFGToFile(self, fileName):
    """ Dump local configuration to file

        :param str fileName: file name

        :return: S_OK()/S_ERROR()
    """
    return gConfigurationData.dumpLocalCFGToFile(fileName)

  def dumpRemoteCFGToFile(self, fileName):
    """ Dump remote configuration to file

        :param str fileName: file name

        :return: S_OK()/S_ERROR()
    """
    return gConfigurationData.dumpRemoteCFGToFile(fileName)

  def addListenerToNewVersionEvent(self, functor):
    """ Add listener to new version event

        :param str functor: functor
    """
    gRefresher.addListenerToNewVersionEvent(functor)

  def dumpCFGAsLocalCache(self, fileName=None, raw=False):
    """ Dump local CFG cache to file

        :param str fileName: file name
        :param bool raw: raw

        :return: S_OK(str)/S_ERROR()
    """
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
    """ Get list of servers

        :return: list
    """
    return gConfigurationData.getServers()

  def useServerCertificate(self):
    """ Get using server certificate status

        :return: bool
    """
    return gConfigurationData.useServerCertificate()

  def getValue(self, optionPath, defaultValue=None):
    """ Get configuration value

        :param str optionPath: option path
        :param defaultValue: default value

        :return: type(defaultValue)
    """
    retVal = self.getOption(optionPath, defaultValue)
    return retVal['Value'] if retVal['OK'] else defaultValue

  def getOption(self, optionPath, typeValue=None):
    """ Get configuration option

        :param str optionPath: option path
        :param typeValue: type of value

        :return: S_OK()/S_ERROR()
    """
    gRefresher.refreshConfigurationIfNeeded()
    optionValue = gConfigurationData.extractOptionFromCFG(optionPath)

    if optionValue is None:
      return S_ERROR(
          "Path %s does not exist or it's not an option" % optionPath,
          callStack=["ConfigurationClient.getOption"],
      )

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
    """ Get configuration sections

        :param str sectionPath: section path
        :param bool listOrdered: ordered

        :return: S_OK(list)/S_ERROR()
    """
    gRefresher.refreshConfigurationIfNeeded()
    sectionList = gConfigurationData.getSectionsFromCFG(sectionPath, ordered=listOrdered)
    if isinstance(sectionList, list):
      return S_OK(sectionList)
    else:
      return S_ERROR("Path %s does not exist or it's not a section" % sectionPath)

  def getOptions(self, sectionPath, listOrdered=True):
    """ Get configuration options

        :param str sectionPath: section path
        :param bool listOrdered: ordered

        :return: S_OK(list)/S_ERROR()
    """
    gRefresher.refreshConfigurationIfNeeded()
    optionList = gConfigurationData.getOptionsFromCFG(sectionPath, ordered=listOrdered)
    if isinstance(optionList, list):
      return S_OK(optionList)
    else:
      return S_ERROR("Path %s does not exist or it's not a section" % sectionPath)

  def getOptionsDict(self, sectionPath):
    """ Get configuration options in dictionary

        :param str sectionPath: section path

        :return: S_OK(dict)/S_ERROR()
    """
    gRefresher.refreshConfigurationIfNeeded()
    optionsDict = {}
    optionList = gConfigurationData.getOptionsFromCFG(sectionPath)
    if isinstance(optionList, list):
      for option in optionList:
        optionsDict[option] = gConfigurationData.extractOptionFromCFG("%s/%s" % (sectionPath, option))
      return S_OK(optionsDict)
    else:
      return S_ERROR("Path %s does not exist or it's not a section" % sectionPath)

  def getOptionsDictRecursively(self, sectionPath):
    """ Get configuration options in dictionary recursively

        :param str sectionPath: section path

        :return: S_OK(dict)/S_ERROR()
    """
    if not gConfigurationData.mergedCFG.isSection(sectionPath):
      return S_ERROR("Path %s does not exist or it's not a section" % sectionPath)
    return S_OK(gConfigurationData.mergedCFG.getAsDict(sectionPath))

  def getConfigurationTree(self, root='', *filters):
    """ Create a dictionary with all sections, subsections and options
        starting from given root. Result can be filtered.

        :param str root: Starting point in the configuration tree.
        :param filters: Select results that contain given substrings (check full path, i.e. with option name)
        :type filters: str or python:list[str]

        :return: S_OK(dict)/S_ERROR() -- dictionary where keys are paths taken from
                 the configuration (e.g. /Systems/Configuration/...).
                 Value is "None" when path points to a section
                 or not "None" if path points to an option.
    """

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
        return S_ERROR("getOptionsDict() failed with message: %s" % options['Message'])

      for key, value in options['Value'].items():
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
        return S_ERROR("getSections() failed with message: %s" % sections['Message'])

      # recursively go through subsections and get their subsections
      for section in sections['Value']:
        subtree = self.getConfigurationTree("%s/%s" % (root, section), *filters)
        if not subtree['OK']:
          return S_ERROR("getConfigurationTree() failed with message: %s" % sections['Message'])
        result.update(subtree['Value'])

    return S_OK(result)

  def setOptionValue(self, optionPath, value):
    """ Set a value in the local configuration

        :param str optionPath: option path
        :param str value: value
    """
    gConfigurationData.setOptionInCFG(optionPath, value)
