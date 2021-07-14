""" This helper looks in the /Operations section of the CS, considering its specific nature:
    the /Operations section is designed in a way that each configuration can be specific to a Setup,
    while maintaining a default.

    So, for example, given the following /Operations section::

      Operations/
          Defaults/
              someSection/
                  someOption = someValue
                  aSecondOption = aSecondValue
          Production/
              someSection/
                  someOption = someValueInProduction
                  aSecondOption = aSecondValueInProduction
          Certification/
              someSection/
                  someOption = someValueInCertification

    The following calls would give different results based on the setup::

      Operations().getValue('someSection/someOption')
        - someValueInProduction if we are in 'Production' setup
        - someValueInCertification if we are in 'Certification' setup

      Operations().getValue('someSection/aSecondOption')
        - aSecondValueInProduction if we are in 'Production' setup
        - aSecondValue if we are in 'Certification' setup    <- looking in Defaults
                                                                since there's no Certification/someSection/aSecondOption


    At the same time, for multi-VO installations, it is also possible to specify different options per-VO,
    like the following::

      Operations/
          aVOName/
              Defaults/
                  someSection/
                      someOption = someValue
                      aSecondOption = aSecondValue
              Production/
                  someSection/
                      someOption = someValueInProduction
                      aSecondOption = aSecondValueInProduction
              Certification/
                  someSection/
                      someOption = someValueInCertification
          anotherVOName/
              Defaults/
                  someSectionName/
                      someOptionX = someValueX
                      aSecondOption = aSecondValue
              setupName/
                  someSection/
                      someOption = someValueInProduction
                      aSecondOption = aSecondValueInProduction

    For this case it becomes then important for the Operations() objects to know the VO name
    for which we want the information, and this can be done in the following ways.

    1. by specifying the VO name directly::

         Operations(vo=anotherVOName).getValue('someSectionName/someOptionX')

    2. by give a group name::

         Operations(group=thisIsAGroupOfVO_X).getValue('someSectionName/someOptionX')

    3. if no VO nor group is provided, the VO will be guessed from the proxy,
    but this works if the object is instantiated by a proxy (and not, e.g., using a server certificate)

    You can also specify the main section::

      Operations(vo=myVO, mainSection='/mainSection').getValue('mySection/myOption')

    in this case, the configuration will be searched in the following sections with the name 'mainSection'::

      DIRAC/
          Setup = Production
      mainSection/
          anotherOption = anotherValue
          mySection/
              myOption = myValue
      Operations/
          Defaults/                       -- this section will not be taken into account
              ...
          Production/
              mainSection/
                  anotherOption = anotherValueInProduction
                  mySection/
                      myOption = myValueInProduction
          myVO/
              Defaults/
                  mainSection/
                      anotherOption = anotherValueForVO
                      mySection/
                          notMyOption = someValueForVO
              Production/
                  mainSection/
                      anotherOption = anotherValueInProductionForVO
                      mySection/
                          myOption = myValueInProductionForVO
              Certification/              -- this section will not be considered because Production setup is specified
                  mainSection/
                      anotherOption = anotherValueInCertificationForVO

    section option will be merged in the following order::

      /mainSection/mySection/myOption -- set a "myValue" for a myOption
      /Operations/Production/mainSection/mySection/myOption -- set a "myValueInProduction" for a myOption
      /Operations/myVO/Defaults/mainSection/mySection/myOption -- myOption is missing here, let's move on
      /Operations/myVO/Production/mainSection/mySection/myOption -- set a "myValueInProductionForVO" for a myOption

    So the method will return "myValueInProductionForVO".

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import six
from six.moves import _thread as thread
import os
from diraccfg import CFG
from DIRAC import S_OK, S_ERROR, gConfig
from DIRAC.Core.Utilities import LockRing
from DIRAC.ConfigurationSystem.Client.Helpers.CSGlobals import getSetup
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOForGroup
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
from DIRAC.Core.Security.ProxyInfo import getVOfromProxyGroup


class Operations(object):
  """ Operations class

      The /Operations CFG section is maintained in a cache by an Operations object

      You can also inherit this class to handle another section of the configuration
      by specifying the path to it in the mainSection argument, e.g.::

        class SessionData(Operations):
          ''' This class will have access to the configuration combined from the following sections
              in the following order:
                - /WebApp/
                - /Operations/<setup>/WebApp/
                - /Operations/<vo>/Defaults/WebApp/
                - /Operations/<vo>/<setup>/WebApp/
          '''

          def __init__(self, credDict, setup):
            self.__credDict = credDict
            super(SessionData, self).__init__(group=credDict.get("group"), setup=setup, mainSection='WebApp')

          def getTitle(self):
            return self.getValue('title', 'Hello world!')
  """

  __cache = {}
  __cacheVersion = 0
  __cacheLock = LockRing.LockRing().getLock()

  def __init__(self, vo=None, group=None, setup=None, mainSection=None):
    """ Determination of VO/setup and generation a list of relevant directories

        :param str vo: VO name
        :param str group: group name
        :param str setup: setup name
        :param str mainSection: main section name
    """
    basePath = '/Operations'
    self._vo = getVOForGroup(group or '') or vo or getVOfromProxyGroup().get('Value', '')
    self._setup = setup or getSetup()
    self._group = group
    self._mainPath = mainSection or ''

    # Define the configuration sections that will be merged in the following order:
    # -> /Operations/Defaults/
    # -> /Operations/<setup>/
    # -> /Operations/<vo>/Defaults/
    # -> /Operations/<vo>/<setup>/

    # If mainSection defined:
    # -> /<mainSection>/
    # -> /Operations/<setup>/<mainSection>/
    # -> /Operations/<vo>/Defaults/<mainSection>/
    # -> /Operations/<vo>/<setup>/<mainSection>/

    self.__paths = [os.path.join('/', self._mainPath)] if self._mainPath else [os.path.join(basePath, 'Defaults')]
    if self._setup:
      self.__paths.append(os.path.join(basePath, self._setup, self._mainPath))
    if self._vo:
      self.__paths.append(os.path.join(basePath, self._vo, 'Defaults', self._mainPath))
      if self._setup:
        self.__paths.append(os.path.join(basePath, self._vo, self._setup, self._mainPath))

  def _cacheExpired(self):
    """ Cache expired or not

        :return: bool
    """
    return Operations.__cacheVersion != gConfigurationData.getVersion()

  def __getCFGCache(self):
    """ Get cached CFG

        :return: CFG
    """
    Operations.__cacheLock.acquire()
    try:
      currentVersion = gConfigurationData.getVersion()
      if currentVersion != Operations.__cacheVersion:
        Operations.__cache = {}
        Operations.__cacheVersion = currentVersion

      cacheKey = (self._vo, self._setup, self._mainPath)
      if cacheKey in Operations.__cache:
        return Operations.__cache[cacheKey]

      mergedCFG = CFG()

      for path in self.__paths:
        pathCFG = gConfigurationData.mergedCFG[path]
        if pathCFG:
          mergedCFG = mergedCFG.mergeWith(pathCFG)

      Operations.__cache[cacheKey] = mergedCFG

      return Operations.__cache[cacheKey]
    finally:
      try:
        Operations.__cacheLock.release()
      except thread.error:
        pass

  def getValue(self, optionPath, defaultValue=None):
    """ Get option value

        :param str optionPath: option path
        :param defaultValue: default value

        :return: value
    """
    return self.__getCFGCache().getOption(optionPath, defaultValue)

  def _getCFG(self, sectionPath='/'):
    """ Get merged CFG object for section

        :param str sectionPath: section path

        :return: S_OK(CFG)/S_ERROR()
    """
    section = self.__getCFGCache().getRecursive(sectionPath)
    if not section:
      return S_ERROR("%s in Operations does not exist" % sectionPath)
    sectionCFG = section['value']
    if isinstance(sectionCFG, six.string_types):
      return S_ERROR("%s in Operations is not a section" % sectionPath)
    return S_OK(sectionCFG)

  def getSections(self, sectionPath='/', listOrdered=False):
    """ Get sections

        :param str sectionPath: section path
        :param bool listOrdered: to get ordered list

        :return: S_OK(list)/S_ERROR()
    """
    result = self._getCFG(sectionPath)
    return S_OK(result['Value'].listSections(listOrdered)) if result['OK'] else result

  def getOptions(self, sectionPath='/', listOrdered=False):
    """ Get options

        :param str sectionPath: section path
        :param bool listOrdered: to get ordered list

        :return: S_OK(list)/S_ERROR()
    """
    result = self._getCFG(sectionPath)
    return S_OK(result['Value'].listOptions(listOrdered)) if result['OK'] else result

  def getOptionsDict(self, sectionPath):
    """ Get options dictionary

        :param str sectionPath: section path

        :return: S_OK(dict)/S_ERROR()
    """
    result = self._getCFG(sectionPath)
    return S_OK({o: result['Value'][o] for o in result['Value'].listOptions()}) if result['OK'] else result

  def getPath(self, instance):
    """ Find the CS path for an option or section

        :param str instance: path with respect to the Operations standard path

        :return: str
    """
    return self.getOptionPath(instance) or self.getSectionPath(instance)

  def getOptionPath(self, option):
    """ Find the CS path for an option

        :param str option: path with respect to the WebApp standard path

        :return: str
    """
    for path in self.__paths:
      optionPath = os.path.join(path, option)
      if gConfig.getValue(optionPath, 'NoValue') != "NoValue":
        return optionPath
    return ''

  def getSectionPath(self, section):
    """ Find the CS path for an section

        :param str section: path with respect to the WebApp standard path

        :return: str
    """
    for path in self.__paths:
      sectionPath = os.path.join(path, section.strip('/'))
      result = gConfig.getSections(os.path.dirname(sectionPath))
      if result['OK'] and os.path.basename(sectionPath) in result['Value']:
        return sectionPath
    return ''
