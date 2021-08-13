""" Basic functions for interacting with CS objects
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


import six
from os.path import join

import DIRAC
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
from DIRAC.Core.Utilities import List
from DIRAC.ConfigurationSystem.Client.Helpers.Path import cfgPath
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
from DIRAC.ConfigurationSystem.private.Refresher import gRefresher

__RCSID__ = "$Id$"


class ConfigurationClient(object):
    """This class provide access to DIRAC configuration. Everywhere in the code you can find the use of
    the global :mod:`~DIRAC.ConfigurationSystem.Client.Config.gConfig` object to obtain configuration data.
    This object is an instance of this class::

      gConfig = ConfigurationClient()

    You can use it or do your own instance::

      # This instance will provide the configuration taking into account additional settings for "myVO" VO
      gConfigMyVO = ConfigurationClient(vo='myVO')

    Usage::

      # The configuration will be assembled for the default VO/setup
      gConfig = ConfigurationClient()

      # Get option for default VO/setup
      gConfig.getValue('/path/to/option')

      # Get option for some VO/setup
      gConfig.getValue('/path/to/option', vo='someVO', setup='someSetup')

      # OR
      gConfig['someVO', 'someSetup'].getValue('/path/to/option')

    """

    # Default place of the dirac.cfg
    diracConfigFilePath = join(DIRAC.rootPath, "etc", "dirac.cfg")

    def __init__(self, vo=None, setup=None, rootPath=None):
        """C'or

        :param str vo: name of the VO, which must be taken into account when providing information.
                       The default will be the default value from the configuration or value derived from proxy.
                       To access the original configuration unchanged, set False here.
        :param str setup: name of the setup, which must be taken into account when providing information.
                          The default will be the default value from the configuration.
        :param str rootPath: in case of need of access to a configuration on the certain directory
        """
        self._vo = vo
        self._setup = setup
        self._root = rootPath
        self._instances = {}

    def __getitem__(self, items):
        """In our case, it provides access to instances sensitive to VO and setup.

        Usage::

          # Create new default instance without VO and setup
          gConfig = ConfigurationClient()

          # Get configuration for default VO and default setup
          gConfig.getValue('/Resource/someOption')

          # Create new instance in dteam VO context
          gConfig['dteam'].getValue('/Resource/someOption')

          # Use existing instance created before
          gConfig['dteam'].getValue('/Resource/someAnotherOption')

          # Get option for VO and setup
          gConfig['dteam', 'mySetup'].getValue('/Resource/someOption')

          # Get option for setup
          gConfig[None, 'mySetup'].getValue('/Resource/someOption')

          # Get the option from the original configuration unchanged relative to VO/setup
          gConfig[False].getValue('/Resource/someOption')

        """
        if isinstance(items, six.string_types):
            items = (items or None,)
        items += (None,) * (3 - len(items))
        if items not in self._instances:
            self._instances[items] = ConfigurationClient(*items)
        return self._instances[items]

    def _discoverVO(self):
        """Discover VO"""
        # Check if it is a client installation and if VO is already not set.
        isServer = gConfigurationData.extractOptionFromCFG("/DIRAC/Security/UseServerCertificate")
        if (isServer or "false").lower() not in ("y", "yes", "true") and not self._vo:
            # Try to detect VO in the proxy
            from DIRAC.Core.Security.ProxyInfo import getVOfromProxyGroup

            self._vo = getVOfromProxyGroup().get("Value") or None

    def loadFile(self, fileName):
        """Load file

        :param str fileName: file name

        :return: S_OK()/S_ERROR()
        """
        return gConfigurationData.loadFile(fileName)

    def loadCFG(self, cfg):
        """Load CFG

        :param CFG() cfg: CFG object

        :return: S_OK()/S_ERROR()
        """
        return gConfigurationData.mergeWithLocal(cfg)

    def forceRefresh(self, fromMaster=False):
        """Force refresh

        :param bool fromMaster: refresh from master

        :return: S_OK()/S_ERROR()
        """
        return gRefresher.forceRefresh(fromMaster=fromMaster)

    def dumpLocalCFGToFile(self, fileName):
        """Dump local configuration to file

        :param str fileName: file name

        :return: S_OK()/S_ERROR()
        """
        return gConfigurationData.dumpLocalCFGToFile(fileName)

    def dumpRemoteCFGToFile(self, fileName):
        """Dump remote configuration to file

        :param str fileName: file name

        :return: S_OK()/S_ERROR()
        """
        return gConfigurationData.dumpRemoteCFGToFile(fileName)

    def addListenerToNewVersionEvent(self, functor):
        """Add listener to new version event

        :param str functor: functor
        """
        gRefresher.addListenerToNewVersionEvent(functor)

    def dumpCFGAsLocalCache(self, fileName=None, raw=False):
        """Dump local CFG cache to file

        :param str fileName: file name
        :param bool raw: raw

        :return: S_OK(str)/S_ERROR()
        """
        cfg = gConfigurationData.mergedCFG.clone()
        try:
            if not raw and cfg.isSection("DIRAC"):
                diracSec = cfg["DIRAC"]
                if diracSec.isSection("Configuration"):  # pylint: disable=no-member
                    confSec = diracSec["Configuration"]  # pylint: disable=unsubscriptable-object
                    for opt in ("Servers", "MasterServer"):
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
        """Get list of servers

        :return: list
        """
        return gConfigurationData.getServers()

    def useServerCertificate(self):
        """Get using server certificate status

        :return: bool
        """
        return gConfigurationData.useServerCertificate()

    def getValue(self, optionPath, defaultValue=None, vo=None, setup=None):
        """Get configuration value

        :param str optionPath: option path
        :param defaultValue: default value
        :param str vo: VO name
        :param str setup: setup name

        :return: type(defaultValue)
        """
        retVal = self.getOption(optionPath, defaultValue, vo=vo, setup=setup)
        return retVal["Value"] if retVal["OK"] else defaultValue

    def getOption(self, optionPath, typeValue=None, vo=None, setup=None):
        """Get configuration option

        :param str optionPath: option path
        :param typeValue: type of value
        :param str vo: VO name
        :param str setup: setup name

        :return: S_OK()/S_ERROR()
        """
        gRefresher.refreshConfigurationIfNeeded()
        optionPath = self._calculatePath(optionPath)
        optionValue = gConfigurationData.extractOptionFromCFG(optionPath, vo=vo or self._vo, setup=setup or self._setup)

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
                return S_OK(requestedType(List.fromChar(optionValue, ",")))
            except Exception as e:
                return S_ERROR("Can't convert value (%s) to comma separated list \n%s" % (str(optionValue), repr(e)))
        elif requestedType == bool:
            try:
                return S_OK(optionValue.lower() in ("y", "yes", "true", "1"))
            except Exception as e:
                return S_ERROR("Can't convert value (%s) to Boolean \n%s" % (str(optionValue), repr(e)))
        elif requestedType == dict:
            try:
                splitOption = List.fromChar(optionValue, ",")
                value = {}
                for opt in splitOption:
                    keyVal = [x.strip() for x in opt.split(":")]
                    if len(keyVal) == 1:
                        keyVal.append(True)
                    value[keyVal[0]] = keyVal[1]
                return S_OK(value)
            except Exception as e:
                return S_ERROR("Can't convert value (%s) to Dict \n%s" % (str(optionValue), repr(e)))
        else:
            try:
                return S_OK(requestedType(optionValue))
            except Exception as e:
                return S_ERROR(
                    "Type mismatch between default (%s) and configured value (%s) \n%s"
                    % (str(typeValue), optionValue, repr(e))
                )

    def getSections(self, sectionPath, listOrdered=True, vo=None, setup=None):
        """Get configuration sections

        :param str sectionPath: section path
        :param bool listOrdered: ordered
        :param str vo: VO name
        :param str setup: setup name

        :return: S_OK(list)/S_ERROR()
        """
        gRefresher.refreshConfigurationIfNeeded()
        sectionPath = self._calculatePath(sectionPath)
        sectionList = gConfigurationData.getSectionsFromCFG(
            sectionPath, ordered=listOrdered, vo=vo or self._vo, setup=setup or self._setup
        )
        if isinstance(sectionList, list):
            return S_OK(sectionList)
        return S_ERROR("Path %s does not exist or it's not a section" % sectionPath)

    def getOptions(self, sectionPath, listOrdered=True, vo=None, setup=None):
        """Get configuration options

        :param str sectionPath: section path
        :param bool listOrdered: ordered
        :param str vo: VO name
        :param str setup: setup name

        :return: S_OK(list)/S_ERROR()
        """
        gRefresher.refreshConfigurationIfNeeded()
        optionList = gConfigurationData.getOptionsFromCFG(
            self._calculatePath(sectionPath), ordered=listOrdered, vo=vo or self._vo, setup=setup or self._setup
        )
        if isinstance(optionList, list):
            return S_OK(optionList)
        return S_ERROR("Path %s does not exist or it's not a section" % sectionPath)

    def getOptionsDict(self, sectionPath, vo=None, setup=None):
        """Get configuration options in dictionary

        :param str sectionPath: section path
        :param str vo: VO name
        :param str setup: setup name

        :return: S_OK(dict)/S_ERROR()
        """
        gRefresher.refreshConfigurationIfNeeded()
        optionsDict = {}
        sectionPath = self._calculatePath(sectionPath)
        optionList = gConfigurationData.getOptionsFromCFG(sectionPath, vo=vo or self._vo, setup=setup or self._setup)
        if isinstance(optionList, list):
            for option in optionList:
                optionsDict[option] = gConfigurationData.extractOptionFromCFG(
                    "%s/%s" % (sectionPath, option), vo=vo or self._vo, setup=setup or self._setup
                )
            return S_OK(optionsDict)
        return S_ERROR("Path %s does not exist or it's not a section" % sectionPath)

    def getOptionsDictRecursively(self, sectionPath, vo=None, setup=None):
        """Get configuration options in dictionary recursively

        :param str sectionPath: section path
        :param str vo: VO name
        :param str setup: setup name

        :return: S_OK(dict)/S_ERROR()
        """
        sectionPath = self._calculatePath(sectionPath)
        result = self.getSections(sectionPath, vo=vo or self._vo, setup=setup or self._setup)
        if not result["OK"]:
            return result
        recDict = {}
        for section in result["Value"]:
            result = self.getOptionsDict(section, vo=vo or self._vo, setup=setup or self._setup)
            if not result["OK"]:
                return result
            recDict[section] = result["Value"]
        return S_OK(recDict)

    def getConfigurationTree(self, root="", *filters, **kwargs):
        """Create a dictionary with all sections, subsections and options
        starting from given root. Result can be filtered.

        :param str root: Starting point in the configuration tree.
        :param filters: Select results that contain given substrings (check full path, i.e. with option name)
        :type filters: str or python:list[str]
        :param str vo: VO name
        :param str setup: setup name

        :return: S_OK(dict)/S_ERROR() -- dictionary where keys are paths taken from
                 the configuration (e.g. /Systems/Configuration/...).
                 Value is "None" when path points to a section
                 or not "None" if path points to an option.
        """

        vo = kwargs.get("vo")
        setup = kwargs.get("setup")

        # check if root is an option (special case)
        option = self.getOption(root, vo=vo, setup=setup)
        if option["OK"]:
            result = {root: option["Value"]}

        else:
            result = {root: None}
            for substr in filters:
                if substr not in root:
                    result = {}
                    break

            # remove slashes at the end
            root = root.rstrip("/")

            # get options of current root
            options = self.getOptionsDict(root, vo=vo, setup=setup)
            if not options["OK"]:
                return S_ERROR("getOptionsDict() failed with message: %s" % options["Message"])

            for key, value in options["Value"].items():
                path = cfgPath(root, key)
                addOption = True
                for substr in filters:
                    if substr not in path:
                        addOption = False
                        break

                if addOption:
                    result[path] = value

            # get subsections of the root
            sections = self.getSections(root, vo=vo, setup=setup)
            if not sections["OK"]:
                return S_ERROR("getSections() failed with message: %s" % sections["Message"])

            # recursively go through subsections and get their subsections
            for section in sections["Value"]:
                subtree = self.getConfigurationTree("%s/%s" % (root, section), vo=vo, setup=setup, *filters)
                if not subtree["OK"]:
                    return S_ERROR("getConfigurationTree() failed with message: %s" % sections["Message"])
                result.update(subtree["Value"])

        return S_OK(result)

    def setOptionValue(self, optionPath, value):
        """Set a value in the local configuration

        :param str optionPath: option path
        :param str value: value
        """
        gConfigurationData.setOptionInCFG(self._calculatePath(optionPath), value)

    def _calculatePath(self, path):
        """An auxiliary method that helps to calculate the path

        :param str path: section path

        :return: str
        """
        return join(self._root, path.lstrip("/")) if self._root else path
