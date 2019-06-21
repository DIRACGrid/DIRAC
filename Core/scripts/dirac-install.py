#!/usr/bin/env python
"""
The main DIRAC installer script. It can be used to install the main DIRAC software, its
modules, web, rest etc. and DIRAC extensions.

In order to deploy DIRAC you have to provide: globalDefaultsURL, which is by default:
"http://diracproject.web.cern.ch/diracproject/configs/globalDefaults.cfg", but it can be
in the local file system in a separate directory. The content of this file is the following::

  Installations
  {
    DIRAC
    {
       DefaultsLocation =  http://diracproject.web.cern.ch/diracproject/dirac.cfg
       LocalInstallation
       {
        PythonVersion = 27
       }
       # in case you have a DIRAC extension
       LHCb
      {
      DefaultsLocation = http://lhcb-rpm.web.cern.ch/lhcb-rpm/lhcbdirac/lhcb.cfg
      }
    }
  }
  Projects
  {
    DIRAC
    {
      DefaultsLocation =  http://diracproject.web.cern.ch/diracproject/dirac.cfg
    }
    # in case you have a DIRAC extension
    LHCb
    {
      DefaultsLocation = http://lhcb-rpm.web.cern.ch/lhcb-rpm/lhcbdirac/lhcb.cfg
    }
  }

the DefaultsLocation for example::

  DefaultsLocation = http://diracproject.web.cern.ch/diracproject/dirac.cfg

must contain a minimal configuration. The following options must be in this
file::

  Releases=,UploadCommand=,BaseURL=

In case you want to overwrite the global configuration file, you have to use --defaultsURL

After providing the default configuration files, DIRAC or your extension can be installed from:

1. in a directory you have to be present globalDefaults.cfg, dirac.cfg and all binaries.
   For example::

    zmathe@dzmathe zmathe]$ ls tars/
    dirac.cfg  diracos-0.1.md5  diracos-0.1.tar.gz  DIRAC-v6r20-pre16.md5  DIRAC-v6r20-pre16.tar.gz
    globalDefaults.cfg release-DIRAC-v6r20-pre16.cfg  release-DIRAC-v6r20-pre16.md5
    zmathe@dzmathe zmathe]$

   For example::

    dirac-install -r v6r20-pre16 --dirac-os --dirac-os-version=0.0.1 -u /home/zmathe/tars

   this command will use  /home/zmathe/tars directory for the source code.
   It will install DIRAC v6r20-pre16, DIRAC OS 0.1 version

2. You can use your dedicated web server or the official DIRAC web server

   for example::

    dirac-install -r v6r20-pre16 --dirac-os --dirac-os-version=0.0.1

   It will install DIRAC v6r20-pre16

3. You have possibility to install a not-yet-released DIRAC, module or extension using -m or --tag options.
   The non release version can be specified.

   for example::

    dirac-install -l DIRAC -r v6r20-pre16 -g v14r0 -t client -m DIRAC --tag=integration

   It will install DIRAC v6r20-pre16, where the DIRAC package based on integration, other other packages will be
   the same what is specified in release.cfg file in v6r20-pre16 tarball.

    dirac-install -l DIRAC -r v6r20-pre16 -g v14r0 -t client  -m DIRAC --tag=v6r20-pre22

   It installs a specific tag

   Note: If the source is not provided, DIRAC repository is used, which is defined in the global
   configuration file.

   We can provide the repository url:code repository:::Project:::branch. for example::

     dirac-install -l DIRAC -r v6r20-pre16 -g v14r0 -t client \\
     -m https://github.com/zmathe/DIRAC.git:::DIRAC:::dev_main_branch, \\
     https://github.com/zmathe/WebAppDIRAC.git:::WebAppDIRAC:::extjs6 -e WebAppDIRAC

   it will install DIRAC based on dev_main_branch and WebAppDIRAC based on extjs6::

     dirac-install -l DIRAC -r v6r20-pre16 -g v14r0 -t client \\
     -m WebAppDIRAC --tag=integration -e WebAppDIRAC

   it will install DIRAC v6r20-pre16 and WebAppDIRAC integration branch

You can use install.cfg configuration file::

  DIRACOS = http://lhcb-rpm.web.cern.ch/lhcb-rpm/dirac/DIRACOS/
  WebAppDIRAC = https://github.com/zmathe/WebAppDIRAC.git
  DIRAC=https://github.com/DIRACGrid/DIRAC.git
  LocalInstallation
  {
    # Project = LHCbDIRAC
    # The project LHCbDIRAC is not defined in the globalsDefaults.cfg
    Project = LHCb
    Release = v9r2-pre8
    Extensions = LHCb
    ConfigurationServer = dips://lhcb-conf-dirac.cern.ch:9135/Configuration/Server
    Setup = LHCb-Production
    SkipCAChecks = True
    SkipCADownload = True
    WebAppDIRAC=extjs6
    DIRAC=rel-v6r20
  }

  dirac-install -l LHCb -r v9r2-pre8 -t server --dirac-os --dirac-os-version=0.0.6 install.cfg

"""

from __future__ import absolute_import, division, print_function, unicode_literals

import sys
import os
import getopt
import imp
import signal
import time
import stat
import shutil
import ssl
import hashlib
import tarfile

from contextlib import closing
from distutils.version import LooseVersion   # pylint: disable=no-name-in-module,import-error

try:
  # For Python 3.0 and later
  from urllib.request import urlopen, HTTPError, URLError
except ImportError:
  # Fall back to Python 2's urllib2
  from urllib2 import urlopen, HTTPError, URLError

__RCSID__ = "$Id$"

executablePerms = stat.S_IWUSR | stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH


def S_OK(value=""):
  return {'OK': True, 'Value': value}


def S_ERROR(msg=""):
  return {'OK': False, 'Message': msg}

############
# Start of CFG
############


class Params(object):

  def __init__(self):
    self.extensions = []
    self.project = 'DIRAC'
    self.installation = 'DIRAC'
    self.release = ""
    self.externalsType = 'client'
    self.pythonVersion = '27'
    self.platform = ""
    self.basePath = os.getcwd()
    self.targetPath = os.getcwd()
    self.buildExternals = False
    self.noAutoBuild = False
    self.debug = False
    self.externalsOnly = False
    self.lcgVer = ''
    self.noLcg = False
    self.useVersionsDir = False
    self.installSource = ""
    self.globalDefaults = False
    self.timeout = 300
    self.diracOSVersion = ''
    self.diracOS = False
    self.tag = ""
    self.modules = {}
    self.externalVersion = ""
    self.cleanPYTHONPATH = False
    self.createLink = False
    self.scriptSymlink = False


cliParams = Params()

###
# Release config manager
###


class ReleaseConfig(object):

  class CFG:

    def __init__(self, cfgData=""):
      """ c'tor
      :param self: self reference
      :param str cfgData: the content of the configuration file
      """
      self.data = {}
      self.children = {}
      if cfgData:
        self.parse(cfgData)

    def parse(self, cfgData):
      """
      It parses the configuration file and propagate the data and children
      with the content of the cfg file
      :param str cfgData: configuration data, which is the content of the configuration file
      """
      try:
        self.__parse(cfgData)
      except BaseException:
        import traceback
        traceback.print_exc()
        raise
      return self

    def getChild(self, path):
      """
      It return the child of a given section
      :param str, list, tuple path: for example: Installations/DIRAC, Projects/DIRAC
      :return object It returns a CFG instance
      """

      child = self
      if isinstance(path, (list, tuple)):
        pathList = path
      else:
        pathList = [sec.strip() for sec in path.split("/") if sec.strip()]
      for childName in pathList:
        if childName not in child.children:
          return False
        child = child.children[childName]
      return child

    def __parse(self, cfgData, cIndex=0):
      """
      It parse a given DIRAC cfg file and store the result in self.data variable.

      :param str cfgData: the content of the configuration file
      :param int cIndex: it is the new line counter
      """

      childName = ""
      numLine = 0
      while cIndex < len(cfgData):
        eol = cfgData.find("\n", cIndex)
        if eol < cIndex:
          # End?
          return cIndex
        numLine += 1
        if eol == cIndex:
          cIndex += 1
          continue
        line = cfgData[cIndex: eol].strip()
        # Jump EOL
        cIndex = eol + 1
        if not line or line[0] == "#":
          continue
        if line.find("+=") > -1:
          fields = line.split("+=")
          opName = fields[0].strip()
          if opName in self.data:
            self.data[opName] += ', %s' % '+='.join(fields[1:]).strip()
          else:
            self.data[opName] = '+='.join(fields[1:]).strip()
          continue

        if line.find("=") > -1:
          fields = line.split("=")
          self.data[fields[0].strip()] = "=".join(fields[1:]).strip()
          continue

        opFound = line.find("{")
        if opFound > -1:
          childName += line[:opFound].strip()
          if not childName:
            raise Exception("No section name defined for opening in line %s" % numLine)
          childName = childName.strip()
          self.children[childName] = ReleaseConfig.CFG()
          eoc = self.children[childName].__parse(cfgData, cIndex)
          cIndex = eoc
          childName = ""
          continue

        if line == "}":
          return cIndex
        # Must be name for section
        childName += line.strip()
      return cIndex

    def createSection(self, name, cfg=None):
      """
      It creates a subsection for an existing CS section.
      :param str name: the name of the section
      :param object cfg: the ReleaseConfig.CFG object loaded into memory
      """

      if isinstance(name, (list, tuple)):
        pathList = name
      else:
        pathList = [sec.strip() for sec in name.split("/") if sec.strip()]
      parent = self
      for lev in pathList[:-1]:
        if lev not in parent.children:
          parent.children[lev] = ReleaseConfig.CFG()
        parent = parent.children[lev]
      secName = pathList[-1]
      if secName not in parent.children:
        if not cfg:
          cfg = ReleaseConfig.CFG()
        parent.children[secName] = cfg
      return parent.children[secName]

    def isSection(self, obList):
      """
      Checks if a given path is a section
      :param str objList: is a path: for example: Releases/v6r20-pre16
      """
      return self.__exists([ob.strip() for ob in obList.split("/") if ob.strip()]) == 2

    def sections(self):
      """
      Returns all sections
      """
      return [k for k in self.children]

    def isOption(self, obList):
      return self.__exists([ob.strip() for ob in obList.split("/") if ob.strip()]) == 1

    def options(self):
      """
      Returns the options
      """
      return [k for k in self.data]

    def __exists(self, obList):
      """
      Check the existence of a certain element

      :param list obList: the list of cfg element names.
      for example: [Releases,v6r20-pre16]
      """
      if len(obList) == 1:
        if obList[0] in self.children:
          return 2
        elif obList[0] in self.data:
          return 1
        else:
          return 0
      if obList[0] in self.children:
        return self.children[obList[0]].__exists(obList[1:])
      return 0

    def get(self, opName, defaultValue=None):
      """
      It return the value of a certain option

      :param str opName: the name of the option
      :param str defaultValue: the default value of a given option
      """
      try:
        value = self.__get([op.strip() for op in opName.split("/") if op.strip()])
      except KeyError:
        if defaultValue is not None:
          return defaultValue
        raise
      if defaultValue is None:
        return value
      defType = type(defaultValue)
      if isinstance(defType, bool):
        return value.lower() in ("1", "true", "yes")
      try:
        return defType(value)
      except ValueError:
        return defaultValue

    def __get(self, obList):
      """
      It return a given section

      :param list obList: the list of cfg element names.
      """
      if len(obList) == 1:
        if obList[0] in self.data:
          return self.data[obList[0]]
        raise KeyError("Missing option %s" % obList[0])
      if obList[0] in self.children:
        return self.children[obList[0]].__get(obList[1:])
      raise KeyError("Missing section %s" % obList[0])

    def toString(self, tabs=0):
      """
      It return the configuration file as a string
      :param int tabs: the number of tabs used to format the CS string
      """

      lines = ["%s%s = %s" % ("  " * tabs, opName, self.data[opName]) for opName in self.data]
      for secName in self.children:
        lines.append("%s%s" % ("  " * tabs, secName))
        lines.append("%s{" % ("  " * tabs))
        lines.append(self.children[secName].toString(tabs + 1))
        lines.append("%s}" % ("  " * tabs))
      return "\n".join(lines)

    def getOptions(self, path=""):
      """
      Rturns the options for a given path

      :param str path: the path to the CS element
      """
      parentPath = [sec.strip() for sec in path.split("/") if sec.strip()][:-1]
      if parentPath:
        parent = self.getChild(parentPath)
      else:
        parent = self
      if not parent:
        return []
      return tuple(parent.data)

    def delPath(self, path):
      """
      It deletes a given CS element

      :param str path: the path to the CS element
      """
      path = [sec.strip() for sec in path.split("/") if sec.strip()]
      if not path:
        return
      keyName = path[-1]
      parentPath = path[:-1]
      if parentPath:
        parent = self.getChild(parentPath)
      else:
        parent = self
      if parent:
        parent.data.pop(keyName)

    def update(self, path, cfg):
      """
      Used to update the CS

      :param str path: path to the CS element
      :param object cfg: the CS object
      """
      parent = self.getChild(path)
      if not parent:
        self.createSection(path, cfg)
        return
      parent.__apply(cfg)

    def __apply(self, cfg):
      """
      It adds a certain cfg subsection to a given section

      :param object cfg: the CS object
      """
      for k in cfg.sections():
        if k in self.children:
          self.children[k].__apply(cfg.getChild(k))
        else:
          self.children[k] = cfg.getChild(k)
      for k in cfg.options():
        self.data[k] = cfg.get(k)
############################################################################
# END OF CFG CLASS
############################################################################

  def __init__(self, instName='DIRAC', projectName='DIRAC', globalDefaultsURL=None):
    """ c'tor
    :param str instName: the name of the installation
    :param str projectName: the name of the project
    :param str globalDefaultsURL: the default url
    """
    if globalDefaultsURL:
      self.globalDefaultsURL = globalDefaultsURL
    else:
      self.globalDefaultsURL = "http://diracproject.web.cern.ch/diracproject/configs/globalDefaults.cfg"
    self.globalDefaults = ReleaseConfig.CFG()
    self.loadedCfgs = []
    self.prjDepends = {}
    self.diracBaseModules = {}
    self.prjRelCFG = {}
    self.projectsLoadedBy = {}
    self.cfgCache = {}

    self.debugCB = False
    self.instName = instName
    self.projectName = projectName

  def setDebugCB(self, debFunc):
    """
    It is used by the dirac-distribution. It sets the debug function
    """
    self.debugCB = debFunc

  def __dbgMsg(self, msg):
    """
    :param str msg: the debug message
    """
    if self.debugCB:
      self.debugCB(msg)

  def __loadCFGFromURL(self, urlcfg, checkHash=False):
    """
    It is used to load the configuration file

    :param str urlcfg: the location of the source repository and
    where the default configuration file is exists.
    :param bool checkHash: check if the file is corrupted.
    """
    # This can be a local file
    if os.path.exists(urlcfg):
      with open(urlcfg, 'r') as relFile:
        cfgData = relFile.read()
    else:
      if urlcfg in self.cfgCache:
        return S_OK(self.cfgCache[urlcfg])
      try:
        cfgData = urlretrieveTimeout(urlcfg, timeout=cliParams.timeout)
        if not cfgData:
          return S_ERROR("Could not get data from %s" % urlcfg)
      except BaseException:
        return S_ERROR("Could not open %s" % urlcfg)
    try:
      # cfgData = cfgFile.read()
      cfg = ReleaseConfig.CFG(cfgData)
    except Exception as excp:
      return S_ERROR("Could not parse %s: %s" % (urlcfg, excp))
    # cfgFile.close()
    if not checkHash:
      self.cfgCache[urlcfg] = cfg
      return S_OK(cfg)
    try:
      md5path = urlcfg[:-4] + ".md5"
      if os.path.exists(md5path):
        md5File = open(md5path, 'r')
        md5Data = md5File.read()
        md5File.close()
      else:
        md5Data = urlretrieveTimeout(md5path, timeout=60)
      md5Hex = md5Data.strip()
      # md5File.close()
      if md5Hex != hashlib.md5(cfgData.encode('utf-8')).hexdigest():
        return S_ERROR("Hash check failed on %s" % urlcfg)
    except Exception as excp:
      return S_ERROR("Hash check failed on %s: %s" % (urlcfg, excp))
    self.cfgCache[urlcfg] = cfg
    return S_OK(cfg)

  def loadInstallationDefaults(self):
    """
    Load the default configurations
    """
    result = self.__loadGlobalDefaults()
    if not result['OK']:
      return result
    return self.__loadObjectDefaults("Installations", self.instName)

  def loadProjectDefaults(self):
    """
    Load default configurations
    """
    result = self.__loadGlobalDefaults()
    if not result['OK']:
      return result
    return self.__loadObjectDefaults("Projects", self.projectName)

  def __loadGlobalDefaults(self):
    """
    It loads the default configuration files
    """

    self.__dbgMsg("Loading global defaults from: %s" % self.globalDefaultsURL)
    result = self.__loadCFGFromURL(self.globalDefaultsURL)
    if not result['OK']:
      return result
    self.globalDefaults = result['Value']
    for k in ("Installations", "Projects"):
      if not self.globalDefaults.isSection(k):
        self.globalDefaults.createSection(k)
    self.__dbgMsg("Loaded global defaults")
    return S_OK()

  def __loadObjectDefaults(self, rootPath, objectName):
    """
    It loads the CFG, if it is not loaded.
    :param str rootPath: the main section. for example: Installations
    :param str objectName: The name of the section. for example: DIRAC
    """

    basePath = "%s/%s" % (rootPath, objectName)
    if basePath in self.loadedCfgs:
      return S_OK()

    # Check if it's a direct alias
    try:
      aliasTo = self.globalDefaults.get(basePath)
    except KeyError:
      aliasTo = False

    if aliasTo:
      self.__dbgMsg("%s is an alias to %s" % (objectName, aliasTo))
      result = self.__loadObjectDefaults(rootPath, aliasTo)
      if not result['OK']:
        return result
      cfg = result['Value']
      self.globalDefaults.update(basePath, cfg)
      return S_OK()

    # Load the defaults
    if self.globalDefaults.get("%s/SkipDefaults" % basePath, False):
      defaultsLocation = ""
    else:
      defaultsLocation = self.globalDefaults.get("%s/DefaultsLocation" % basePath, "")

    if not defaultsLocation:
      self.__dbgMsg("No defaults file defined for %s %s" % (rootPath.lower()[:-1], objectName))
    else:
      self.__dbgMsg("Defaults for %s are in %s" % (basePath, defaultsLocation))
      result = self.__loadCFGFromURL(defaultsLocation)
      if not result['OK']:
        return result
      cfg = result['Value']
      self.globalDefaults.update(basePath, cfg)

    # Check if the defaults have a sub alias
    try:
      aliasTo = self.globalDefaults.get("%s/Alias" % basePath)
    except KeyError:
      aliasTo = False

    if aliasTo:
      self.__dbgMsg("%s is an alias to %s" % (objectName, aliasTo))
      result = self.__loadObjectDefaults(rootPath, aliasTo)
      if not result['OK']:
        return result
      cfg = result['Value']
      self.globalDefaults.update(basePath, cfg)

    self.loadedCfgs.append(basePath)
    return S_OK(self.globalDefaults.getChild(basePath))

  def loadInstallationLocalDefaults(self, fileName):
    """
    Load the configuration file from a file

    :param str fileName: the configuration file name
    """
    try:
      fd = open(fileName, "r")
      # TODO: Merge with installation CFG
      cfg = ReleaseConfig.CFG().parse(fd.read())
      fd.close()
    except Exception as excp:
      return S_ERROR("Could not load %s: %s" % (fileName, excp))
    self.globalDefaults.update("Installations/%s" % self.instName, cfg)
    self.globalDefaults.update("Projects/%s" % self.instName, cfg)
    if self.projectName:
      # we have an extension and have a local cfg file
      self.globalDefaults.update("Projects/%s" % self.projectName, cfg)

    return S_OK()

  def getModuleVersionFromLocalCfg(self, moduleName):
    """
    It returns the version of a certain module defined in the LocalInstallation section
    :param str moduleName:
    :return str: the version of a certain module
    """
    return self.globalDefaults.get("Installations/%s/LocalInstallation/%s" % (self.instName, moduleName), "")

  def getInstallationCFG(self, instName=None):
    """
    Returns the installation name

    :param str instName: the installation name
    """
    if not instName:
      instName = self.instName
    return self.globalDefaults.getChild("Installations/%s" % instName)

  def getInstallationConfig(self, opName, instName=None):
    """
    It returns the configurations from the Installations section.
    This is usually provided in the local configuration file

    :param str opName: the option name for example: LocalInstallation/Release
    :param str instName:
    """
    if not instName:
      instName = self.instName
    return self.globalDefaults.get("Installations/%s/%s" % (instName, opName))

  def isProjectLoaded(self, project):
    """
    Checks if the project is loaded.

    :param str project: the name of the project
    """
    return project in self.prjRelCFG

  def getTarsLocation(self, project, module=None):
    """
      Returns the location of the binaries for a given project for example: LHCb or DIRAC, etc...

      :param str project: the name of the project
      """
    sourceUrl = self.globalDefaults.get("Projects/%s/BaseURL" % project, "")
    if module:
      # in case we define a different URL in the CS
      differntSourceUrl = self.globalDefaults.get("Projects/%s/%s" % (project, module), "")
      if differntSourceUrl:
        sourceUrl = differntSourceUrl
    if sourceUrl:
      return S_OK(sourceUrl)
    return S_ERROR("Don't know how to find the installation tarballs for project %s" % project)

  def getDiracOsLocation(self, project=None):
    """
    Returns the location of the DIRAC os binary for a given project for example: LHCb or DIRAC, etc...

    :param str project: the name of the project
    """
    if project is None:
      project = 'DIRAC'

    diracOsLoc = "Projects/%s/DIRACOS" % self.projectName
    if self.globalDefaults.isOption(diracOsLoc):
      # use from the VO specific configuration file
      location = self.globalDefaults.get(diracOsLoc, "")
    else:
      # use the default OS, provided by DIRAC
      location = self.globalDefaults.get("Projects/%s/DIRACOS" % project, "")
    return S_OK(location)

  def getUploadCommand(self, project=None):
    """
    It returns the command used to upload the binary

    :param str project: the name of the project
    """
    if not project:
      project = self.projectName
    defLoc = self.globalDefaults.get("Projects/%s/UploadCommand" % project, "")
    if defLoc:
      return S_OK(defLoc)
    return S_ERROR("No UploadCommand for %s" % project)

  def __loadReleaseConfig(self, project, release, releaseMode, sourceURL=None, relLocation=None):
    """
    It loads the release configuration file

    :param str project: the name of the project
    :param str release: the release version
    :param str releaseMode: the type of the release server/client
    :param str sourceURL: the source of the binary
    :param str relLocation: the release configuration file
    """
    if project not in self.prjRelCFG:
      self.prjRelCFG[project] = {}
    if release in self.prjRelCFG[project]:
      self.__dbgMsg("Release config for %s:%s has already been loaded" % (project, release))
      return S_OK()

    if relLocation:
      relcfgLoc = relLocation
    else:
      if releaseMode:
        try:
          relcfgLoc = self.globalDefaults.get("Projects/%s/Releases" % project)
        except KeyError:
          return S_ERROR("Missing Releases file for project %s" % project)
      else:
        if not sourceURL:
          result = self.getTarsLocation(project)
          if not result['OK']:
            return result
          siu = result['Value']
        else:
          siu = sourceURL
        relcfgLoc = "%s/release-%s-%s.cfg" % (siu, project, release)
    self.__dbgMsg("Releases file is %s" % relcfgLoc)
    result = self.__loadCFGFromURL(relcfgLoc, checkHash=not releaseMode)
    if not result['OK']:
      return result
    self.prjRelCFG[project][release] = result['Value']
    self.__dbgMsg("Loaded releases file %s" % relcfgLoc)

    return S_OK(self.prjRelCFG[project][release])

  def getReleaseCFG(self, project, release):
    """
    Returns the release configuration object

    :param str project: the name of the project
    :param str release: the release version
    """
    return self.prjRelCFG[project][release]

  def dumpReleasesToPath(self):
    """
    It dumps the content of the loaded configuration (memory content) to
    a given file
    """
    for project in self.prjRelCFG:
      prjRels = self.prjRelCFG[project]
      for release in prjRels:
        self.__dbgMsg("Dumping releases file for %s:%s" % (project, release))
        fd = open(
            os.path.join(
                cliParams.targetPath, "releases-%s-%s.cfg" %
                (project, release)), "w")
        fd.write(prjRels[release].toString())
        fd.close()

  def __checkCircularDependencies(self, key, routePath=None):
    """
    Check the dependencies

    :param str key: the name of the project and the release version
    :param list routePath: it stores the software packages, used to check the
    dependency
    """

    if not routePath:
      routePath = []
    if key not in self.projectsLoadedBy:
      return S_OK()
    routePath.insert(0, key)
    for lKey in self.projectsLoadedBy[key]:
      if lKey in routePath:
        routePath.insert(0, lKey)
        route = "->".join(["%s:%s" % sKey for sKey in routePath])
        return S_ERROR("Circular dependency found for %s: %s" % ("%s:%s" % lKey, route))
      result = self.__checkCircularDependencies(lKey, routePath)
      if not result['OK']:
        return result
    routePath.pop(0)
    return S_OK()

  def loadProjectRelease(self, releases,
                         project=None,
                         sourceURL=None,
                         releaseMode=None,
                         relLocation=None):
    """
    This method loads all project configurations (*.cfg). If a project is an extension of DIRAC,
    it will load the extension and after will load the base DIRAC module.

    :param list releases: list of releases, which will be loaded: for example: v6r19
    :param str project: the name of the project, if it is given. For example: DIRAC
    :param str sourceURL: the code repository
    :param str releaseMode:
    :param str relLocation: local configuration file,
                            which contains the releases. for example: file:///`pwd`/releases.cfg
    """

    if not project:
      project = self.projectName

    if not isinstance(releases, (list, tuple)):
      releases = [releases]

    # Load defaults
    result = self.__loadObjectDefaults("Projects", project)
    if not result['OK']:
      self.__dbgMsg("Could not load defaults for project %s" % project)
      return result

    if project not in self.prjDepends:
      self.prjDepends[project] = {}

    for release in releases:
      self.__dbgMsg("Processing dependencies for %s:%s" % (project, release))
      result = self.__loadReleaseConfig(project, release, releaseMode, sourceURL, relLocation)
      if not result['OK']:
        return result
      relCFG = result['Value']
      # Calculate dependencies and avoid circular deps
      self.prjDepends[project][release] = [(project, release)]
      relDeps = self.prjDepends[project][release]

      if not relCFG.getChild("Releases/%s" % (release)):  # pylint: disable=no-member
        return S_ERROR(
            "Release %s is not defined for project %s in the release file" %
            (release, project))

      initialDeps = self.getReleaseDependencies(project, release)
      if initialDeps:
        self.__dbgMsg("%s %s depends on %s" %
                      (project, release, ", ".join(["%s:%s" %
                                                    (k, initialDeps[k]) for k in initialDeps])))
      relDeps.extend([(p, initialDeps[p]) for p in initialDeps])
      for depProject in initialDeps:
        depVersion = initialDeps[depProject]
        # Check if already processed
        dKey = (depProject, depVersion)
        if dKey not in self.projectsLoadedBy:
          self.projectsLoadedBy[dKey] = []
        self.projectsLoadedBy[dKey].append((project, release))
        result = self.__checkCircularDependencies(dKey)
        if not result['OK']:
          return result
        # if it has already been processed just return OK
        if len(self.projectsLoadedBy[dKey]) > 1:
          return S_OK()

        # Load dependencies and calculate incompatibilities
        result = self.loadProjectRelease(depVersion, project=depProject)
        if not result['OK']:
          return result
        subDep = self.prjDepends[depProject][depVersion]
        # Merge dependencies
        for sKey in subDep:
          if sKey not in relDeps:
            relDeps.append(sKey)
            continue
          prj, vrs = sKey
          for pKey in relDeps:
            if pKey[0] == prj and pKey[1] != vrs:
              errMsg = "%s is required with two different versions ( %s and %s ) \
              starting with %s:%s" % (prj,
                                      pKey[1], vrs,
                                      project, release)
              return S_ERROR(errMsg)

      # Same version already required
      if project in relDeps and relDeps[project] != release:
        errMsg = "%s:%s requires itself with a different version through dependencies ( %s )" % (
            project, release, relDeps[project])
        return S_ERROR(errMsg)

      # we have now all dependencies, let's retrieve the resources (code repository)
      for project, version in relDeps:
        if project in self.diracBaseModules:
          continue
        modules = self.getModulesForRelease(version, project)
        if modules['OK']:
          for dependency in modules['Value']:
            self.diracBaseModules.setdefault(dependency, {})
            self.diracBaseModules[dependency]['Version'] = modules['Value'][dependency]
            res = self.getModSource(version, dependency, project)
            if not res['OK']:
              self.__dbgMsg(
                  "Unable to found the source URL for %s : %s" %
                  (dependency, res['Message']))
            else:
              self.diracBaseModules[dependency]['sourceUrl'] = res['Value'][1]

    return S_OK()

  def getReleaseOption(self, project, release, option):
    """
      Returns a given option

      :param str project: the name of the project
      :param str release: the release version
      :param str option: the option name
      """
    try:
      return self.prjRelCFG[project][release].get(option)
    except KeyError:
      self.__dbgMsg("Missing option %s for %s:%s" % (option, project, release))
      # try to found the option in a different release
      for project in self.prjRelCFG:
        for release in self.prjRelCFG[project]:
          if self.prjRelCFG[project][release].isOption(option):
            return self.prjRelCFG[project][release].get(option)
      return False

  def getReleaseDependencies(self, project, release):
    """
    It return the dependencies for a certain project

    :param str project: the name of the project
    :param str release: the release version
    """
    try:
      data = self.prjRelCFG[project][release].get("Releases/%s/Depends" % release)
    except KeyError:
      return {}
    data = [field for field in data.split(",") if field.strip()]
    deps = {}
    for field in data:
      field = field.strip()
      if not field:
        continue
      pv = field.split(":")
      if len(pv) == 1:
        deps[pv[0].strip()] = release
      else:
        deps[pv[0].strip()] = ":".join(pv[1:]).strip()
    return deps

  def getModulesForRelease(self, release, project=None):
    """
    Returns the modules for a given release for example: WebAppDIRAC,
    RESTDIRAC, LHCbWebAppDIRAC, etc

    :param str release: the release version
    :param str project: the project name
    """
    if not project:
      project = self.projectName
    if project not in self.prjRelCFG:
      return S_ERROR("Project %s has not been loaded. I'm a MEGA BUG! Please report me!" % project)
    if release not in self.prjRelCFG[project]:
      return S_ERROR("Version %s has not been loaded for project %s" % (release, project))
    config = self.prjRelCFG[project][release]
    if not config.isSection("Releases/%s" % release):
      return S_ERROR("Release %s is not defined for project %s" % (release, project))
    # Defined Modules explicitly in the release
    modules = self.getReleaseOption(project, release, "Releases/%s/Modules" % release)
    if modules:
      dMods = {}
      for entry in [entry.split(":") for entry in modules.split(
              ",") if entry.strip()]:  # pylint: disable=no-member
        if len(entry) == 1:
          dMods[entry[0].strip()] = release
        else:
          dMods[entry[0].strip()] = entry[1].strip()
      modules = dMods
    else:
      # Default modules with the same version as the release version
      modules = self.getReleaseOption(project, release, "DefaultModules")
      if modules:
        modules = dict((modName.strip(), release) for modName in modules.split(",")
                       if modName.strip())  # pylint: disable=no-member
      else:
        # Mod = project and same version
        modules = {project: release}
    # Check project is in the modNames if not DIRAC
    if project != "DIRAC":
      for modName in modules:
        if modName.find(project) != 0:
          return S_ERROR("Module %s does not start with the name %s" % (modName, project))
    return S_OK(modules)

  def getModSource(self, release, modName, project=None):
    """
    It reads the Sources section from the .cfg file for example:
    Sources
    {
      Web = git://github.com/DIRACGrid/DIRACWeb.git
      VMDIRAC = git://github.com/DIRACGrid/VMDIRAC.git
      DIRAC = git://github.com/DIRACGrid/DIRAC.git
      MPIDIRAC = git://github.com/DIRACGrid/MPIDIRAC.git
      BoincDIRAC = git://github.com/DIRACGrid/BoincDIRAC.git
      RESTDIRAC = git://github.com/DIRACGrid/RESTDIRAC.git
      COMDIRAC = git://github.com/DIRACGrid/COMDIRAC.git
      FSDIRAC = git://github.com/DIRACGrid/FSDIRAC.git
      WebAppDIRAC = git://github.com/DIRACGrid/WebAppDIRAC.git
    }

    :param str release: the release which is already loaded for example: v6r19
    :param str modName: the name of the DIRAC module for example: WebAppDIRAC
    :param str project: the name of the project for example: DIRAC
    """

    if self.projectName not in self.prjRelCFG:
      return S_ERROR(
          "Project %s has not been loaded. I'm a MEGA BUG! Please report me!" %
          self.projectName)

    if not project:
      project = self.projectName
    modLocation = self.getReleaseOption(project, release, "Sources/%s" % modName)
    if not modLocation:
      return S_ERROR("Source origin for module %s is not defined" % modName)
    modTpl = [field.strip() for field in modLocation.split(
        "|") if field.strip()]  # pylint: disable=no-member
    if len(modTpl) == 1:
      return S_OK((False, modTpl[0]))
    return S_OK((modTpl[0], modTpl[1]))

  def getExtenalsVersion(self, release=None):
    """
    It returns the version of DIRAC Externals. If it is not provided,
    uses the default cfg

    :param str release: the release version
    """

    if 'DIRAC' not in self.prjRelCFG:
      return False
    if not release:
      release = list(self.prjRelCFG['DIRAC'])
      release = sorted(release, key=LooseVersion)[-1]
    try:
      return self.prjRelCFG['DIRAC'][release].get('Releases/%s/Externals' % release)
    except KeyError:
      return False

  def getDiracOSVersion(self, diracOSVersion=None):
    """
      It returns the DIRACOS version
      :param str diracOSVersion: the OS version
      """

    if diracOSVersion:
      return diracOSVersion
    try:
      diracOSVersion = self.prjRelCFG[self.projectName][cliParams.release].get(
          "Releases/%s/DIRACOS" % cliParams.release, diracOSVersion)
      if not diracOSVersion:
        # the DIRAC extension does not specify DIRACOS version
        for release in self.prjRelCFG['DIRAC']:
          logWARN("Getting DIRACOS version from DIRAC %s!" % release)
          diracOSVersion = self.prjRelCFG['DIRAC'][release].get(
              "Releases/%s/DIRACOS" % release, diracOSVersion)
    except KeyError:
      pass
    return diracOSVersion

  def getLCGVersion(self, lcgVersion=None):
    """
    It returns the LCG version
    :param str lcgVersion: LCG version
    """
    if lcgVersion:
      return lcgVersion
    try:
      return self.prjRelCFG[self.projectName][cliParams.release].get(
          "Releases/%s/LcgVer" % cliParams.release, lcgVersion)
    except KeyError:
      pass
    return lcgVersion

  def getModulesToInstall(self, release, extensions=None):
    """
    It returns the modules to be installed.
    :param str release: the release version to be deployed
    :param str extensions: DIRAC extension
    :return: the order of the nodules and modules to be installed.
    """
    if not extensions:
      extensions = []
    extraFound = []
    modsToInstall = {}
    modsOrder = []
    if self.projectName not in self.prjDepends:
      return S_ERROR("Project %s has not been loaded" % self.projectName)
    if release not in self.prjDepends[self.projectName]:
      return S_ERROR(
          "Version %s has not been loaded for project %s" %
          (release, self.projectName))
    # Get a list of projects with their releases
    projects = list(self.prjDepends[self.projectName][release])
    for project, relVersion in projects:
      try:
        requiredModules = self.prjRelCFG[project][relVersion].get("RequiredExtraModules")
        requiredModules = [modName.strip()
                           for modName in requiredModules.split("/") if modName.strip()]
      except KeyError:
        requiredModules = []
      for modName in requiredModules:
        if modName not in extensions:
          extensions.append(modName)
      self.__dbgMsg("Discovering modules to install for %s (%s)" % (project, relVersion))
      result = self.getModulesForRelease(relVersion, project)
      if not result['OK']:
        return result
      modVersions = result['Value']
      try:
        defaultMods = self.prjRelCFG[project][relVersion].get("DefaultModules")
        modNames = [mod.strip() for mod in defaultMods.split(",") if mod.strip()]
      except KeyError:
        modNames = []
      for extension in extensions:
        # Check if the version of the extension module is specified in the command line
        extraVersion = None
        if ":" in extension:
          extension, extraVersion = extension.split(":")
          modVersions[extension] = extraVersion
        if extension in modVersions:
          modNames.append(extension)
          extraFound.append(extension)
        if 'DIRAC' not in extension:
          dextension = "%sDIRAC" % extension
          if dextension in modVersions:
            modNames.append(dextension)
            extraFound.append(extension)
      modNameVer = ["%s:%s" % (modName, modVersions[modName]) for modName in modNames]
      self.__dbgMsg("Modules to be installed for %s are: %s" % (project, ", ".join(modNameVer)))
      for modName in modNames:
        result = self.getTarsLocation(project, modName)
        if not result['OK']:
          return result
        tarsURL = result['Value']
        modVersion = modVersions[modName]
        defLoc = self.getModuleVersionFromLocalCfg(modName)
        if defLoc:
          modVersion = defLoc  # this overwrite the version which are defined in the release.cfg
        modsToInstall[modName] = (tarsURL, modVersion)
        modsOrder.insert(0, modName)

    for modName in extensions:
      if modName.split(":")[0] not in extraFound:
        return S_ERROR("No module %s defined. You sure it's defined for this release?" % modName)

    return S_OK((modsOrder, modsToInstall))


#################################################################################
# End of ReleaseConfig
#################################################################################


# platformAlias = { 'Darwin_i386_10.6' : 'Darwin_i386_10.5' }
platformAlias = {}

####
# Start of helper functions
####


def logDEBUG(msg):
  """
  :param str msg: debug message
  """
  if cliParams.debug:
    for line in msg.split("\n"):
      print ("%s UTC dirac-install [DEBUG] %s" % (time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime()), line))
    sys.stdout.flush()


def logERROR(msg):
  """
  :param str msg: error message
  """
  for line in msg.split("\n"):
    print ("%s UTC dirac-install [ERROR] %s" % (time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime()), line))
  sys.stdout.flush()


def logWARN(msg):
  """
  :param str msg: warning message
  """
  for line in msg.split("\n"):
    print ("%s UTC dirac-install [WARN] %s" % (time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime()), line))
  sys.stdout.flush()


def logNOTICE(msg):
  """
  :param str msg: notice message
  """
  for line in msg.split("\n"):
    print ("%s UTC dirac-install [NOTICE]  %s" % (time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime()), line))
  sys.stdout.flush()


def alarmTimeoutHandler(*args):
  """
  When a connection time out then raise and exception
  """
  raise Exception('Timeout')


def urlretrieveTimeout(url, fileName='', timeout=0):
  """
   Retrieve remote url to local file, with timeout wrapper

   :param str fileName: file name
   :param int timeout: time out in second used for downloading the files.
  """
  if fileName:
    # This can be a local file
    if os.path.exists(url):  # we do not download from web, use locally
      logDEBUG('Local file used: "%s"' % url)
      shutil.copy(url, fileName)
      return True
    localFD = open(fileName, "wb")

  # NOTE: Not thread-safe, since all threads will catch same alarm.
  #       This is OK for dirac-install, since there are no threads.
  logDEBUG('Retrieving remote file "%s"' % url)

  urlData = ''
  if timeout:
    signal.signal(signal.SIGALRM, alarmTimeoutHandler)
    # set timeout alarm
    signal.alarm(timeout + 5)
  try:
    # if "http_proxy" in os.environ and os.environ['http_proxy']:
    #   proxyIP = os.environ['http_proxy']
    #   proxy = urllib2.ProxyHandler( {'http': proxyIP} )
    #   opener = urllib2.build_opener( proxy )
    #   #opener = urllib2.build_opener()
    #  urllib2.install_opener( opener )

    # Try to use insecure context explicitly, needed for python >= 2.7.9
    try:
      context = ssl._create_unverified_context()
      remoteFD = urlopen(url, context=context)  # pylint: disable=unexpected-keyword-arg
      # the keyword 'context' is present from 2.7.9+
    except AttributeError:
      remoteFD = urlopen(url)
    expectedBytes = 0
    # Sometimes repositories do not return Content-Length parameter
    try:
      expectedBytes = long(remoteFD.info()['Content-Length'])
    except Exception as x:
      logWARN('Content-Length parameter not returned, skipping expectedBytes check')

    receivedBytes = 0
    data = remoteFD.read(16384)
    count = 1
    progressBar = False
    while data:
      receivedBytes += len(data)
      if fileName:
        localFD.write(data)
      else:
        urlData += data.decode('utf8', 'ignore')
      data = remoteFD.read(16384)
      if count % 20 == 0 and sys.stdout.isatty():
        print (u'\033[1D' + ".", end=" ")
        sys.stdout.flush()
        progressBar = True
      count += 1
    if progressBar and sys.stdout.isatty():
      # return cursor to the beginning of the line
      print ('\033[1K', end=" ")
      print ('\033[1A')
    if fileName:
      localFD.close()
    remoteFD.close()
    if receivedBytes != expectedBytes and expectedBytes > 0:
      logERROR("File should be %s bytes but received %s" % (expectedBytes, receivedBytes))
      return False
  except HTTPError as x:
    if x.code == 404:
      logERROR("%s does not exist" % url)
      if timeout:
        signal.alarm(0)
      return False
  except URLError:
    logERROR('Timeout after %s seconds on transfer request for "%s"' % (str(timeout), url))
  except Exception as x:
    if x == 'Timeout':
      logERROR('Timeout after %s seconds on transfer request for "%s"' % (str(timeout), url))
    if timeout:
      signal.alarm(0)
    raise x
  if timeout:
    signal.alarm(0)

  if fileName:
    return True
  else:
    return urlData


def downloadAndExtractTarball(tarsURL, pkgName, pkgVer, checkHash=True, cache=False):
  """
  It downloads and extracts a given tarball from a given destination: file system,
  web server or code repository.

  :param str tarsURL: the location of the source repository
  :param str pkgName: the name of the package to be installed
  :param str pkgVer: the version of the package
  :param bool checkHash: check the sanity of the file
  :param bool cache: use local cache for the tarballs

  """
  tarName = "%s-%s.tar.gz" % (pkgName, pkgVer)
  tarPath = os.path.join(cliParams.targetPath, tarName)
  tarFileURL = "%s/%s" % (tarsURL, tarName)
  tarFileCVMFS = "/cvmfs/dirac.egi.eu/installSource/%s" % tarName
  cacheDir = os.path.join(cliParams.basePath, ".installCache")
  tarCachePath = os.path.join(cacheDir, tarName)
  isSource = False
  if cache and os.path.isfile(tarCachePath):
    logNOTICE("Using cached copy of %s" % tarName)
    shutil.copy(tarCachePath, tarPath)
  elif os.path.exists(tarFileCVMFS):
    logNOTICE("Using CVMFS copy of %s" % tarName)
    tarPath = tarFileCVMFS
    checkHash = False
    cache = False
  else:
    logNOTICE("Retrieving %s" % tarFileURL)
    try:
      if not urlretrieveTimeout(tarFileURL, tarPath, cliParams.timeout):
        if os.path.exists(tarPath):
          os.unlink(tarPath)
        retVal = checkoutFromGit(pkgName, tarsURL, pkgVer)
        if not retVal['OK']:
          logERROR("Cannot download %s" % tarName)
          logERROR("Cannot download %s" % retVal['Value'])
          return False
        else:
          isSource = True
    except Exception as e:
      logERROR("Cannot download %s: %s" % (tarName, str(e)))
      sys.exit(1)
  if not isSource and checkHash:
    md5Name = "%s-%s.md5" % (pkgName, pkgVer)
    md5Path = os.path.join(cliParams.targetPath, md5Name)
    md5FileURL = "%s/%s" % (tarsURL, md5Name)
    md5CachePath = os.path.join(cacheDir, md5Name)
    if cache and os.path.isfile(md5CachePath):
      logNOTICE("Using cached copy of %s" % md5Name)
      shutil.copy(md5CachePath, md5Path)
    else:
      logNOTICE("Retrieving %s" % md5FileURL)
      try:
        if not urlretrieveTimeout(md5FileURL, md5Path, 60):
          logERROR("Cannot download %s" % tarName)
          return False
      except Exception as e:
        logERROR("Cannot download %s: %s" % (md5Name, str(e)))
        return False
    # Read md5
    fd = open(os.path.join(cliParams.targetPath, md5Name), "r")
    md5Expected = fd.read().strip()
    fd.close()
    # Calculate md5
    md5Calculated = hashlib.md5()
    with open(os.path.join(cliParams.targetPath, tarName), "rb") as fd:
      buf = fd.read(4096)
      while buf:
        md5Calculated.update(buf)
        buf = fd.read(4096)

    # Check
    if md5Expected != md5Calculated.hexdigest():
      logERROR("Oops... md5 for package %s failed!" % pkgVer)
      sys.exit(1)
    # Delete md5 file
    if cache:
      if not os.path.isdir(cacheDir):
        os.makedirs(cacheDir)
      os.rename(md5Path, md5CachePath)
    else:
      os.unlink(md5Path)
  # Extract
  # cwd = os.getcwd()
  # os.chdir(cliParams.targetPath)
  # tf = tarfile.open( tarPath, "r" )
  # for member in tf.getmembers():
  #  tf.extract( member )
  # os.chdir(cwd)
  if not isSource:
    with closing(tarfile.open(tarPath, mode="r:*")) as tar:
      for tarinfo in tar:  # pylint: disable=not-an-iterable
        try:
          tar.extract(tarinfo, cliParams.targetPath)  # pylint: disable=no-member
        except IOError:
          os.remove(tarinfo.name)
          tar.extract(tarinfo, cliParams.targetPath)  # pylint: disable=no-member
        finally:
          try:
            os.chmod(tarinfo.name, tarinfo.mode)
          except OSError:  # the file can be a link
            pass
    # Delete tar
    if cache:
      if not os.path.isdir(cacheDir):
        os.makedirs(cacheDir)
      os.rename(tarPath, tarCachePath)
    else:
      if tarPath != tarFileCVMFS:
        os.unlink(tarPath)

  postInstallScript = os.path.join(cliParams.targetPath, pkgName, 'dirac-postInstall.py')
  if os.path.isfile(postInstallScript):
    os.chmod(postInstallScript, executablePerms)
    logNOTICE("Executing %s..." % postInstallScript)
    if os.system("python '%s' > '%s.out' 2> '%s.err'" % (postInstallScript,
                                                         postInstallScript,
                                                         postInstallScript)):
      logERROR("Post installation script %s failed. Check %s.err" % (postInstallScript,
                                                                     postInstallScript))
  return True


def fixBuildPaths():
  """
  At compilation time many scripts get the building directory inserted,
  this needs to be changed to point to the current installation path:
  cliParams.targetPath
"""

  # Locate build path (from header of pydoc)
  binaryPath = os.path.join(cliParams.targetPath, cliParams.platform)
  pydocPath = os.path.join(binaryPath, 'bin', 'pydoc')
  try:
    fd = open(pydocPath)
    line = fd.readline()
    fd.close()
    buildPath = line[2:line.find(cliParams.platform) - 1]
    replaceCmd = "grep -rIl '%s' %s | xargs sed -i'.org' 's:%s:%s:g'" % (buildPath,
                                                                         binaryPath,
                                                                         buildPath,
                                                                         cliParams.targetPath)
    os.system(replaceCmd)

  except BaseException:
    pass


def fixPythonShebang():
  """
  Some scripts (like the gfal2 scripts) come with a shebang pointing to the system python.
  We replace it with the environment one
 """

  binaryPath = os.path.join(cliParams.targetPath, cliParams.platform)
  try:
    replaceCmd = "grep -rIl '#!/usr/bin/python' %s/bin |\
     xargs sed -i'.org' 's:#!/usr/bin/python:#!/usr/bin/env python:g'" % binaryPath
    os.system(replaceCmd)
  except BaseException:
    pass


def runExternalsPostInstall():
  """
   If there are any postInstall in externals, run them
  """
  if cliParams.diracOS or cliParams.diracOSVersion:
    postInstallPath = os.path.join(cliParams.targetPath, "postInstall")
  else:
    postInstallPath = os.path.join(cliParams.targetPath, cliParams.platform, "postInstall")
  if not os.path.isdir(postInstallPath):
    logDEBUG("There's no %s directory. Skipping postInstall step" % postInstallPath)
    return
  postInstallSuffix = "-postInstall"
  for scriptName in os.listdir(postInstallPath):
    if not scriptName.endswith(postInstallSuffix):
      logDEBUG("%s does not have the %s suffix. Skipping.." % (scriptName, postInstallSuffix))
      continue
    scriptPath = os.path.join(postInstallPath, scriptName)
    os.chmod(scriptPath, executablePerms)
    logNOTICE("Executing %s..." % scriptPath)
    if os.system("'%s' > '%s.out' 2> '%s.err'" % (scriptPath, scriptPath, scriptPath)):
      logERROR("Post installation script %s failed. Check %s.err" % (scriptPath, scriptPath))
      sys.exit(1)


def fixMySQLScript():
  """
   Update the mysql.server script (if installed) to point to the proper datadir
  """
  scriptPath = os.path.join(cliParams.targetPath, 'scripts', 'dirac-fix-mysql-script')
  bashrcFile = os.path.join(cliParams.targetPath, 'bashrc')
  if cliParams.useVersionsDir:
    bashrcFile = os.path.join(cliParams.basePath, 'bashrc')
  command = 'source %s; %s > /dev/null' % (bashrcFile, scriptPath)
  if os.path.exists(scriptPath):
    logNOTICE("Executing %s..." % command)
    os.system('bash -c "%s"' % command)


def checkPlatformAliasLink():
  """
  Make a link if there's an alias
  """
  if cliParams.platform in platformAlias:
    os.symlink(os.path.join(cliParams.targetPath, platformAlias[cliParams.platform]),
               os.path.join(cliParams.targetPath, cliParams.platform))


def installExternalRequirements(extType):
  """ Install the extension requirements if any
  """
  reqScript = os.path.join(cliParams.targetPath, "scripts", 'dirac-externals-requirements')
  bashrcFile = os.path.join(cliParams.targetPath, 'bashrc')
  if cliParams.useVersionsDir:
    bashrcFile = os.path.join(cliParams.basePath, 'bashrc')
  if os.path.isfile(reqScript):
    os.chmod(reqScript, executablePerms)
    logNOTICE("Executing %s..." % reqScript)
    command = "python '%s' -t '%s' > '%s.out' 2> '%s.err'" % (reqScript, extType,
                                                              reqScript, reqScript)
    if os.system('bash -c "source %s; %s"' % (bashrcFile, command)):
      logERROR("Requirements installation script %s failed. Check %s.err" % (reqScript,
                                                                             reqScript))
  return True


def discoverModules(modules):
  """
  Created the dictionary which contains all modules, which can be installed
  for example: {"DIRAC:{"sourceUrl":"https://github.com/zmathe/DIRAC.git","Vesrion:v6r20p11"}}

  :param: str modules: it contains meta information for the module,
  which will be installed: https://github.com/zmathe/DIRAC.git:::DIRAC:::dev_main_branch
  """

  projects = {}

  for module in modules.split(","):
    s = m = v = None
    try:
      s, m, v = module.split(":::")
    except ValueError:
      m = module.split(":::")[0]  # the source and version is not provided

    projects[m] = {}
    if s and v:
      projects[m] = {"sourceUrl": s, "Version": v}
  return projects

####
# End of helper functions
####


cmdOpts = (('r:', 'release=', 'Release version to install'),
           ('l:', 'project=', 'Project to install'),
           ('e:', 'extensions=', 'Extensions to install (comma separated)'),
           ('t:', 'installType=', 'Installation type (client/server)'),
           ('i:', 'pythonVersion=', 'Python version to compile (27/26)'),
           ('p:', 'platform=', 'Platform to install'),
           ('P:', 'installationPath=', 'Path where to install (default current working dir)'),
           ('b', 'build', 'Force local compilation'),
           ('g:', 'grid=', 'lcg tools package version'),
           ('  ', 'no-lcg-bundle', 'lcg tools not to be installed'),
           ('B', 'noAutoBuild', 'Do not build if not available'),
           ('v', 'useVersionsDir', 'Use versions directory'),
           ('u:', 'baseURL=', "Use URL as the source for installation tarballs"),
           ('d', 'debug', 'Show debug messages'),
           ('V:', 'installation=', 'Installation from which to extract parameter values'),
           ('X', 'externalsOnly', 'Only install external binaries'),
           ('M:', 'defaultsURL=', 'Where to retrieve the global defaults from'),
           ('h', 'help', 'Show this help'),
           ('T:', 'Timeout=', 'Timeout for downloads (default = %s)'),
           ('  ', 'dirac-os-version=', 'the version of the DIRAC OS'),
           ('  ', 'dirac-os', 'Enable installation of DIRAC OS'),
           ('  ', 'tag=', 'release version to install from git, http or local'),
           ('m:', 'module=',
            'Module to be installed. for example: -m DIRAC or -m git://github.com/DIRACGrid/DIRAC.git:DIRAC'),
           ('s:', 'source=', 'location of the modules to be installed'),
           ('x:', 'external=', 'external version'),
           ('  ', 'cleanPYTHONPATH', 'Only use the DIRAC PYTHONPATH (for pilots installation)'),
           ('  ', 'createLink', 'create version symbolic link from the versions directory. This is equivalent to the \
           following command: ln -s /opt/dirac/versions/vArBpC vArBpC'),
           ('  ', 'scriptSymlink', 'Symlink the scripts instead of creating wrapper')
           )


def usage():
  print ("\nUsage:\n\n  %s <opts> <cfgFile>" % os.path.basename(sys.argv[0]))
  print ("\nOptions:")
  for cmdOpt in cmdOpts:
    print ("\n  %s %s : %s" % (cmdOpt[0].ljust(3), cmdOpt[1].ljust(20), cmdOpt[2]))
  print
  print ("Known options and default values from /defaults section of releases file")
  for options in [('Release', cliParams.release),
                  ('Project', cliParams.project),
                  ('ModulesToInstall', []),
                  ('ExternalsType', cliParams.externalsType),
                  ('PythonVersion', cliParams.pythonVersion),
                  ('LcgVer', cliParams.lcgVer),
                  ('UseVersionsDir', cliParams.useVersionsDir),
                  ('BuildExternals', cliParams.buildExternals),
                  ('NoAutoBuild', cliParams.noAutoBuild),
                  ('Debug', cliParams.debug),
                  ('Timeout', cliParams.timeout)]:
    print (" %s = %s" % options)

  sys.exit(0)


def loadConfiguration():
  """
  It loads the configuration file
  """
  optList, args = getopt.getopt(sys.argv[1:],
                                "".join([opt[0] for opt in cmdOpts]),
                                [opt[1] for opt in cmdOpts])

  # First check if the name is defined
  for o, v in optList:
    if o in ('-h', '--help'):
      usage()
    elif o in ('-V', '--installation'):
      cliParams.installation = v
    elif o in ("-d", "--debug"):
      cliParams.debug = True
    elif o in ("-M", "--defaultsURL"):
      cliParams.globalDefaults = v

  releaseConfig = ReleaseConfig(
      instName=cliParams.installation,
      globalDefaultsURL=cliParams.globalDefaults)
  if cliParams.debug:
    releaseConfig.debugCB = logDEBUG

  result = releaseConfig.loadInstallationDefaults()
  if not result['OK']:
    logERROR("Could not load defaults: %s" % result['Message'])

  # at the end we load the local configuration and merge with the global cfg
  for arg in args:
    if len(arg) > 4 and arg.find(".cfg") == len(arg) - 4:
      result = releaseConfig.loadInstallationLocalDefaults(arg)
      if not result['OK']:
        logERROR(result['Message'])
      else:
        logNOTICE("Loaded %s" % arg)

  for opName in ('release', 'externalsType', 'installType', 'pythonVersion',
                 'buildExternals', 'noAutoBuild', 'debug', 'globalDefaults',
                 'lcgVer', 'useVersionsDir', 'targetPath',
                 'project', 'release', 'extensions', 'timeout'):
    try:
      opVal = releaseConfig.getInstallationConfig(
          "LocalInstallation/%s" % (opName[0].upper() + opName[1:]))
    except KeyError:
      continue

    if opName == 'installType':
      opName = 'externalsType'
    if sys.version_info[0] < 3:
      str_type = basestring
    else:
      str_type = str
    if isinstance(getattr(cliParams, opName), str_type):
      setattr(cliParams, opName, opVal)
    elif isinstance(getattr(cliParams, opName), bool):
      setattr(cliParams, opName, opVal.lower() in ("y", "yes", "true", "1"))
    elif isinstance(getattr(cliParams, opName), list):
      setattr(cliParams, opName, [opV.strip() for opV in opVal.split(",") if opV])

  # Now parse the ops
  for o, v in optList:
    if o in ('-r', '--release'):
      cliParams.release = v
    elif o in ('-l', '--project'):
      cliParams.project = v
    elif o in ('-e', '--extensions'):
      for pkg in [p.strip() for p in v.split(",") if p.strip()]:
        if pkg not in cliParams.extensions:
          cliParams.extensions.append(pkg)
    elif o in ('-t', '--installType'):
      cliParams.externalsType = v
    elif o in ('-i', '--pythonVersion'):
      cliParams.pythonVersion = v
    elif o in ('-p', '--platform'):
      cliParams.platform = v
    elif o in ('-d', '--debug'):
      cliParams.debug = True
    elif o in ('-g', '--grid'):
      cliParams.lcgVer = v
    elif o in ('--no-lcg-bundle'):
      cliParams.noLcg = True
    elif o in ('-u', '--baseURL'):
      cliParams.installSource = v
    elif o in ('-P', '--installationPath'):
      cliParams.targetPath = v
      try:
        os.makedirs(v)
      except BaseException:
        pass
    elif o in ('-v', '--useVersionsDir'):
      cliParams.useVersionsDir = True
    elif o in ('-b', '--build'):
      cliParams.buildExternals = True
    elif o in ("-B", '--noAutoBuild'):
      cliParams.noAutoBuild = True
    elif o in ('-X', '--externalsOnly'):
      cliParams.externalsOnly = True
    elif o in ('-T', '--Timeout'):
      try:
        cliParams.timeout = max(cliParams.timeout, int(v))
        cliParams.timeout = min(cliParams.timeout, 3600)
      except ValueError:
        pass
    elif o == '--dirac-os-version':
      cliParams.diracOSVersion = v
    elif o == '--dirac-os':
      cliParams.diracOS = True
    elif o == '--tag':
      cliParams.tag = v
    elif o in ('-m', '--module'):
      cliParams.modules = discoverModules(v)
    elif o in ('-x', '--external'):
      cliParams.externalVersion = v
    elif o == '--cleanPYTHONPATH':
      cliParams.cleanPYTHONPATH = True
    elif o == '--createLink':
      cliParams.createLink = True
    elif o == '--scriptSymlink':
      cliParams.scriptSymlink = True

  if not cliParams.release and not cliParams.modules:
    logERROR("Missing release to install")
    usage()

  cliParams.basePath = cliParams.targetPath
  if cliParams.useVersionsDir:
    # install under <installPath>/versions/<version>_<timestamp>
    cliParams.targetPath = os.path.join(
        cliParams.targetPath, 'versions', '%s_%s' % (cliParams.release, int(time.time())))
    try:
      os.makedirs(cliParams.targetPath)
    except BaseException:
      pass

  # If we are running an update, DIRACOS will be set in the environment
  if not cliParams.diracOS and 'DIRACOS' in os.environ:
    logWARN("Forcing to install DIRACOS, because it is already installed!")
    cliParams.diracOS = True

  logNOTICE("Destination path for installation is %s" % cliParams.targetPath)
  releaseConfig.projectName = cliParams.project

  result = releaseConfig.loadProjectRelease(cliParams.release,
                                            project=cliParams.project,
                                            sourceURL=cliParams.installSource)
  if not result['OK']:
    return result

  if not releaseConfig.isProjectLoaded("DIRAC"):
    return S_ERROR("DIRAC is not depended by this installation. Aborting")

  return S_OK(releaseConfig)


def compileExternals(extVersion):
  """
  It is used to compile the external for a given platform

  :param str extVersion: the external version
  """
  logNOTICE("Compiling externals %s" % extVersion)
  buildCmd = os.path.join(
      cliParams.targetPath,
      "DIRAC",
      "Core",
      "scripts",
      "dirac-compile-externals.py")
  buildCmd = "%s -t '%s' -D '%s' -v '%s' -i '%s'" % (buildCmd,
                                                     cliParams.externalsType,
                                                     os.path.join(
                                                         cliParams.targetPath,
                                                         cliParams.platform),
                                                     extVersion,
                                                     cliParams.pythonVersion)
  if os.system(buildCmd):
    logERROR("Could not compile binaries")
    return False
  return True


def getPlatform():
  """
  It returns the platform, where this script is running using Platform.py
  """
  platformPath = os.path.join(cliParams.targetPath, "DIRAC", "Core", "Utilities", "Platform.py")
  try:
    platFD = open(platformPath, "r")
  except IOError:
    logERROR("Cannot open Platform.py. Is DIRAC installed?")
    return ''

  Platform = imp.load_module("Platform", platFD, platformPath, ("", "r", imp.PY_SOURCE))
  platFD.close()
  return Platform.getPlatformString()


def installExternals(releaseConfig):
  """
  It install the DIRAC external. The version of the external is provided by
  the cmd or in the configuration file.

  :param object releaseConfig:
  """
  if not releaseConfig:
    externalsVersion = cliParams.externalVersion
  else:
    externalsVersion = releaseConfig.getExtenalsVersion()
  if not externalsVersion:
    logERROR("No externals defined")
    return False

  if not cliParams.platform:
    cliParams.platform = getPlatform()
  if not cliParams.platform:
    return False

  if cliParams.installSource:
    tarsURL = cliParams.installSource
  else:
    tarsURL = releaseConfig.getTarsLocation('DIRAC')['Value']

  if cliParams.buildExternals:
    compileExternals(externalsVersion)
  else:
    logDEBUG("Using platform: %s" % cliParams.platform)
    extVer = "%s-%s-%s-python%s" % (cliParams.externalsType, externalsVersion,
                                    cliParams.platform, cliParams.pythonVersion)
    logDEBUG("Externals %s are to be installed" % extVer)
    if not downloadAndExtractTarball(tarsURL, "Externals", extVer, cache=True):
      return (not cliParams.noAutoBuild) and compileExternals(externalsVersion)
    logNOTICE("Fixing externals paths...")
    fixBuildPaths()
  logNOTICE("Running externals post install...")
  checkPlatformAliasLink()
  return True


def installLCGutils(releaseConfig):
  """
  DIRAC uses various tools from LCG area. This method install a given
  lcg version.
  :param object releaseConfig: the configuration file object (class ReleaseConfig)
  """
  if not cliParams.platform:
    cliParams.platform = getPlatform()
  if not cliParams.platform:
    return False

  if cliParams.installSource:
    tarsURL = cliParams.installSource
  else:
    tarsURL = releaseConfig.getTarsLocation('DIRAC')['Value']

  # lcg utils?
  # LCG utils if required
  if not releaseConfig:
    lcgVer = cliParams.lcgVer
  else:
    lcgVer = releaseConfig.getLCGVersion(cliParams.lcgVer)
  if lcgVer:
    verString = "%s-%s-python%s" % (lcgVer, cliParams.platform, cliParams.pythonVersion)
    # HACK: try to find a more elegant solution for the lcg bundles location
    if not downloadAndExtractTarball(
        tarsURL +
        "/../lcgBundles",
        "DIRAC-lcg",
        verString,
        False,
            cache=True):
      logERROR(
          "\nThe requested LCG software version %s for the local operating system could not be downloaded." %
          verString)
      logERROR("Please, check the availability of the LCG software bindings for you \
      platform 'DIRAC-lcg-%s' \n in the repository %s/lcgBundles/." %
               (verString, os.path.dirname(tarsURL)))
      logERROR(
          "\nIf you would like to skip the installation of the LCG software, redo the installation with \
          adding the option --no-lcg-bundle to the command line.")
      return False

  logNOTICE("Fixing Python Shebang...")
  fixPythonShebang()
  return True


def createPermanentDirLinks():
  """ Create links to permanent directories from within the version directory
  """
  if cliParams.useVersionsDir:
    try:
      for directory in ['startup', 'runit', 'data', 'work', 'control', 'sbin', 'etc', 'webRoot']:
        fake = os.path.join(cliParams.targetPath, directory)
        real = os.path.join(cliParams.basePath, directory)
        if not os.path.exists(real):
          os.makedirs(real)
        if os.path.exists(fake):
          # Try to reproduce the directory structure to avoid lacking directories
          fakeDirs = os.listdir(fake)
          for fd in fakeDirs:
            if os.path.isdir(os.path.join(fake, fd)):
              if not os.path.exists(os.path.join(real, fd)):
                os.makedirs(os.path.join(real, fd))
          os.rename(fake, fake + '.bak')
        os.symlink(real, fake)
    except Exception as x:
      logERROR(str(x))
      return False

  return True


def createOldProLinks():
  """ Create links to permanent directories from within the version directory
  """
  proPath = cliParams.targetPath
  if cliParams.useVersionsDir:
    oldPath = os.path.join(cliParams.basePath, 'old')
    proPath = os.path.join(cliParams.basePath, 'pro')
    try:
      if os.path.exists(proPath) or os.path.islink(proPath):
        if os.path.exists(oldPath) or os.path.islink(oldPath):
          os.unlink(oldPath)
        os.rename(proPath, oldPath)
      os.symlink(cliParams.targetPath, proPath)
    except Exception as x:
      logERROR(str(x))
      return False

  return True


def createBashrc():
  """ Create DIRAC environment setting script for the bash shell
  """

  proPath = cliParams.targetPath
  # Now create bashrc at basePath
  try:
    bashrcFile = os.path.join(cliParams.targetPath, 'bashrc')
    if cliParams.useVersionsDir:
      bashrcFile = os.path.join(cliParams.basePath, 'bashrc')
      proPath = os.path.join(cliParams.basePath, 'pro')
    logNOTICE('Creating %s' % bashrcFile)
    if not os.path.exists(bashrcFile):
      lines = ['# DIRAC bashrc file, used by service and agent run scripts to set environment',
               'export PYTHONUNBUFFERED=yes',
               'export PYTHONOPTIMIZE=x']
      if 'HOME' in os.environ:
        lines.append('[ -z "$HOME" ] && export HOME=%s' % os.environ['HOME'])

      # Determining where the CAs are...
      if 'X509_CERT_DIR' in os.environ:
        certDir = os.environ['X509_CERT_DIR']
      else:
        if os.path.isdir('/etc/grid-security/certificates') and \
           os.listdir('/etc/grid-security/certificates'):
          # Assuming that, if present, it is not empty, and has correct CAs
          certDir = '/etc/grid-security/certificates'
        else:
          # But this will have to be created at some point (dirac-configure)
          certDir = '%s/etc/grid-security/certificates' % proPath
      lines.extend(['# CAs path for SSL verification',
                    'export X509_CERT_DIR=%s' % certDir,
                    'export SSL_CERT_DIR=%s' % certDir])

      lines.append(
          'export X509_VOMS_DIR=%s' %
          os.path.join(
              proPath,
              'etc',
              'grid-security',
              'vomsdir'))
      lines.extend(
          [
              '# Some DIRAC locations',
              '[ -z "$DIRAC" ] && export DIRAC=%s' %
              proPath,
              'export DIRACBIN=%s' %
              os.path.join(
                  "$DIRAC",
                  cliParams.platform,
                  'bin'),
              'export DIRACSCRIPTS=%s' %
              os.path.join(
                  "$DIRAC",
                  'scripts'),
              'export DIRACLIB=%s' %
              os.path.join(
                  "$DIRAC",
                  cliParams.platform,
                  'lib'),
              'export TERMINFO=%s' %
              __getTerminfoLocations(
                  os.path.join(
                      "$DIRAC",
                      cliParams.platform,
                      'share',
                      'terminfo')),
              'export RRD_DEFAULT_FONT=%s' %
              os.path.join(
                  "$DIRAC",
                  cliParams.platform,
                  'share',
                  'rrdtool',
                  'fonts',
                  'DejaVuSansMono-Roman.ttf')])

      lines.extend(['# Prepend the PYTHONPATH, the LD_LIBRARY_PATH, and the DYLD_LIBRARY_PATH'])
      lines.extend(['( echo $PATH | grep -q $DIRACBIN ) || export PATH=$DIRACBIN:$PATH',
                    '( echo $PATH | grep -q $DIRACSCRIPTS ) || export PATH=$DIRACSCRIPTS:$PATH',
                    '( echo $LD_LIBRARY_PATH | grep -q $DIRACLIB ) || \
                    export LD_LIBRARY_PATH=$DIRACLIB:$LD_LIBRARY_PATH',
                    '( echo $LD_LIBRARY_PATH | grep -q $DIRACLIB/mysql ) || \
                    export LD_LIBRARY_PATH=$DIRACLIB/mysql:$LD_LIBRARY_PATH',
                    '( echo $DYLD_LIBRARY_PATH | grep -q $DIRACLIB ) || \
                    export DYLD_LIBRARY_PATH=$DIRACLIB:$DYLD_LIBRARY_PATH',
                    '( echo $DYLD_LIBRARY_PATH | grep -q $DIRACLIB/mysql ) || \
                    export DYLD_LIBRARY_PATH=$DIRACLIB/mysql:$DYLD_LIBRARY_PATH'])

      if cliParams.cleanPYTHONPATH:
        lines.extend(['export PYTHONPATH=$DIRAC'])
      else:
        lines.extend(['( echo $PYTHONPATH | grep -q $DIRAC ) || export PYTHONPATH=$DIRAC:$PYTHONPATH'])

      lines.extend(['# new OpenSSL version require OPENSSL_CONF to point to some accessible location',
                    'export OPENSSL_CONF=/tmp'])

      # gfal2 requires some environment variables to be set
      lines.extend(['# Gfal2 configuration and plugins', 'export GFAL_CONFIG_DIR=%s' %
                    os.path.join("$DIRAC", cliParams.platform, 'etc/gfal2.d'), 'export  GFAL_PLUGIN_DIR=%s' %
                    os.path.join("$DIRACLIB", 'gfal2-plugins')])
      # add DIRACPLAT environment variable for client installations
      if cliParams.externalsType == 'client':
        lines.extend(['# DIRAC platform',
                      '[ -z "$DIRACPLAT" ] && export DIRACPLAT=`$DIRAC/scripts/dirac-platform`'])
      # Add the lines required for globus-* tools to use IPv6
      lines.extend(['# IPv6 support',
                    'export GLOBUS_IO_IPV6=TRUE',
                    'export GLOBUS_FTP_CLIENT_IPV6=TRUE'])
      # Add the lines required for ARC CE support
      lines.extend(['# ARC Computing Element',
                    'export ARC_PLUGIN_PATH=$DIRACLIB/arc'])
      lines.append('')
      f = open(bashrcFile, 'w')
      f.write('\n'.join(lines))
      f.close()
  except Exception as x:
    logERROR(str(x))
    return False

  return True


def createCshrc():
  """ Create DIRAC environment setting script for the (t)csh shell
  """
  proPath = cliParams.targetPath
  # Now create cshrc at basePath
  try:
    cshrcFile = os.path.join(cliParams.targetPath, 'cshrc')
    if cliParams.useVersionsDir:
      cshrcFile = os.path.join(cliParams.basePath, 'cshrc')
      proPath = os.path.join(cliParams.basePath, 'pro')
    logNOTICE('Creating %s' % cshrcFile)
    if not os.path.exists(cshrcFile):
      lines = ['# DIRAC cshrc file, used by clients to set up the environment',
               'setenv PYTHONUNBUFFERED yes',
               'setenv PYTHONOPTIMIZE x']

      # Determining where the CAs are...
      if 'X509_CERT_DIR' in os.environ:
        certDir = os.environ['X509_CERT_DIR']
      else:
        if os.path.isdir('/etc/grid-security/certificates'):
          # Assuming that, if present, it is not empty, and has correct CAs
          certDir = '/etc/grid-security/certificates'
        else:
          # But this will have to be created at some point (dirac-configure)
          certDir = '%s/etc/grid-security/certificates' % proPath
      lines.extend(['# CAs path for SSL verification',
                    'setenv X509_CERT_DIR %s' % certDir,
                    'setenv SSL_CERT_DIR %s' % certDir])

      lines.append(
          'setenv X509_VOMS_DIR %s' %
          os.path.join(
              proPath,
              'etc',
              'grid-security',
              'vomsdir'))
      lines.extend(['# Some DIRAC locations',
                    '( test $?DIRAC -eq 1 ) || setenv DIRAC %s' % proPath,
                    'setenv DIRACBIN %s' % os.path.join("$DIRAC", cliParams.platform, 'bin'),
                    'setenv DIRACSCRIPTS %s' % os.path.join("$DIRAC", 'scripts'),
                    'setenv DIRACLIB %s' % os.path.join("$DIRAC", cliParams.platform, 'lib'),
                    'setenv TERMINFO %s' % __getTerminfoLocations(os.path.join("$DIRAC",
                                                                               cliParams.platform,
                                                                               'share',
                                                                               'terminfo'))])

      lines.extend(['# Prepend the PYTHONPATH, the LD_LIBRARY_PATH, and the DYLD_LIBRARY_PATH'])

      lines.extend(['( test $?PATH -eq 1 ) || setenv PATH ""',
                    '( test $?LD_LIBRARY_PATH -eq 1 ) || setenv LD_LIBRARY_PATH ""',
                    '( test $?DY_LD_LIBRARY_PATH -eq 1 ) || setenv DYLD_LIBRARY_PATH ""',
                    '( test $?PYTHONPATH -eq 1 ) || setenv PYTHONPATH ""',
                    '( echo $PATH | grep -q $DIRACBIN ) || setenv PATH ${DIRACBIN}:$PATH',
                    '( echo $PATH | grep -q $DIRACSCRIPTS ) || setenv PATH ${DIRACSCRIPTS}:$PATH',
                    '( echo $LD_LIBRARY_PATH | grep -q $DIRACLIB ) || \
                    setenv LD_LIBRARY_PATH ${DIRACLIB}:$LD_LIBRARY_PATH',
                    '( echo $LD_LIBRARY_PATH | grep -q $DIRACLIB/mysql ) || \
                    setenv LD_LIBRARY_PATH ${DIRACLIB}/mysql:$LD_LIBRARY_PATH',
                    '( echo $DYLD_LIBRARY_PATH | grep -q $DIRACLIB ) || \
                    setenv DYLD_LIBRARY_PATH ${DIRACLIB}:$DYLD_LIBRARY_PATH',
                    '( echo $DYLD_LIBRARY_PATH | grep -q $DIRACLIB/mysql ) || \
                    setenv DYLD_LIBRARY_PATH ${DIRACLIB}/mysql:$DYLD_LIBRARY_PATH'])

      if cliParams.cleanPYTHONPATH:
        lines.extend(['setenv PYTHONPATH ${DIRAC}'])
      else:
        lines.extend(['( echo $PYTHONPATH | grep -q $DIRAC ) || setenv PYTHONPATH ${DIRAC}:$PYTHONPATH'])

      lines.extend(['# new OpenSSL version require OPENSSL_CONF to point to some accessible location',
                    'setenv OPENSSL_CONF /tmp'])
      lines.extend(['# IPv6 support',
                    'setenv GLOBUS_IO_IPV6 TRUE',
                    'setenv GLOBUS_FTP_CLIENT_IPV6 TRUE'])
      # gfal2 requires some environment variables to be set
      lines.extend(['# Gfal2 configuration and plugins', 'setenv GFAL_CONFIG_DIR %s' %
                    os.path.join("$DIRAC", cliParams.platform, 'etc/gfal2.d'), 'setenv  GFAL_PLUGIN_DIR %s' %
                    os.path.join("$DIRACLIB", 'gfal2-plugins')])
      # add DIRACPLAT environment variable for client installations
      if cliParams.externalsType == 'client':
        lines.extend(['# DIRAC platform',
                      'test $?DIRACPLAT -eq 1 || setenv DIRACPLAT `$DIRAC/scripts/dirac-platform`'])
      # Add the lines required for ARC CE support
      lines.extend(['# ARC Computing Element',
                    'setenv ARC_PLUGIN_PATH $DIRACLIB/arc'])
      lines.append('')
      f = open(cshrcFile, 'w')
      f.write('\n'.join(lines))
      f.close()
  except Exception as x:
    logERROR(str(x))
    return False

  return True


def writeDefaultConfiguration():
  """
  After DIRAC is installed a default configuration file is created,
  which contains a minimal setup.
  """
  if not releaseConfig:
    return
  instCFG = releaseConfig.getInstallationCFG()
  if not instCFG:
    return
  for opName in instCFG.getOptions():
    instCFG.delPath(opName)

  # filePath = os.path.join( cliParams.targetPath, "defaults-%s.cfg" % cliParams.installation )
  # Keep the default configuration file in the working directory
  filePath = "defaults-%s.cfg" % cliParams.installation
  try:
    fd = open(filePath, "wb")
    fd.write(instCFG.toString())
    fd.close()
  except Exception as excp:
    logERROR("Could not write %s: %s" % (filePath, excp))
  logNOTICE("Defaults written to %s" % filePath)


def __getTerminfoLocations(defaultLocation=None):
  """returns the terminfo locations as a colon separated string"""

  terminfoLocations = []
  if defaultLocation:
    terminfoLocations = [defaultLocation]

  for termpath in ['/usr/share/terminfo', '/etc/terminfo']:
    if os.path.exists(termpath):
      terminfoLocations.append(termpath)

  return ":".join(terminfoLocations)


def installDiracOS(releaseConfig):
  """
  Install the DIRAC os.

  :param str releaseConfig: the version of the DIRAC OS
  """
  diracOSVersion = releaseConfig.getDiracOSVersion(cliParams.diracOSVersion)
  if not diracOSVersion:
    logERROR("No diracos defined")
    return False
  tarsURL = None
  if cliParams.installSource:
    tarsURL = cliParams.installSource
  else:
    tarsURL = releaseConfig.getDiracOsLocation()['Value']
  if not tarsURL:
    tarsURL = releaseConfig.getTarsLocation('DIRAC')['Value']
    logWARN("DIRACOS location is not specified using %s" % tarsURL)
  if not downloadAndExtractTarball(tarsURL, "diracos", diracOSVersion, cache=True):
    return False
  logNOTICE("Fixing externals paths...")
  fixBuildPaths()
  logNOTICE("Running externals post install...")
  checkPlatformAliasLink()
  return True


def createBashrcForDiracOS():
  """ Create DIRAC environment setting script for the bash shell
  """

  proPath = cliParams.targetPath
  # Now create bashrc at basePath
  try:
    bashrcFile = os.path.join(cliParams.targetPath, 'bashrc')
    if cliParams.useVersionsDir:
      bashrcFile = os.path.join(cliParams.basePath, 'bashrc')
      proPath = os.path.join(cliParams.basePath, 'pro')
    logNOTICE('Creating %s' % bashrcFile)
    if not os.path.exists(bashrcFile):
      lines = ['# DIRAC bashrc file, used by service and agent run scripts to set environment',
               'export PYTHONUNBUFFERED=yes',
               'export PYTHONOPTIMIZE=x',
               '[ -z "$DIRACOS" ] && export DIRACOS=%s/diracos' % proPath,
               '. %s/diracos/diracosrc' % proPath]
      if 'HOME' in os.environ:
        lines.append('[ -z "$HOME" ] && export HOME=%s' % os.environ['HOME'])

      # Determining where the CAs are...
      if 'X509_CERT_DIR' in os.environ:
        certDir = os.environ['X509_CERT_DIR']
      else:
        if os.path.isdir('/etc/grid-security/certificates'):
          # Assuming that, if present, it is not empty, and has correct CAs
          certDir = '/etc/grid-security/certificates'
        else:
          # But this will have to be created at some point (dirac-configure)
          certDir = '%s/etc/grid-security/certificates' % proPath
      lines.extend(['# CAs path for SSL verification',
                    'export X509_CERT_DIR=%s' % certDir,
                    'export SSL_CERT_DIR=%s' % certDir])

      lines.append(
          'export X509_VOMS_DIR=%s' %
          os.path.join(
              proPath,
              'etc',
              'grid-security',
              'vomsdir'))
      lines.extend(
          [
              '# Some DIRAC locations',
              '[ -z "$DIRAC" ] && export DIRAC=%s' %
              proPath,
              'export DIRACSCRIPTS=%s' %
              os.path.join(
                  "$DIRAC",
                  'scripts'),
              'export TERMINFO=%s' %
              __getTerminfoLocations(
                  os.path.join(
                      "$DIRACOS",
                      'usr',
                      'share',
                      'terminfo')),
              'export RRD_DEFAULT_FONT=%s' %
              os.path.join(
                  "$DIRACOS",
                  'usr',
                  'share',
                  'fonts',
                  'DejaVuSansMono-Roman.ttf')])

      lines.extend(['# Prepend the PYTHONPATH, the LD_LIBRARY_PATH, and the DYLD_LIBRARY_PATH'])

      lines.extend(['( echo $PATH | grep -q $DIRACSCRIPTS ) || export PATH=$DIRACSCRIPTS:$PATH'])

      if cliParams.cleanPYTHONPATH:
        lines.extend(['export PYTHONPATH=$DIRAC'])
      else:
        lines.extend(['( echo $PYTHONPATH | grep -q $DIRAC ) || export PYTHONPATH=$DIRAC:$PYTHONPATH'])

      lines.extend(['# new OpenSSL version require OPENSSL_CONF to point to some accessible location',
                    'export OPENSSL_CONF=/tmp'])

      # gfal2 requires some environment variables to be set
      # Note: eventually this line should disappear as already set by diracosrc
      lines.extend(['# Gfal2 configuration and plugins',
                    'export GFAL_CONFIG_DIR=$DIRACOS/etc/gfal2.d',
                    'export  GFAL_PLUGIN_DIR=$DIRACOS/usr/lib64/gfal2-plugins/'])
      # add DIRACPLAT environment variable for client installations
      if cliParams.externalsType == 'client':
        lines.extend(['# DIRAC platform',
                      '[ -z "$DIRACPLAT" ] && export DIRACPLAT=`$DIRAC/scripts/dirac-platform`'])
      # Add the lines required for globus-* tools to use IPv6
      lines.extend(['# IPv6 support',
                    'export GLOBUS_IO_IPV6=TRUE',
                    'export GLOBUS_FTP_CLIENT_IPV6=TRUE'])
      # Add the lines required for ARC CE support
      # Note: eventually this line should disappear as already set by diracosrc
      lines.extend(['# ARC Computing Element',
                    'export ARC_PLUGIN_PATH=$DIRACOS/usr/lib64/arc'])
      lines.append('')
      with open(bashrcFile, 'w') as f:
        f.write('\n'.join(lines))
  except Exception as x:
    logERROR(str(x))
    return False

  return True


def checkoutFromGit(moduleName, sourceURL, tagVersion, destinationDir=None):
  """
  This method checkout a given tag from a git repository.
  Note: we can checkout any project form a git repository.

  :param str moduleName: The name of the Module: for example: LHCbWebDIRAC
  :param str sourceURL: The code repository: https://github.com/DIRACGrid/WebAppDIRAC.git
  :param str tagVersion: the tag for example: v3r1p10

  """

  codeRepo = moduleName + 'Repo'

  fDirName = os.path.join(cliParams.targetPath, codeRepo)
  cmd = "git clone '%s' '%s'" % (sourceURL, fDirName)

  logNOTICE("Executing: %s" % cmd)
  if os.system(cmd):
    return S_ERROR("Error while retrieving sources from git")

  branchName = "%s-%s" % (tagVersion, os.getpid())

  isTagCmd = "( cd '%s'; git tag -l | grep '%s' )" % (fDirName, tagVersion)
  if os.system(isTagCmd):
    # No tag found, assume branch
    branchSource = 'origin/%s' % tagVersion
  else:
    branchSource = tagVersion

  cmd = "( cd '%s'; git checkout -b '%s' '%s' )" % (fDirName, branchName, branchSource)

  logNOTICE("Executing: %s" % cmd)
  exportRes = os.system(cmd)

  if exportRes:
    return S_ERROR("Error while exporting from git")
  if os.path.exists(fDirName + '/' + moduleName):
    cmd = "ln -s %s/%s" % (codeRepo, moduleName)
  else:
    cmd = "mv %s %s" % (codeRepo, moduleName)

  logNOTICE("Executing: %s" % cmd)
  retVal = os.system(cmd)

  if retVal:
    return S_ERROR("Error while creating module: %s" % (moduleName))

  return S_OK()


def createSymbolicLink():
  """
  It creates a symbolic link to the actual directory from versions
  directory.
  """

  cmd = "ln -s %s %s" % (cliParams.targetPath, cliParams.release)
  logNOTICE("Executing: %s" % cmd)
  retVal = os.system(cmd)
  if retVal:
    return S_ERROR("Error while creating symbolic link!")

  return S_OK()


if __name__ == "__main__":
  logNOTICE("Processing installation requirements")
  result = loadConfiguration()
  releaseConfig = None
  modsToInstall = {}
  modsOrder = []
  if not result['OK']:
    # the configuration files does not exists, which means the module is not released.
    if cliParams.modules:
      logNOTICE(str(cliParams.modules))
      for i in cliParams.modules:
        modsOrder.append(i)
        modsToInstall[i] = (cliParams.modules[i]['sourceUrl'], cliParams.modules[i]['Version'])
    else:
      # there is no module provided which can be deployed
      logERROR(result['Message'])
      sys.exit(1)
  else:
    releaseConfig = result['Value']
  if not createPermanentDirLinks():
    sys.exit(1)

  if not cliParams.externalsOnly:
    logNOTICE("Discovering modules to install")
    if releaseConfig:
      result = releaseConfig.getModulesToInstall(cliParams.release, cliParams.extensions)
      if not result['OK']:
        logERROR(result['Message'])
        sys.exit(1)
      modsOrder, modsToInstall = result['Value']
    if cliParams.debug and releaseConfig:
      logNOTICE("Writing down the releases files")
      releaseConfig.dumpReleasesToPath()
    logNOTICE("Installing modules...")
    for modName in set(modsOrder):
      tarsURL, modVersion = modsToInstall[modName]
      if cliParams.installSource and not cliParams.modules:
        # we install not release version of DIRAC
        tarsURL = cliParams.installSource
      if modName in cliParams.modules:
        sourceURL = cliParams.modules[modName].get('sourceUrl')
        if 'Version' in cliParams.modules[modName]:
          modVersion = cliParams.modules[modName]['Version']
        if not sourceURL:
          retVal = releaseConfig.getModSource(cliParams.release, modName)
          if retVal['OK']:
            tarsURL = retVal['Value'][1]  # this is the git repository url
            modVersion = cliParams.tag
        else:
          tarsURL = sourceURL
        retVal = checkoutFromGit(modName, tarsURL, modVersion)
        if not retVal['OK']:
          logERROR("Cannot checkout %s" % retVal['Message'])
          sys.exit(1)
        continue
      logNOTICE("Installing %s:%s" % (modName, modVersion))
      if not downloadAndExtractTarball(tarsURL, modName, modVersion):
        sys.exit(1)
    logNOTICE("Deploying scripts...")
    ddeLocation = os.path.join(cliParams.targetPath, "DIRAC", "Core",
                               "scripts", "dirac-deploy-scripts.py")
    if not os.path.isfile(ddeLocation):
      ddeLocation = os.path.join(cliParams.targetPath, "DIRAC", "Core",
                                 "scripts", "dirac_deploy_scripts.py")
    if os.path.isfile(ddeLocation):
      cmd = ddeLocation

      # if specified, create symlink instead of wrapper.
      if cliParams.scriptSymlink:
        cmd += ' --symlink'

      # In MacOS /usr/bin/env does not find python in the $PATH, passing binary path
      # as an argument to the dirac-deploy-scripts
      if not cliParams.platform:
        cliParams.platform = getPlatform()
      if "Darwin" in cliParams.platform:
        binaryPath = os.path.join(cliParams.targetPath, cliParams.platform)
        logNOTICE("For MacOS (Darwin) use explicit binary path %s" % binaryPath)
        cmd += ' %s' % binaryPath

      os.system(cmd)
    else:
      logDEBUG("No dirac-deploy-scripts found. This doesn't look good")
  else:
    logNOTICE("Skipping installing DIRAC")

  if cliParams.diracOS:
    logNOTICE("Installing DIRAC OS %s..." % cliParams.diracOSVersion)
    if not installDiracOS(releaseConfig):
      sys.exit(1)
    if not createBashrcForDiracOS():
      sys.exit(1)
  else:
    logNOTICE("Installing %s externals..." % cliParams.externalsType)
    if not installExternals(releaseConfig):
      sys.exit(1)
    if cliParams.noLcg:
      logNOTICE("Skipping installation of LCG software...")
    else:
      logNOTICE("Installing LCG software...")
      if not installLCGutils(releaseConfig):
        sys.exit(1)
    if not createBashrc():
      sys.exit(1)
    if not createCshrc():
      sys.exit(1)
  if not createOldProLinks():
    sys.exit(1)
  runExternalsPostInstall()
  writeDefaultConfiguration()
  if cliParams.externalsType == "server":
    fixMySQLScript()
  installExternalRequirements(cliParams.externalsType)
  if cliParams.createLink:
    createSymbolicLink()
  logNOTICE("%s properly installed" % cliParams.installation)
  sys.exit(0)
