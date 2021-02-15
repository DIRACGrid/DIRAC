""" This is the guy that parses and interprets the local configuration options.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import re
import os
import sys
import getopt

import DIRAC
from DIRAC import gLogger
from DIRAC import S_OK, S_ERROR

from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
from DIRAC.ConfigurationSystem.private.Refresher import gRefresher
from DIRAC.ConfigurationSystem.Client.PathFinder import getServiceSection, getAgentSection, getExecutorSection
from DIRAC.Core.Utilities.Devloader import Devloader


class LocalConfiguration(object):
  """
  Main class to interface with Configuration of a running DIRAC Component.

  For most cases this is handled via
    - DIRAC.Core.Base.Script class for scripts
    - dirac-agent for agents
    - dirac-service for services
  """

  def __init__(self, defaultSectionPath=""):
    self.currentSectionPath = defaultSectionPath
    self.mandatoryEntryList = []
    self.optionalEntryList = []
    self.commandOptionList = []
    self.unprocessedSwitches = []
    self.additionalCFGFiles = []
    self.parsedOptionList = []
    self.commandArgList = []
    self.cliAdditionalCFGFiles = []
    self.__registerBasicOptions()
    self.isParsed = False
    self.componentName = "Unknown"
    self.componentType = False
    self.loggingSection = "/DIRAC"
    self.initialized = False
    self.__scriptDescription = ''
    self.__helpUsageDoc = ''
    self.__helpArgumentsDoc = ''
    self.__helpExampleDoc = ''
    self.__debugMode = 0
    self.firstOptionIndex = 1

  def disableParsingCommandLine(self):
    self.isParsed = True

  def __getAbsolutePath(self, optionPath):
    if optionPath[0] == "/":
      return optionPath
    else:
      return "%s/%s" % (self.currentSectionPath, optionPath)

  def addMandatoryEntry(self, optionPath):
    """
    Define a mandatory Configuration data option for the parsing of the command line
    """
    self.mandatoryEntryList.append(optionPath)

  def addDefaultEntry(self, optionPath, value):
    """
    Define a default value for a Configuration data option
    """
    if optionPath[0] == "/":
      if not gConfigurationData.extractOptionFromCFG(optionPath):
        self.__setOptionValue(optionPath, value)
    else:
      self.optionalEntryList.append((optionPath,
                                     str(value)))

  def addCFGFile(self, filePath):
    """
    Load additional .cfg file to be parsed
    """
    self.additionalCFGFiles.append(filePath)

  def setUsageMessage(self, usageMsg):
    """ Define and parse message to be display by the showHelp method.

        :param str usageMsg: script description that can contain Usage, Example, Arguments, Options blocks
    """
    # Searched text
    context = r"(.*?)"
    # Start of any block description or end of __doc__
    startAnyBlockOrEnd = r"(?:\n(?:Usage|Example|Arguments|Options):+\n|$)"

    r = r"%s%s" % (context, startAnyBlockOrEnd)
    if usageMsg:
      # The description block is the first in the __doc__
      desc = re.search(r, usageMsg, re.DOTALL)
      if desc:
        self.__scriptDescription = '\n' + desc.group(1).strip('\n') + '\n'
      # The usage block starts with '\nUsage:\n' or '\nUsage::\n'
      usage = re.search(r"%s%s" % (r"Usage:+", r), usageMsg, re.DOTALL)
      if usage:
        self.__helpUsageDoc = '\nUsage:\n' + usage.group(1).strip('\n') + '\n'
      # The argument block starts with '\Arguments:\n' or '\Arguments::\n'
      args = re.search(r"%s%s" % (r"Arguments:+", r), usageMsg, re.DOTALL)
      if args:
        self.__helpArgumentsDoc = '\nArguments:\n' + args.group(1).strip('\n') + '\n'
      # The example block starts with '\Example:\n' or '\Example::\n'
      expl = re.search(r"%s%s" % (r"Example:+", r), usageMsg, re.DOTALL)
      if expl:
        self.__helpExampleDoc = '\nExample:\n' + expl.group(1).strip('\n') + '\n'

  def __setOptionValue(self, optionPath, value):
    gConfigurationData.setOptionInCFG(self.__getAbsolutePath(optionPath),
                                      str(value))

  def __registerBasicOptions(self):
    self.registerCmdOpt("o:", "option=", "Option=value to add",
                        self.__setOptionByCmd)
    self.registerCmdOpt("s:", "section=", "Set base section for relative parsed options",
                        self.__setSectionByCmd)
    self.registerCmdOpt("c:", "cert=", "Use server certificate to connect to Core Services",
                        self.__setUseCertByCmd)
    self.registerCmdOpt("d", "debug", "Set debug mode (-ddd is extra debug)",
                        self.__setDebugMode)
    self.registerCmdOpt("", "cfg=", "Load additional config file",
                        None)
    devLoader = Devloader()
    if devLoader.enabled:
      self.registerCmdOpt("", "autoreload", "Automatically restart if there's any change in the module",
                          self.__setAutoreload)
    self.registerCmdOpt("", "license", "Show DIRAC's LICENSE",
                        self.showLicense)
    self.registerCmdOpt("h", "help", "Shows this help",
                        self.showHelp)

  def registerCmdOpt(self, shortOption, longOption, helpString, function=False):
    """
    Register a new command line option
    """
    shortOption = shortOption.strip()
    longOption = longOption.strip()
    if not shortOption and not longOption:
      raise Exception("No short or long options defined")
    for optTuple in self.commandOptionList:
      if shortOption and optTuple[0] == shortOption:
        raise Exception("Short switch %s is already defined!" % shortOption)
      if longOption and optTuple[1] == longOption:
        raise Exception("Long switch %s is already defined!" % longOption)
    self.commandOptionList.append((shortOption, longOption, helpString, function))

  def getExtraCLICFGFiles(self):
    """
    Retrieve list of parsed .cfg files
    """
    if not self.isParsed:
      self.__parseCommandLine()
    return self.cliAdditionalCFGFiles

  def getPositionalArguments(self):
    """
    Retrieve list of command line positional arguments
    """
    if not self.isParsed:
      self.__parseCommandLine()
    return self.commandArgList

  def getUnprocessedSwitches(self):
    """
    Retrieve list of command line switches without a callback function
    """
    if not self.isParsed:
      self.__parseCommandLine()
    return self.unprocessedSwitches

  def __checkMandatoryOptions(self):
    try:
      isMandatoryMissing = False
      for optionPath in self.mandatoryEntryList:
        optionPath = self.__getAbsolutePath(optionPath)
        if not gConfigurationData.extractOptionFromCFG(optionPath):
          gLogger.fatal("Missing mandatory local configuration option", optionPath)
          isMandatoryMissing = True
      if isMandatoryMissing:
        return S_ERROR()
      return S_OK()
    except Exception as e:
      gLogger.exception()
      return S_ERROR(str(e))

  # TODO: Initialize if not previously initialized
  def initialize(self, componentName):
    """
    Make sure DIRAC is properly initialized
    """
    if self.initialized:
      return S_OK()
    self.initialized = True
    # Set that the command line has already been parsed
    self.isParsed = True
    if not self.componentType:
      self.setConfigurationForScript(componentName)
    try:
      retVal = self.__addUserDataToConfiguration()
      self.__initLogger(self.componentName, self.loggingSection)
      if not retVal['OK']:
        return retVal
      retVal = self.__checkMandatoryOptions()
      if not retVal['OK']:
        return retVal
    except Exception as e:
      gLogger.exception()
      return S_ERROR(str(e))
    return S_OK()

  def __initLogger(self, componentName, logSection, forceInit=False):
    gLogger.initialize(componentName, logSection, forceInit=forceInit)

    if self.__debugMode == 1:
      gLogger.setLevel("VERBOSE")
    elif self.__debugMode == 2:
      gLogger.setLevel("VERBOSE")
      gLogger.showHeaders(True)
    elif self.__debugMode >= 3:
      gLogger.setLevel("DEBUG")
      gLogger.showHeaders(True)

  def loadUserData(self):
    """
    This is the magic method that reads the command line and processes it
    It is used by the Script Base class and the dirac-service and dirac-agent scripts
    Before being called:
    - any additional switches to be processed
    - mandatory and default configuration configuration options must be defined.

    """
    if self.initialized:
      return S_OK()
    self.initialized = True
    try:
      retVal = self.__addUserDataToConfiguration()

      for optionTuple in self.optionalEntryList:
        optionPath = self.__getAbsolutePath(optionTuple[0])
        if not gConfigurationData.extractOptionFromCFG(optionPath):
          gConfigurationData.setOptionInCFG(optionPath, optionTuple[1])

      self.__initLogger(self.componentName, self.loggingSection)
      if not retVal['OK']:
        return retVal

      retVal = self.__checkMandatoryOptions()
      if not retVal['OK']:
        return retVal

    except Exception as e:
      gLogger.exception()
      return S_ERROR(str(e))
    return S_OK()

  def __parseCommandLine(self):
    gLogger.debug("Parsing command line")
    shortOption = ""
    longOptionList = []
    for optionTuple in self.commandOptionList:
      if shortOption.find(optionTuple[0]) < 0:
        shortOption += "%s" % optionTuple[0]
      else:
        if optionTuple[0]:
          gLogger.error("Short option -%s has been already defined" % optionTuple[0])
      if not optionTuple[1] in longOptionList:
        longOptionList.append("%s" % optionTuple[1])
      else:
        if optionTuple[1]:
          gLogger.error("Long option --%s has been already defined" % optionTuple[1])

    try:
      opts, args = getopt.gnu_getopt(sys.argv[self.firstOptionIndex:], shortOption, longOptionList)
    except getopt.GetoptError as x:
      # x = option "-k" not recognized
      # print help information and exit
      gLogger.fatal("Error when parsing command line arguments: %s" % str(x))
      self.showHelp(exitCode=2)

    for opt, val in opts:
      if opt in ('-h', '--help'):
        self.showHelp()
        sys.exit(2)
      if opt == '--cfg':
        self.cliAdditionalCFGFiles.append(os.path.expanduser(val))

    # environment variable to ensure smooth transition
    if os.getenv("DIRAC_NO_CFG", None):
      self.commandArgList = args
    else:
      # to avoid issuing the warning for correctly passed cfg files
      extraCfg = [os.path.expanduser(arg) for arg in args if arg.endswith(".cfg")]
      self.cliAdditionalCFGFiles.extend(extraCfg)
      self.commandArgList = [arg for arg in args if not arg.endswith(".cfg")]
      if extraCfg:
        # use error level to make sure users will always see the Warning
        gLogger.error("""WARNING: Parsing of '.cfg' files as command line arguments is changing!
          Set the environment variable 'export DIRAC_NO_CFG=1' to pass the file as a positional
          argument (this will become the default).
          To modify the local configuration use '--cfg <configfile>' instead.""")
        if os.environ.get("DIRAC_DEPRECATED_FAIL", None):
          raise NotImplementedError("ERROR: using deprecated config file passing option.")
    self.parsedOptionList = opts
    self.isParsed = True

  def __loadCFGFiles(self):
    """
    Loads possibly several cfg files, in order:
    1. ~/.dirac.cfg
    2. cfg files pointed by DIRACSYSCONFIG env variable (comma-separated)
    3. cfg files specified in addCFGFile calls
    4. cfg files that come from the command line
    """
    errorsList = []
    if 'DIRACSYSCONFIG' in os.environ:
      diracSysConfigFiles = os.environ['DIRACSYSCONFIG'].replace(' ', '').split(',')
      for diracSysConfigFile in reversed(diracSysConfigFiles):
        gConfigurationData.loadFile(diracSysConfigFile)
    gConfigurationData.loadFile(os.path.expanduser("~/.dirac.cfg"))
    for fileName in self.additionalCFGFiles:
      gLogger.debug("Loading file %s" % fileName)
      retVal = gConfigurationData.loadFile(fileName)
      if not retVal['OK']:
        gLogger.debug("Could not load file %s: %s" % (fileName, retVal['Message']))
        errorsList.append(retVal['Message'])
    for fileName in self.cliAdditionalCFGFiles:
      gLogger.debug("Loading file %s" % fileName)
      retVal = gConfigurationData.loadFile(fileName)
      if not retVal['OK']:
        gLogger.debug("Could not load file %s: %s" % (fileName, retVal['Message']))
        errorsList.append(retVal['Message'])
    return errorsList

  def __addUserDataToConfiguration(self):
    if not self.isParsed:
      self.__parseCommandLine()

    errorsList = self.__loadCFGFiles()

    if gConfigurationData.getServers():
      retVal = self.syncRemoteConfiguration()
      if not retVal['OK']:
        return retVal
    else:
      gLogger.warn("Running without remote configuration")

    try:
      if self.componentType == "service":
        self.__setDefaultSection(getServiceSection(self.componentName))
      elif self.componentType == "agent":
        self.__setDefaultSection(getAgentSection(self.componentName))
      elif self.componentType == "executor":
        self.__setDefaultSection(getExecutorSection(self.componentName))
      elif self.componentType == "web":
        self.__setDefaultSection("/%s" % self.componentName)
      elif self.componentType == "script":
        if self.componentName and self.componentName[0] == "/":
          self.__setDefaultSection(self.componentName)
          self.componentName = self.componentName[1:]
        else:
          self.__setDefaultSection("/Scripts/%s" % self.componentName)
      else:
        self.__setDefaultSection("/")
    except Exception as e:
      errorsList.append(str(e))

    self.unprocessedSwitches = []

    for optionName, optionValue in self.parsedOptionList:
      optionName = optionName.lstrip("-")
      for definedOptionTuple in self.commandOptionList:
        if optionName == definedOptionTuple[0].replace(":", "") or \
                optionName == definedOptionTuple[1].replace("=", ""):
          if definedOptionTuple[3]:
            retVal = definedOptionTuple[3](optionValue)
            if not isinstance(retVal, dict):
              errorsList.append("Callback for switch '%s' does not return S_OK or S_ERROR" % optionName)
            elif not retVal['OK']:
              errorsList.append(retVal['Message'])
          else:
            self.unprocessedSwitches.append((optionName, optionValue))

    if len(errorsList) > 0:
      return S_ERROR("\n%s" % "\n".join(errorsList))
    return S_OK()

  def disableCS(self):
    """
    Do not contact Configuration Server upon initialization
    """
    gRefresher.disable()

  def enableCS(self):
    """
    Force the connection the Configuration Server

    (And incidentaly reinitialize the ObjectLoader and logger)
    """
    res = gRefresher.enable()

    # This is quite ugly but necessary for the logging
    # We force the reinitialization of the ObjectLoader
    # so that it also takes into account the extensions
    # (since the first time it is loaded by the logger BEFORE the full CS init)
    # And then we regenerate all the backend
    if res['OK']:
      from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
      objLoader = ObjectLoader()
      objLoader.reloadRootModules()
      self.__initLogger(self.componentName, self.loggingSection, forceInit=True)
    return res

  def isCSEnabled(self):
    """
    Retrieve current status of the connection to Configuration Server
    """
    return gRefresher.isEnabled()

  def syncRemoteConfiguration(self, strict=False):
    """
    Force a Resync with Configuration Server
    Under normal conditions this is triggered by an access to any
    configuration data.
    """
    if self.componentName == "Configuration/Server":
      if gConfigurationData.isMaster():
        gLogger.info("Starting Master Configuration Server")
        gRefresher.disable()
        return S_OK()
    retDict = gRefresher.forceRefresh()
    if not retDict['OK']:
      gLogger.error("Can't update from any server", retDict['Message'])
      if strict:
        return retDict
    return S_OK()

  def __setDefaultSection(self, sectionPath):
    self.currentSectionPath = sectionPath
    self.loggingSection = self.currentSectionPath

  def setConfigurationForServer(self, serviceName):
    """
    Declare this is a DIRAC service
    """
    self.componentName = serviceName
    self.componentType = "service"

  def setConfigurationForAgent(self, agentName):
    """
    Declare this is a DIRAC agent
    """
    self.componentName = agentName
    self.componentType = "agent"

  def setConfigurationForExecutor(self, executorName):
    """
    Declare this is a DIRAC agent
    """
    self.componentName = executorName
    self.componentType = "executor"

  def setConfigurationForWeb(self, webName):
    """
    Declare this is a DIRAC agent
    """
    self.componentName = webName
    self.componentType = "web"

  def setConfigurationForScript(self, scriptName):
    """
    Declare this is a DIRAC script
    """
    self.componentName = scriptName
    self.componentType = "script"

  def __setSectionByCmd(self, value):
    if value[0] != "/":
      return S_ERROR("%s is not a valid section. It should start with '/'" % value)
    self.currentSectionPath = value
    return S_OK()

  def __setOptionByCmd(self, value):
    valueList = value.split("=")
    if len(valueList) < 2:
      # FIXME: in the method above an exception is raised, check consitency
      return S_ERROR("-o expects a option=value argument.\nFor example %s -o Port=1234" % sys.argv[0])
    self.__setOptionValue(valueList[0], "=".join(valueList[1:]))
    return S_OK()

  def __setUseCertByCmd(self, value):
    useCert = "no"
    if value.lower() in ("y", "yes", "true"):
      useCert = "yes"
    self.__setOptionValue("/DIRAC/Security/UseServerCertificate", useCert)
    return S_OK()

  def __setDebugMode(self, dummy=False):
    self.__debugMode += 1
    return S_OK()

  def __setAutoreload(self, filepath=False):
    devLoader = Devloader()
    devLoader.bootstrap()
    if filepath:
      devLoader.watchFile(filepath)
    gLogger.notice("Devloader started")
    return S_OK()

  def getDebugMode(self):
    return self.__debugMode

  def showLicense(self, dummy=False):
    """
    Print license
    """
    lpath = os.path.join(DIRAC.rootPath, "DIRAC", "LICENSE")
    sys.stdout.write(" - DIRAC is GPLv3 licensed\n\n")
    try:
      with open(lpath) as fd:
        sys.stdout.write(fd.read())
    except IOError:
      sys.stdout.write("Can't find GPLv3 license at %s. Somebody stole it!\n" % lpath)
      sys.stdout.write("Please check out http://www.gnu.org/licenses/gpl-3.0.html for more info\n")
    DIRAC.exit(0)

  def showHelp(self, dummy=False, exitCode=0):
    """
    Printout help message including a Usage message if defined via setUsageMessage method
    """
    if self.__scriptDescription:
      gLogger.notice(self.__scriptDescription)

    if self.__helpUsageDoc:
      gLogger.notice(self.__helpUsageDoc)
    else:
      gLogger.notice("\nUsage:")
      gLogger.notice("\n  %s [options] ..." % os.path.basename(sys.argv[0]))
      if dummy:
        gLogger.notice(dummy)

    gLogger.notice("\nGeneral options:")
    iLastOpt = 0
    for iPos, iVal in enumerate(self.commandOptionList):
      optionTuple = iVal
      if optionTuple[0].endswith(':'):
        line = "  -%s --%s : %s" % (optionTuple[0][:-1].ljust(2),
                                    (optionTuple[1][:-1] + ' <value> ').ljust(22),
                                    optionTuple[2])
        gLogger.notice(line)
      else:
        gLogger.notice("  -%s --%s : %s" % (optionTuple[0].ljust(2), optionTuple[1].ljust(22), optionTuple[2]))
      iLastOpt = iPos
      if optionTuple[0] == 'h':
        # Last general opt is always help
        break
    if iLastOpt + 1 < len(self.commandOptionList):
      gLogger.notice("\nOptions:")
      for iPos in range(iLastOpt + 1, len(self.commandOptionList)):
        optionTuple = self.commandOptionList[iPos]
        if optionTuple[0].endswith(':'):
          line = "  -%s --%s : %s" % (optionTuple[0][:-1].ljust(2),
                                      (optionTuple[1][:-1] + ' <value> ').ljust(22),
                                      optionTuple[2])
          gLogger.notice(line)
        else:
          gLogger.notice("  -%s --%s : %s" % (optionTuple[0].ljust(2), optionTuple[1].ljust(22), optionTuple[2]))

    if self.__helpArgumentsDoc:
      gLogger.notice(self.__helpArgumentsDoc)

    if self.__helpExampleDoc:
      gLogger.notice(self.__helpExampleDoc)

    gLogger.notice("")
    DIRAC.exit(exitCode)

  def deleteOption(self, optionPath):
    """
    Remove a Configuration Option from the local Configuration
    """
    gConfigurationData.deleteOptionInCFG(optionPath)
