#!/usr/bin/env python
########################################################################
# File :    dirac-configure
# Author :  Ricardo Graciani
########################################################################
"""
Main script to write dirac.cfg for a new DIRAC installation and initial download of CAs and CRLs if necessary.

To be used by VO specific scripts to configure new DIRAC installations

Additionally all options can all be passed inside a .cfg file, see the `--cfg` option.
The following options are recognized::

    Setup
    ConfigurationServer
    IncludeAllServers
    Gateway
    SiteName
    CEName
    VirtualOrganization
    UseServerCertificate
    SkipCAChecks
    SkipCADownload
    UseVersionsDir
    Architecture
    LocalSE
    LogLevel

Setup and ConfigurationServer(Gateway) is mandatory options.

As in any other script command line option take precedence over .cfg files.
The combination of both is written into the installed dirac.cfg.

Notice: It will not overwrite exiting info in current dirac.cfg if it exists.

Example:
  $ dirac-configure -d
                    -S LHCb-Development
                    -C 'dips://lhcbprod.pic.es:9135/Configuration/Server'
                    -W 'dips://lhcbprod.pic.es:9135'
                    --SkipCAChecks
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import sys
import os

import six

import DIRAC
from DIRAC.Core.Utilities.File import mkDir
from DIRAC.Core.Utilities.DIRACScript import DIRACScript
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.Core.Utilities.SiteSEMapping import getSEsForSite
from DIRAC.ConfigurationSystem.Client.Helpers import cfgInstallPath, cfgPath, Registry
from DIRAC.FrameworkSystem.Client.BundleDeliveryClient import BundleDeliveryClient


class ConfigureInit(DIRACScript):

  def initParameters(self):
    self.vo = None
    self.setup = None
    self.update = False
    self.ceName = None
    self.localSE = None
    self.logLevel = None
    self.siteName = None
    self.outputFile = ''
    self.extensions = ''
    self.architecture = None
    self.skipCAChecks = False
    self.useServerCert = False
    self.gatewayServer = None
    self.useVersionsDir = False
    self.skipCADownload = False
    self.skipVOMSDownload = False
    self.includeAllServers = False
    self.configurationServer = None
    self.switches = [
        ("S:", "Setup=", "Set <setup> as DIRAC setup", self.setSetup),
        ("e:", "Extensions=", "Set <extensions> as DIRAC extensions", self.setExtensions),
        ("C:", "ConfigurationServer=", "Set <server> as DIRAC configuration server", self.setServer),
        ("I", "IncludeAllServers", "include all Configuration Servers", self.setAllServers),
        ("n:", "SiteName=", "Set <sitename> as DIRAC Site Name", self.setSiteName),
        ("N:", "CEName=", "Determiner <sitename> from <cename>", self.setCEName),
        ("V:", "VO=", "Set the VO name", self.setVO),
        ("W:", "gateway=", "Configure <gateway> as DIRAC Gateway for the site", self.setGateway),
        ("U", "UseServerCertificate", "Configure to use Server Certificate", self.setServerCert),
        ("H", "SkipCAChecks", "Configure to skip check of CAs", self.setSkipCAChecks),
        ("D", "SkipCADownload", "Configure to skip download of CAs", self.setSkipCADownload),
        ("M", "SkipVOMSDownload", "Configure to skip download of VOMS info", self.setSkipVOMSDownload),
        ("v", "UseVersionsDir", "Use versions directory", self.setUseVersionsDir),
        ("A:", "Architecture=", "Configure /Architecture=<architecture>", self.setArchitecture),
        ("L:", "LocalSE=", "Configure LocalSite/LocalSE=<localse>", self.setLocalSE),
        ("F", "ForceUpdate",
         "Force Update of cfg file (i.e. dirac.cfg) (otherwise nothing happens if dirac.cfg already exists)",
         self.forceUpdate),
        ("O:", "output=", "output configuration file", self.setOutput)
    ]

  def setGateway(self, optionValue):
    self.gatewayServer = optionValue
    self.setServer(self.gatewayServer + '/Configuration/Server')
    DIRAC.gConfig.setOptionValue(cfgInstallPath('Gateway'), self.gatewayServer)
    return DIRAC.S_OK()

  def setOutput(self, optionValue):
    self.outputFile = optionValue
    return DIRAC.S_OK()

  def setServer(self, optionValue):
    self.configurationServer = optionValue
    DIRAC.gConfig.setOptionValue('/DIRAC/Configuration/Servers', self.configurationServer)
    DIRAC.gConfig.setOptionValue(cfgInstallPath('ConfigurationServer'), self.configurationServer)
    return DIRAC.S_OK()

  def setAllServers(self, optionValue):
    self.includeAllServers = True

  def setSetup(self, optionValue):
    self.setup = optionValue
    DIRAC.gConfig.setOptionValue('/DIRAC/Setup', self.setup)
    DIRAC.gConfig.setOptionValue(cfgInstallPath('Setup'), self.setup)
    return DIRAC.S_OK()

  def setSiteName(self, optionValue):
    self.siteName = optionValue
    self.localCfg.addDefaultEntry('/LocalSite/Site', self.siteName)
    DIRAC.__siteName = False
    DIRAC.gConfig.setOptionValue(cfgInstallPath('SiteName'), self.siteName)
    return DIRAC.S_OK()

  def setCEName(self, optionValue):
    self.ceName = optionValue
    DIRAC.gConfig.setOptionValue(cfgInstallPath('CEName'), self.ceName)
    return DIRAC.S_OK()

  def setServerCert(self, optionValue):
    self.useServerCert = True
    DIRAC.gConfig.setOptionValue(cfgInstallPath('UseServerCertificate'), self.useServerCert)
    return DIRAC.S_OK()

  def setSkipCAChecks(self, optionValue):
    self.skipCAChecks = True
    DIRAC.gConfig.setOptionValue(cfgInstallPath('SkipCAChecks'), self.skipCAChecks)
    return DIRAC.S_OK()

  def setSkipCADownload(self, optionValue):
    self.skipCADownload = True
    DIRAC.gConfig.setOptionValue(cfgInstallPath('SkipCADownload'), self.skipCADownload)
    return DIRAC.S_OK()

  def setSkipVOMSDownload(self, optionValue):
    self.skipVOMSDownload = True
    DIRAC.gConfig.setOptionValue(cfgInstallPath('SkipVOMSDownload'), self.skipVOMSDownload)
    return DIRAC.S_OK()

  def setUseVersionsDir(self, optionValue):
    self.useVersionsDir = True
    DIRAC.gConfig.setOptionValue(cfgInstallPath('UseVersionsDir'), self.useVersionsDir)
    return DIRAC.S_OK()

  def setArchitecture(self, optionValue):
    self.architecture = optionValue
    self.localCfg.addDefaultEntry('/LocalSite/Architecture', self.architecture)
    DIRAC.gConfig.setOptionValue(cfgInstallPath('Architecture'), self.architecture)
    return DIRAC.S_OK()


def _runConfigurationWizard(setups, defaultSetup):
  """The implementation of the configuration wizard"""
  from prompt_toolkit import prompt, print_formatted_text, HTML
  from prompt_toolkit.completion import FuzzyWordCompleter

  # It makes no sense to have suggestions if there is no default so adjust the message accordingly
  msg = ""
  if setups:
    msg = "press tab for suggestions"
    if defaultSetup:
      msg = "default</b> <green>%s</green><b>, %s" % (defaultSetup, msg)
    msg = " (%s)" % msg
  # Get the Setup
  setup = prompt(
      HTML("<b>Choose a DIRAC Setup%s:</b>\n" % msg),
      completer=FuzzyWordCompleter(list(setups)),
  )
  if defaultSetup and not setup:
    setup = defaultSetup
  if setup not in setups:
    print_formatted_text(HTML("Unknown setup <yellow>%s</yellow> chosen" % setup))
    confirm = prompt(HTML("<b>Are you sure you want to continue?</b> "), default="n")
    if confirm.lower() not in ["y", "yes"]:
      return None

  # Get the URL to the master CS
  csURL = prompt(HTML("<b>Choose a configuration server URL (leave blank for default):</b>\n"))
  if not csURL:
    csURL = setups[setup]

  # Confirm
  print_formatted_text(HTML(
      "<b>Configuration is:</b>\n" +
      "  * <b>Setup:</b> <green>%s</green>\n" % setup +
      "  * <b>Configuration server:</b> <green>%s</green>\n" % csURL
  ))
  confirm = prompt(HTML("<b>Are you sure you want to continue?</b> "), default="y")
  if confirm.lower() in ["y", "yes"]:
    return setup, csURL
  else:
    return None


def runConfigurationWizard(params):
  """Interactively configure DIRAC using metadata from installed extensions"""
  import subprocess
  from prompt_toolkit import prompt, print_formatted_text, HTML
  from DIRAC.Core.Utilities.Extensions import extensionsByPriority, getExtensionMetadata

  for extension in extensionsByPriority():
    extensionMetadata = getExtensionMetadata(extension)
    if extensionMetadata.get("primary_extension", False):
      break
  defaultSetup = extensionMetadata.get("default_setup", "")
  setups = extensionMetadata.get("setups", {})

  # Run the wizard
  try:
    # Get the user's password and create a proxy so we can download from the CS
    while True:
      userPasswd = prompt(u"Enter Certificate password: ", is_password=True)
      result = subprocess.run(  # pylint: disable=no-member
          ["dirac-proxy-init", "--nocs", "--no-upload", "--pwstdin"],
          input=userPasswd,
          encoding="utf-8",
          check=False,
      )
      if result.returncode == 0:
        break
      print_formatted_text(HTML(
          "<red>Wizard failed, retrying...</red> (press Control + C to exit)\n"
      ))

    print_formatted_text()

    # Ask the user for the appropriate configuration settings
    while True:
      result = _runConfigurationWizard(setups, defaultSetup)
      if result:
        break
      print_formatted_text(HTML(
          "<red>Wizard failed, retrying...</red> (press Control + C to exit)\n"
      ))
  except KeyboardInterrupt:
    print_formatted_text(HTML("<red>Cancelled</red>"))
    sys.exit(1)

  def setVO(self, optionValue):
    self.vo = optionValue
    self.localCfg.addDefaultEntry('/DIRAC/VirtualOrganization', self.vo)
    DIRAC.gConfig.setOptionValue(cfgInstallPath('VirtualOrganization'), self.vo)
    return DIRAC.S_OK()

  def forceUpdate(self, optionValue):
    self.update = True
    return DIRAC.S_OK()

  def setExtensions(self, optionValue):
    self.extensions = optionValue
    DIRAC.gConfig.setOptionValue('/DIRAC/Extensions', self.extensions)
    DIRAC.gConfig.setOptionValue(cfgInstallPath('Extensions'), self.extensions)
    return DIRAC.S_OK()


@ConfigureInit()
def main(self):
  self.disableCS()

  self.registerSwitches(self.switches)
  self.parseCommandLine(ignoreErrors=True)

  if not self.logLevel:
    self.logLevel = DIRAC.gConfig.getValue(cfgInstallPath('LogLevel'), '')
    if self.logLevel:
      DIRAC.gLogger.setLevel(self.logLevel)
  else:
    DIRAC.gConfig.setOptionValue(cfgInstallPath('LogLevel'), self.logLevel)

  if not self.gatewayServer:
    newGatewayServer = DIRAC.gConfig.getValue(cfgInstallPath('Gateway'), '')
    if newGatewayServer:
      self.setGateway(newGatewayServer)

  if not self.configurationServer:
    newConfigurationServer = DIRAC.gConfig.getValue(cfgInstallPath('ConfigurationServer'), '')
    if newConfigurationServer:
      self.setServer(newConfigurationServer)

  if not self.includeAllServers:
    newIncludeAllServer = DIRAC.gConfig.getValue(cfgInstallPath('IncludeAllServers'), False)
    if newIncludeAllServer:
      self.setAllServers(True)

  if not self.setup:
    newSetup = DIRAC.gConfig.getValue(cfgInstallPath('Setup'), '')
    if newSetup:
      self.setSetup(newSetup)

  if not self.siteName:
    newSiteName = DIRAC.gConfig.getValue(cfgInstallPath('SiteName'), '')
    if newSiteName:
      self.setSiteName(newSiteName)

  if not self.ceName:
    newCEName = DIRAC.gConfig.getValue(cfgInstallPath('CEName'), '')
    if newCEName:
      self.setCEName(newCEName)

  if not self.useServerCert:
    newUserServerCert = DIRAC.gConfig.getValue(cfgInstallPath('UseServerCertificate'), False)
    if newUserServerCert:
      self.setServerCert(newUserServerCert)

  if not self.skipCAChecks:
    newSkipCAChecks = DIRAC.gConfig.getValue(cfgInstallPath('SkipCAChecks'), False)
    if newSkipCAChecks:
      self.setSkipCAChecks(newSkipCAChecks)

  if not self.skipCADownload:
    newSkipCADownload = DIRAC.gConfig.getValue(cfgInstallPath('SkipCADownload'), False)
    if newSkipCADownload:
      self.setSkipCADownload(newSkipCADownload)

  if not self.useVersionsDir:
    newUseVersionsDir = DIRAC.gConfig.getValue(cfgInstallPath('UseVersionsDir'), False)
    if newUseVersionsDir:
      self.setUseVersionsDir(newUseVersionsDir)
      # Set proper Defaults in configuration (even if they will be properly overwrite by gComponentInstaller
      instancePath = os.path.dirname(os.path.dirname(DIRAC.rootPath))
      rootPath = os.path.join(instancePath, 'pro')
      DIRAC.gConfig.setOptionValue(cfgInstallPath('InstancePath'), instancePath)
      DIRAC.gConfig.setOptionValue(cfgInstallPath('RootPath'), rootPath)

  if not self.architecture:
    newArchitecture = DIRAC.gConfig.getValue(cfgInstallPath('Architecture'), '')
    if newArchitecture:
      self.setArchitecture(newArchitecture)

  if not self.vo:
    newVO = DIRAC.gConfig.getValue(cfgInstallPath('VirtualOrganization'), '')
    if newVO:
      self.setVO(newVO)

  if not self.extensions:
    newExtensions = DIRAC.gConfig.getValue(cfgInstallPath('Extensions'), '')
    if newExtensions:
      self.setExtensions(newExtensions)

  DIRAC.gLogger.notice('Executing: %s ' % (' '.join(sys.argv)))
  DIRAC.gLogger.notice('Checking DIRAC installation at "%s"' % DIRAC.rootPath)

  if self.update:
    if self.outputFile:
      DIRAC.gLogger.notice('Will update the output file %s' % self.outputFile)
    else:
      DIRAC.gLogger.notice('Will update %s' % DIRAC.gConfig.diracConfigFilePath)

  if self.setup:
    DIRAC.gLogger.verbose('/DIRAC/Setup =', self.setup)
  if self.vo:
    DIRAC.gLogger.verbose('/DIRAC/VirtualOrganization =', self.vo)
  if self.configurationServer:
    DIRAC.gLogger.verbose('/DIRAC/Configuration/Servers =', self.configurationServer)
  if self.siteName:
    DIRAC.gLogger.verbose('/LocalSite/Site =', self.siteName)
  if self.architecture:
    DIRAC.gLogger.verbose('/LocalSite/Architecture =', self.architecture)
  if self.localSE:
    DIRAC.gLogger.verbose('/LocalSite/localSE =', self.localSE)

  if not self.useServerCert:
    DIRAC.gLogger.verbose('/DIRAC/Security/UseServerCertificate =', 'no')
    # Being sure it was not there before
    self.localCfg.deleteOption('/DIRAC/Security/UseServerCertificate')
    self.localCfg.addDefaultEntry('/DIRAC/Security/UseServerCertificate', 'no')
  else:
    DIRAC.gLogger.verbose('/DIRAC/Security/UseServerCertificate =', 'yes')
    # Being sure it was not there before
    self.localCfg.deleteOption('/DIRAC/Security/UseServerCertificate')
    self.localCfg.addDefaultEntry('/DIRAC/Security/UseServerCertificate', 'yes')

  host = DIRAC.gConfig.getValue(cfgInstallPath("Host"), "")
  if host:
    DIRAC.gConfig.setOptionValue(cfgPath("DIRAC", "Hostname"), host)

  if self.skipCAChecks:
    DIRAC.gLogger.verbose('/DIRAC/Security/SkipCAChecks =', 'yes')
    # Being sure it was not there before
    self.localCfg.deleteOption('/DIRAC/Security/SkipCAChecks')
    self.localCfg.addDefaultEntry('/DIRAC/Security/SkipCAChecks', 'yes')
  else:
    # Necessary to allow initial download of CA's
    if not self.skipCADownload:
      DIRAC.gConfig.setOptionValue('/DIRAC/Security/SkipCAChecks', 'yes')
  if not self.skipCADownload:
    self.enableCS()
    try:
      dirName = os.path.join(DIRAC.rootPath, 'etc', 'grid-security', 'certificates')
      mkDir(dirName)
    except BaseException:
      DIRAC.gLogger.exception()
      DIRAC.gLogger.fatal('Fail to create directory:', dirName)
      DIRAC.exit(-1)
    try:
      bdc = BundleDeliveryClient()
      result = bdc.syncCAs()
      if result['OK']:
        result = bdc.syncCRLs()
    except Exception as e:
      DIRAC.gLogger.error('Failed to sync CAs and CRLs: %s' % str(e))

    if not self.skipCAChecks:
      self.localCfg.deleteOption('/DIRAC/Security/SkipCAChecks')

  if self.ceName or self.siteName:
    # This is used in the pilot context, we should have a proxy, or a certificate, and access to CS
    if self.useServerCert:
      # Being sure it was not there before
      self.localCfg.deleteOption('/DIRAC/Security/UseServerCertificate')
      self.localCfg.addDefaultEntry('/DIRAC/Security/UseServerCertificate', 'yes')
    self.enableCS()
    # Get the site resource section
    gridSections = DIRAC.gConfig.getSections('/Resources/Sites/')
    if not gridSections['OK']:
      DIRAC.gLogger.warn('Could not get grid sections list')
      grids = []
    else:
      grids = gridSections['Value']
    # try to get siteName from ceName or Local SE from siteName using Remote Configuration
    for grid in grids:
      siteSections = DIRAC.gConfig.getSections('/Resources/Sites/%s/' % grid)
      if not siteSections['OK']:
        DIRAC.gLogger.warn('Could not get %s site list' % grid)
        sites = []
      else:
        sites = siteSections['Value']

      if not self.siteName:
        if self.ceName:
          for site in sites:
            res = DIRAC.gConfig.getSections('/Resources/Sites/%s/%s/CEs/' % (grid, site), [])
            if not res['OK']:
              DIRAC.gLogger.warn('Could not get %s CEs list' % site)
            if self.ceName in res['Value']:
              self.siteName = site
              break
      if self.siteName:
        DIRAC.gLogger.notice('Setting /LocalSite/Site = %s' % self.siteName)
        self.localCfg.addDefaultEntry('/LocalSite/Site', self.siteName)
        DIRAC.__siteName = False
        if self.ceName:
          DIRAC.gLogger.notice('Setting /LocalSite/GridCE = %s' % self.ceName)
          self.localCfg.addDefaultEntry('/LocalSite/GridCE', self.ceName)

        if not self.localSE and self.siteName in sites:
          self.localSE = getSEsForSite(self.siteName)
          if self.localSE['OK'] and self.localSE['Value']:
            self.localSE = ','.join(self.localSE['Value'])
            DIRAC.gLogger.notice('Setting /LocalSite/LocalSE =', self.localSE)
            self.localCfg.addDefaultEntry('/LocalSite/LocalSE', self.localSE)
          break

  if self.gatewayServer:
    DIRAC.gLogger.verbose('/DIRAC/Gateways/%s =' % DIRAC.siteName(), self.gatewayServer)
    self.localCfg.addDefaultEntry('/DIRAC/Gateways/%s' % DIRAC.siteName(), self.gatewayServer)

  # Create the local cfg if it is not yet there
  if not self.outputFile:
    self.outputFile = DIRAC.gConfig.diracConfigFilePath
  self.outputFile = os.path.abspath(self.outputFile)
  if not os.path.exists(self.outputFile):
    configDir = os.path.dirname(self.outputFile)
    mkDir(configDir)
    self.update = True
    DIRAC.gConfig.dumpLocalCFGToFile(self.outputFile)

  if self.includeAllServers:
    # We need user proxy or server certificate to continue in order to get all the CS URLs
    if not self.useServerCert:
      self.enableCS()
      result = getProxyInfo()
      if not result['OK']:
        DIRAC.gLogger.notice('Configuration is not completed because no user proxy is available')
        DIRAC.gLogger.notice('Create one using dirac-proxy-init and execute again with -F option')
        sys.exit(1)
    else:
      self.localCfg.deleteOption('/DIRAC/Security/UseServerCertificate')
      # When using Server Certs CA's will be checked, the flag only disables initial download
      # this will be replaced by the use of SkipCADownload
      self.localCfg.addDefaultEntry('/DIRAC/Security/UseServerCertificate', 'yes')
      self.enableCS()

    DIRAC.gConfig.setOptionValue('/DIRAC/Configuration/Servers', ','.join(DIRAC.gConfig.getServersList()))
    DIRAC.gLogger.verbose('/DIRAC/Configuration/Servers =', ','.join(DIRAC.gConfig.getServersList()))

  if self.useServerCert:
    # always removing before dumping
    self.localCfg.deleteOption('/DIRAC/Security/UseServerCertificate')
    self.localCfg.deleteOption('/DIRAC/Security/SkipCAChecks')
    self.localCfg.deleteOption('/DIRAC/Security/SkipVOMSDownload')

  if self.update:
    DIRAC.gConfig.dumpLocalCFGToFile(self.outputFile)

  # ## LAST PART: do the vomsdir/vomses magic

  # This has to be done for all VOs in the installation

  if self.skipVOMSDownload:
    # We stop here
    sys.exit(0)

  result = Registry.getVOMSServerInfo()
  if not result['OK']:
    sys.exit(1)

  error = ''
  vomsDict = result['Value']
  for vo in vomsDict:
    voName = vomsDict[vo]['VOMSName']
    vomsDirPath = os.path.join(DIRAC.rootPath, 'etc', 'grid-security', 'vomsdir', voName)
    vomsesDirPath = os.path.join(DIRAC.rootPath, 'etc', 'grid-security', 'vomses')
    for path in (vomsDirPath, vomsesDirPath):
      mkDir(path)
    vomsesLines = []
    for vomsHost in vomsDict[vo].get('Servers', {}):
      hostFilePath = os.path.join(vomsDirPath, "%s.lsc" % vomsHost)
      try:
        DN = vomsDict[vo]['Servers'][vomsHost]['DN']
        CA = vomsDict[vo]['Servers'][vomsHost]['CA']
        port = vomsDict[vo]['Servers'][vomsHost]['Port']
        if not DN or not CA or not port:
          DIRAC.gLogger.error('DN = %s' % DN)
          DIRAC.gLogger.error('CA = %s' % CA)
          DIRAC.gLogger.error('Port = %s' % port)
          DIRAC.gLogger.error('Missing Parameter for %s' % vomsHost)
          continue
        with open(hostFilePath, "wt") as fd:
          fd.write("%s\n%s\n" % (DN, CA))
        vomsesLines.append('"%s" "%s" "%s" "%s" "%s" "24"' % (voName, vomsHost, port, DN, voName))
        DIRAC.gLogger.notice("Created vomsdir file %s" % hostFilePath)
      except Exception:
        DIRAC.gLogger.exception("Could not generate vomsdir file for host", vomsHost)
        error = "Could not generate vomsdir file for VO %s, host %s" % (voName, vomsHost)
    try:
      vomsesFilePath = os.path.join(vomsesDirPath, voName)
      with open(vomsesFilePath, "wt") as fd:
        fd.write("%s\n" % "\n".join(vomsesLines))
      DIRAC.gLogger.notice("Created vomses file %s" % vomsesFilePath)
    except Exception:
      DIRAC.gLogger.exception("Could not generate vomses file")
      error = "Could not generate vomses file for VO %s" % voName

  if self.useServerCert:
    self.localCfg.deleteOption('/DIRAC/Security/UseServerCertificate')
    # When using Server Certs CA's will be checked, the flag only disables initial download
    # this will be replaced by the use of SkipCADownload
    self.localCfg.deleteOption('/DIRAC/Security/SkipCAChecks')

  if error:
    sys.exit(1)

  sys.exit(0)


if __name__ == "__main__":
  main()  # pylint: disable=no-value-for-parameter
