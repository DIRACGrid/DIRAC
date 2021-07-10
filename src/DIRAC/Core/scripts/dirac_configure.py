#!/usr/bin/env python
########################################################################
# File :    dirac-configure
# Author :  Ricardo Graciani
########################################################################
"""
  Main script to write dirac.cfg for a new DIRAC installation and initial download of CAs and CRLs
  if necessary.

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

  As in any other script command line option take precedence over .cfg files passed as arguments.
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
import sys
import os

import six

import DIRAC
from DIRAC.Core.Utilities.File import mkDir
from DIRAC.Core.Utilities.DIRACScript import DIRACScript
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.ConfigurationSystem.Client.Helpers import cfgInstallPath, cfgPath, Registry
from DIRAC.Core.Utilities.SiteSEMapping import getSEsForSite
from DIRAC.FrameworkSystem.Client.BundleDeliveryClient import BundleDeliveryClient

__RCSID__ = "$Id$"


class Params(DIRACScript):

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

  def setLocalSE(self, optionValue):
    self.localSE = optionValue
    self.localCfg.addDefaultEntry('/LocalSite/LocalSE', self.localSE)
    DIRAC.gConfig.setOptionValue(cfgInstallPath('LocalSE'), self.localSE)
    return DIRAC.S_OK()

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

  # Apply the arguments to the params object
  setup, csURL = result
  params.setSetup(setup)
  params.setServer(csURL)
  params.setSkipCAChecks(True)

  # Do the actual configuration
  runDiracConfigure(params)

  # Generate a new proxy without passing --nocs
  result = subprocess.run(  # pylint: disable=no-member
      ["dirac-proxy-init", "--pwstdin"],
      input=userPasswd,
      encoding="utf-8",
      check=False,
  )
  sys.exit(result.returncode)


@Params()
def main(self):
  self.disableCS()
  if six.PY3 and len(sys.argv) < 2:
    runConfigurationWizard(self)
  else:
    runDiracConfigure(self)


def runDiracConfigure(params):
  params.registerSwitches(params.switches)
  params.parseCommandLine(ignoreErrors=True)

  if not params.logLevel:
    params.logLevel = DIRAC.gConfig.getValue(cfgInstallPath('LogLevel'), '')
    if params.logLevel:
      DIRAC.gLogger.setLevel(params.logLevel)
  else:
    DIRAC.gConfig.setOptionValue(cfgInstallPath('LogLevel'), params.logLevel)

  if not params.gatewayServer:
    newGatewayServer = DIRAC.gConfig.getValue(cfgInstallPath('Gateway'), '')
    if newGatewayServer:
      params.setGateway(newGatewayServer)

  if not params.configurationServer:
    newConfigurationServer = DIRAC.gConfig.getValue(cfgInstallPath('ConfigurationServer'), '')
    if newConfigurationServer:
      params.setServer(newConfigurationServer)

  if not params.includeAllServers:
    newIncludeAllServer = DIRAC.gConfig.getValue(cfgInstallPath('IncludeAllServers'), False)
    if newIncludeAllServer:
      params.setAllServers(True)

  if not params.setup:
    newSetup = DIRAC.gConfig.getValue(cfgInstallPath('Setup'), '')
    if newSetup:
      params.setSetup(newSetup)

  if not params.siteName:
    newSiteName = DIRAC.gConfig.getValue(cfgInstallPath('SiteName'), '')
    if newSiteName:
      params.setSiteName(newSiteName)

  if not params.ceName:
    newCEName = DIRAC.gConfig.getValue(cfgInstallPath('CEName'), '')
    if newCEName:
      params.setCEName(newCEName)

  if not params.useServerCert:
    newUserServerCert = DIRAC.gConfig.getValue(cfgInstallPath('UseServerCertificate'), False)
    if newUserServerCert:
      params.setServerCert(newUserServerCert)

  if not params.skipCAChecks:
    newSkipCAChecks = DIRAC.gConfig.getValue(cfgInstallPath('SkipCAChecks'), False)
    if newSkipCAChecks:
      params.setSkipCAChecks(newSkipCAChecks)

  if not params.skipCADownload:
    newSkipCADownload = DIRAC.gConfig.getValue(cfgInstallPath('SkipCADownload'), False)
    if newSkipCADownload:
      params.setSkipCADownload(newSkipCADownload)

  if not params.useVersionsDir:
    newUseVersionsDir = DIRAC.gConfig.getValue(cfgInstallPath('UseVersionsDir'), False)
    if newUseVersionsDir:
      params.setUseVersionsDir(newUseVersionsDir)
      # Set proper Defaults in configuration (even if they will be properly overwrite by gComponentInstaller
      instancePath = os.path.dirname(os.path.dirname(DIRAC.rootPath))
      rootPath = os.path.join(instancePath, 'pro')
      DIRAC.gConfig.setOptionValue(cfgInstallPath('InstancePath'), instancePath)
      DIRAC.gConfig.setOptionValue(cfgInstallPath('RootPath'), rootPath)

  if not params.architecture:
    newArchitecture = DIRAC.gConfig.getValue(cfgInstallPath('Architecture'), '')
    if newArchitecture:
      params.setArchitecture(newArchitecture)

  if not params.vo:
    newVO = DIRAC.gConfig.getValue(cfgInstallPath('VirtualOrganization'), '')
    if newVO:
      params.setVO(newVO)

  if not params.extensions:
    newExtensions = DIRAC.gConfig.getValue(cfgInstallPath('Extensions'), '')
    if newExtensions:
      params.setExtensions(newExtensions)

  DIRAC.gLogger.notice('Executing: %s ' % (' '.join(sys.argv)))
  DIRAC.gLogger.notice('Checking DIRAC installation at "%s"' % DIRAC.rootPath)

  if params.update:
    if params.outputFile:
      DIRAC.gLogger.notice('Will update the output file %s' % params.outputFile)
    else:
      DIRAC.gLogger.notice('Will update %s' % DIRAC.gConfig.diracConfigFilePath)

  if params.setup:
    DIRAC.gLogger.verbose('/DIRAC/Setup =', params.setup)
  if params.vo:
    DIRAC.gLogger.verbose('/DIRAC/VirtualOrganization =', params.vo)
  if params.configurationServer:
    DIRAC.gLogger.verbose('/DIRAC/Configuration/Servers =', params.configurationServer)

  if params.siteName:
    DIRAC.gLogger.verbose('/LocalSite/Site =', params.siteName)
  if params.architecture:
    DIRAC.gLogger.verbose('/LocalSite/Architecture =', params.architecture)
  if params.localSE:
    DIRAC.gLogger.verbose('/LocalSite/localSE =', params.localSE)

  if not params.useServerCert:
    DIRAC.gLogger.verbose('/DIRAC/Security/UseServerCertificate =', 'no')
    # Being sure it was not there before
    params.localCfg.deleteOption('/DIRAC/Security/UseServerCertificate')
    params.localCfg.addDefaultEntry('/DIRAC/Security/UseServerCertificate', 'no')
  else:
    DIRAC.gLogger.verbose('/DIRAC/Security/UseServerCertificate =', 'yes')
    # Being sure it was not there before
    params.localCfg.deleteOption('/DIRAC/Security/UseServerCertificate')
    params.localCfg.addDefaultEntry('/DIRAC/Security/UseServerCertificate', 'yes')

  host = DIRAC.gConfig.getValue(cfgInstallPath("Host"), "")
  if host:
    DIRAC.gConfig.setOptionValue(cfgPath("DIRAC", "Hostname"), host)

  if params.skipCAChecks:
    DIRAC.gLogger.verbose('/DIRAC/Security/SkipCAChecks =', 'yes')
    # Being sure it was not there before
    params.localCfg.deleteOption('/DIRAC/Security/SkipCAChecks')
    params.localCfg.addDefaultEntry('/DIRAC/Security/SkipCAChecks', 'yes')
  else:
    # Necessary to allow initial download of CA's
    if not params.skipCADownload:
      DIRAC.gConfig.setOptionValue('/DIRAC/Security/SkipCAChecks', 'yes')
  if not params.skipCADownload:
    params.enableCS()
    try:
      dirName = os.path.join(DIRAC.rootPath, 'etc', 'grid-security', 'certificates')
      mkDir(dirName)
    except Exception:
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

    if not params.skipCAChecks:
      params.localCfg.deleteOption('/DIRAC/Security/SkipCAChecks')

  if params.ceName or params.siteName:
    # This is used in the pilot context, we should have a proxy, or a certificate, and access to CS
    if params.useServerCert:
      # Being sure it was not there before
      params.localCfg.deleteOption('/DIRAC/Security/UseServerCertificate')
      params.localCfg.addDefaultEntry('/DIRAC/Security/UseServerCertificate', 'yes')
    params.enableCS()
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

      if not params.siteName:
        if params.ceName:
          for site in sites:
            res = DIRAC.gConfig.getSections('/Resources/Sites/%s/%s/CEs/' % (grid, site), [])
            if not res['OK']:
              DIRAC.gLogger.warn('Could not get %s CEs list' % site)
            if params.ceName in res['Value']:
              params.siteName = site
              break
      if params.siteName:
        DIRAC.gLogger.notice('Setting /LocalSite/Site = %s' % params.siteName)
        params.localCfg.addDefaultEntry('/LocalSite/Site', params.siteName)
        DIRAC.__siteName = False
        if params.ceName:
          DIRAC.gLogger.notice('Setting /LocalSite/GridCE = %s' % params.ceName)
          params.localCfg.addDefaultEntry('/LocalSite/GridCE', params.ceName)

        if not params.localSE and params.siteName in sites:
          params.localSE = getSEsForSite(params.siteName)
          if params.localSE['OK'] and params.localSE['Value']:
            params.localSE = ','.join(params.localSE['Value'])
            DIRAC.gLogger.notice('Setting /LocalSite/LocalSE =', params.localSE)
            params.localCfg.addDefaultEntry('/LocalSite/LocalSE', params.localSE)
          break

  if params.gatewayServer:
    DIRAC.gLogger.verbose('/DIRAC/Gateways/%s =' % DIRAC.siteName(), params.gatewayServer)
    params.localCfg.addDefaultEntry('/DIRAC/Gateways/%s' % DIRAC.siteName(), params.gatewayServer)

  # Create the local cfg if it is not yet there
  if not params.outputFile:
    params.outputFile = DIRAC.gConfig.diracConfigFilePath
  params.outputFile = os.path.abspath(params.outputFile)
  if not os.path.exists(params.outputFile):
    configDir = os.path.dirname(params.outputFile)
    mkDir(configDir)
    params.update = True
    DIRAC.gConfig.dumpLocalCFGToFile(params.outputFile)

  if params.includeAllServers:
    # We need user proxy or server certificate to continue in order to get all the CS URLs
    if not params.useServerCert:
      params.enableCS()
      result = getProxyInfo()
      if not result['OK']:
        DIRAC.gLogger.notice('Configuration is not completed because no user proxy is available')
        DIRAC.gLogger.notice('Create one using dirac-proxy-init and execute again with -F option')
        sys.exit(1)
    else:
      params.localCfg.deleteOption('/DIRAC/Security/UseServerCertificate')
      # When using Server Certs CA's will be checked, the flag only disables initial download
      # this will be replaced by the use of SkipCADownload
      params.localCfg.addDefaultEntry('/DIRAC/Security/UseServerCertificate', 'yes')
      params.enableCS()

    DIRAC.gConfig.setOptionValue('/DIRAC/Configuration/Servers', ','.join(DIRAC.gConfig.getServersList()))
    DIRAC.gLogger.verbose('/DIRAC/Configuration/Servers =', ','.join(DIRAC.gConfig.getServersList()))

  if params.useServerCert:
    # always removing before dumping
    params.localCfg.deleteOption('/DIRAC/Security/UseServerCertificate')
    params.localCfg.deleteOption('/DIRAC/Security/SkipCAChecks')
    params.localCfg.deleteOption('/DIRAC/Security/SkipVOMSDownload')

  if params.update:
    DIRAC.gConfig.dumpLocalCFGToFile(params.outputFile)

  # ## LAST PART: do the vomsdir/vomses magic

  # This has to be done for all VOs in the installation

  if params.skipVOMSDownload:
    return

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

  if params.useServerCert:
    params.localCfg.deleteOption('/DIRAC/Security/UseServerCertificate')
    # When using Server Certs CA's will be checked, the flag only disables initial download
    # this will be replaced by the use of SkipCADownload
    params.localCfg.deleteOption('/DIRAC/Security/SkipCAChecks')

  if error:
    sys.exit(1)


if __name__ == "__main__":
  main()  # pylint: disable=no-value-for-parameter
