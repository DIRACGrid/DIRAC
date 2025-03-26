#!/usr/bin/env python
########################################################################
# File :    dirac-configure
# Author :  Ricardo Graciani
########################################################################
"""
  Main script to write dirac.cfg for a new DIRAC installation and initial download of CAs and CRLs
  if necessary.

  To be used by VO specific scripts to configure new DIRAC installations.

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
import os
import sys
import warnings

import urllib3

import DIRAC
from DIRAC.ConfigurationSystem.Client.Helpers import Registry, cfgInstallPath, cfgPath
from DIRAC.Core.Base.Script import Script
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.Core.Utilities.File import mkDir
from DIRAC.Core.Utilities.SiteSEMapping import getSEsForSite
from DIRAC.FrameworkSystem.Client.BundleDeliveryClient import BundleDeliveryClient


class Params:
    def __init__(self):
        self.issuer = None
        self.logLevel = None
        self.setup = None
        self.configurationServer = None
        self.includeAllServers = False
        self.gatewayServer = None
        self.siteName = None
        self.useServerCert = False
        self.skipCAChecks = False
        self.skipCADownload = False
        self.useVersionsDir = False
        self.architecture = None
        self.localSE = None
        self.ceName = None
        self.vo = None
        self.update = False
        self.outputFile = ""
        self.skipVOMSDownload = False
        self.extensions = ""
        self.legacyExchangeApiKey = ""

    def setGateway(self, optionValue):
        self.gatewayServer = optionValue
        self.setServer(self.gatewayServer + "/Configuration/Server")
        DIRAC.gConfig.setOptionValue(cfgInstallPath("Gateway"), self.gatewayServer)
        return DIRAC.S_OK()

    def setOutput(self, optionValue):
        self.outputFile = optionValue
        return DIRAC.S_OK()

    def setServer(self, optionValue):
        self.configurationServer = optionValue
        DIRAC.gConfig.setOptionValue("/DIRAC/Configuration/Servers", self.configurationServer)
        DIRAC.gConfig.setOptionValue(cfgInstallPath("ConfigurationServer"), self.configurationServer)
        return DIRAC.S_OK()

    def setAllServers(self, optionValue):
        self.includeAllServers = True

    def setSetup(self, optionValue):
        DIRAC.gLogger.warn("Ignoring Setup option as it is not used for DIRAC v9.0+")
        return DIRAC.S_OK()

    def setSiteName(self, optionValue):
        self.siteName = optionValue
        Script.localCfg.addDefaultEntry("/LocalSite/Site", self.siteName)
        DIRAC.__siteName = False
        DIRAC.gConfig.setOptionValue(cfgInstallPath("SiteName"), self.siteName)
        return DIRAC.S_OK()

    def setCEName(self, optionValue):
        self.ceName = optionValue
        DIRAC.gConfig.setOptionValue(cfgInstallPath("CEName"), self.ceName)
        return DIRAC.S_OK()

    def setServerCert(self, optionValue):
        self.useServerCert = True
        DIRAC.gConfig.setOptionValue(cfgInstallPath("UseServerCertificate"), self.useServerCert)
        return DIRAC.S_OK()

    def setSkipCAChecks(self, optionValue):
        self.skipCAChecks = True
        DIRAC.gConfig.setOptionValue(cfgInstallPath("SkipCAChecks"), self.skipCAChecks)
        return DIRAC.S_OK()

    def setSkipCADownload(self, optionValue):
        self.skipCADownload = True
        DIRAC.gConfig.setOptionValue(cfgInstallPath("SkipCADownload"), self.skipCADownload)
        return DIRAC.S_OK()

    def setSkipVOMSDownload(self, optionValue):
        self.skipVOMSDownload = True
        DIRAC.gConfig.setOptionValue(cfgInstallPath("SkipVOMSDownload"), self.skipVOMSDownload)
        return DIRAC.S_OK()

    def setArchitecture(self, optionValue):
        self.architecture = optionValue
        Script.localCfg.addDefaultEntry("/LocalSite/Architecture", self.architecture)
        DIRAC.gConfig.setOptionValue(cfgInstallPath("Architecture"), self.architecture)
        return DIRAC.S_OK()

    def setLocalSE(self, optionValue):
        self.localSE = optionValue
        Script.localCfg.addDefaultEntry("/LocalSite/LocalSE", self.localSE)
        DIRAC.gConfig.setOptionValue(cfgInstallPath("LocalSE"), self.localSE)
        return DIRAC.S_OK()

    def setVO(self, optionValue):
        self.vo = optionValue
        Script.localCfg.addDefaultEntry("/DIRAC/VirtualOrganization", self.vo)
        DIRAC.gConfig.setOptionValue(cfgInstallPath("VirtualOrganization"), self.vo)
        return DIRAC.S_OK()

    def forceUpdate(self, optionValue):
        self.update = True
        return DIRAC.S_OK()

    def setExtensions(self, optionValue):
        self.extensions = optionValue
        DIRAC.gConfig.setOptionValue("/DIRAC/Extensions", self.extensions)
        DIRAC.gConfig.setOptionValue(cfgInstallPath("Extensions"), self.extensions)
        return DIRAC.S_OK()

    def setIssuer(self, optionValue):
        # If the user selects the issuer, it means that there will be authorization through tokens
        os.environ["DIRAC_USE_ACCESS_TOKEN"] = "True"
        # Allow the user to enter only the server domain
        if not optionValue.startswith(("http://", "https://")):
            optionValue = f"https://{optionValue.lstrip('/')}"
        if len(optionValue.strip("/").split("/")) == 3:
            optionValue = f"{optionValue.rstrip('/')}/auth"
        self.issuer = optionValue
        DIRAC.gConfig.setOptionValue("/DIRAC/Security/Authorization/issuer", self.issuer)
        return DIRAC.S_OK()

    def setLegacyExchangeApiKey(self, optionValue):
        self.legacyExchangeApiKey = optionValue
        Script.localCfg.addDefaultEntry("/DiracX/LegacyExchangeApiKey", self.legacyExchangeApiKey)
        DIRAC.gConfig.setOptionValue(cfgInstallPath("LegacyExchangeApiKey"), self.legacyExchangeApiKey)
        return DIRAC.S_OK()

    def setDiracxUrl(self, optionValue):
        self.diracxUrl = optionValue
        Script.localCfg.addDefaultEntry("/DiracX/URL", self.diracxUrl)
        DIRAC.gConfig.setOptionValue(cfgInstallPath("URL"), self.diracxUrl)
        return DIRAC.S_OK()


def _runConfigurationWizard(setups, defaultSetup):
    """The implementation of the configuration wizard"""
    from prompt_toolkit import HTML, print_formatted_text, prompt
    from prompt_toolkit.completion import FuzzyWordCompleter

    # It makes no sense to have suggestions if there is no default so adjust the message accordingly
    msg = ""
    if setups:
        msg = "press tab for suggestions"
        if defaultSetup:
            msg = f"default</b> <green>{defaultSetup}</green><b>, {msg}"
        msg = f" ({msg})"
    # Get the Setup
    setup = prompt(
        HTML(f"<b>Choose a DIRAC Setup{msg}:</b>\n"),
        completer=FuzzyWordCompleter(list(setups)),
    )
    if defaultSetup and not setup:
        setup = defaultSetup
    if setup not in setups:
        print_formatted_text(HTML(f"Unknown setup <yellow>{setup}</yellow> chosen"))
        confirm = prompt(HTML("<b>Are you sure you want to continue?</b> "), default="n")
        if confirm.lower() not in ["y", "yes"]:
            return None

    # Get the URL to the controller CS
    csURL = prompt(HTML("<b>Choose a configuration server URL (leave blank for default):</b>\n"))
    if not csURL:
        csURL = setups[setup]

    # Confirm
    print_formatted_text(
        HTML(
            "<b>Configuration is:</b>\n"
            + f"  * <b>Setup:</b> <green>{setup}</green>\n"
            + f"  * <b>Configuration server:</b> <green>{csURL}</green>\n"
        )
    )
    confirm = prompt(HTML("<b>Are you sure you want to continue?</b> "), default="y")
    if confirm.lower() in ["y", "yes"]:
        return setup, csURL


def runConfigurationWizard(params):
    """Interactively configure DIRAC using metadata from installed extensions"""
    import subprocess

    from prompt_toolkit import HTML, print_formatted_text, prompt

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
            userPasswd = prompt("Enter Certificate password: ", is_password=True)
            result = subprocess.run(  # pylint: disable=no-member
                ["dirac-proxy-init", "--nocs", "--no-upload", "--pwstdin"],
                input=userPasswd,
                encoding="utf-8",
                check=False,
            )
            if result.returncode == 0:
                break
            print_formatted_text(HTML("<red>Wizard failed, retrying...</red> (press Control + C to exit)\n"))

        print_formatted_text()

        # Ask the user for the appropriate configuration settings
        while True:
            result = _runConfigurationWizard(setups, defaultSetup)
            if result:
                break
            print_formatted_text(HTML("<red>Wizard failed, retrying...</red> (press Control + C to exit)\n"))
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


@Script()
def main():
    Script.disableCS()
    params = Params()
    if len(sys.argv) < 2:
        runConfigurationWizard(params)
    else:
        return runDiracConfigure(params)


def login(params):
    from prompt_toolkit import HTML, print_formatted_text, prompt

    from DIRAC.FrameworkSystem.private.authorization.utils.Tokens import (
        getTokenFileLocation,
        readTokenFromFile,
        writeTokenDictToTokenFile,
    )
    from DIRAC.Resources.IdProvider.IdProviderFactory import IdProviderFactory

    # Init authorization client
    result = IdProviderFactory().getIdProvider("DIRACCLI", issuer=params.issuer, scope=" ")
    if not result["OK"]:
        return result
    idpObj = result["Value"]

    # Get token file path
    tokenFile = getTokenFileLocation()

    # Submit Device authorisation flow
    if not (result := idpObj.deviceAuthorization())["OK"]:
        return result

    # Revoke old tokens from token file
    if os.path.isfile(tokenFile):
        if not (result := readTokenFromFile(tokenFile))["OK"]:
            DIRAC.gLogger.warn(result["Message"])
        elif result["Value"]:
            oldToken = result["Value"]
            for tokenType in ["access_token", "refresh_token"]:
                result = idpObj.revokeToken(oldToken[tokenType], tokenType)
                if result["OK"]:
                    DIRAC.gLogger.debug(f"{tokenType} is revoked from", tokenFile)
                else:
                    DIRAC.gLogger.warn(result["Message"])

    # Save new tokens to token file
    if not (result := writeTokenDictToTokenFile(idpObj.token, tokenFile))["OK"]:
        return result
    DIRAC.gLogger.debug(f"New token is saved to {result['Value']}.")

    # Get server setups and controller CS server URL
    csURL = idpObj.get_metadata("configuration_server")
    setups = idpObj.get_metadata("setups")

    if len(setups) == 1 and csURL:
        # If setup only one dont ask user
        params.setSetup(setups[0])
        params.setServer(csURL)
    elif setups and csURL:
        # Ask the user for the appropriate configuration settings
        while True:
            result = _runConfigurationWizard({setup: csURL for setup in setups}, setups[0])
            if result:
                break
            print_formatted_text(HTML("<red>Wizard failed, retrying...</red> (press Control + C to exit)\n"))
        # Apply the arguments to the params object
        setup, csURL = result
        params.setSetup(setup)
        params.setServer(csURL)

    confirm = prompt(HTML("<b>Do you want to use tokens instead of certificates by default?</b> "), default="no")
    return DIRAC.S_OK("yes") if confirm.lower() in ["y", "yes"] else DIRAC.S_OK(None)


def runDiracConfigure(params):
    Script.registerSwitch("", "login=", "Set DIRAC authorization endpoint", params.setIssuer)
    Script.registerSwitch("S:", "Setup=", "Set <setup> as DIRAC setup", params.setSetup)
    Script.registerSwitch("e:", "Extensions=", "Set <extensions> as DIRAC extensions", params.setExtensions)
    Script.registerSwitch("C:", "ConfigurationServer=", "Set <server> as DIRAC configuration server", params.setServer)
    Script.registerSwitch("I", "IncludeAllServers", "include all Configuration Servers", params.setAllServers)
    Script.registerSwitch("n:", "SiteName=", "Set <sitename> as DIRAC Site Name", params.setSiteName)
    Script.registerSwitch("N:", "CEName=", "Set <cename> as Computing Element name", params.setCEName)
    Script.registerSwitch("V:", "VO=", "Set the VO name", params.setVO)
    Script.registerSwitch(
        "K:", "LegacyExchangeApiKey=", "Set the Api Key to talk to DiracX", params.setLegacyExchangeApiKey
    )
    Script.registerSwitch("", "DiracxUrl=", "Set the URL to talk to DiracX", params.setDiracxUrl)

    Script.registerSwitch("W:", "gateway=", "Configure <gateway> as DIRAC Gateway for the site", params.setGateway)

    Script.registerSwitch("U", "UseServerCertificate", "Configure to use Server Certificate", params.setServerCert)
    Script.registerSwitch("H", "SkipCAChecks", "Configure to skip check of CAs", params.setSkipCAChecks)
    Script.registerSwitch("D", "SkipCADownload", "Configure to skip download of CAs", params.setSkipCADownload)
    Script.registerSwitch(
        "M", "SkipVOMSDownload", "Configure to skip download of VOMS info", params.setSkipVOMSDownload
    )

    Script.registerSwitch("A:", "Architecture=", "Configure /Architecture=<architecture>", params.setArchitecture)
    Script.registerSwitch("L:", "LocalSE=", "Configure LocalSite/LocalSE=<localse>", params.setLocalSE)

    Script.registerSwitch(
        "F",
        "ForceUpdate",
        "Force Update of cfg file (i.e. dirac.cfg) (otherwise nothing happens if dirac.cfg already exists)",
        params.forceUpdate,
    )

    Script.registerSwitch("O:", "output=", "output configuration file", params.setOutput)

    Script.parseCommandLine(ignoreErrors=True)

    # Use token auth
    if params.issuer:
        result = login(params)
        if not result["OK"]:
            DIRAC.gLogger.error(f"Authorization failed: {result['Message']}")
            DIRAC.exit(1)
        useTokens = result["Value"]
        if useTokens:
            DIRAC.gConfig.setOptionValue("/DIRAC/Security/UseTokens", useTokens)
        else:
            DIRAC.gLogger.notice('To use tokens, please, set "/DIRAC/Security/UseTokens=yes".')

    if not params.logLevel:
        params.logLevel = DIRAC.gConfig.getValue(cfgInstallPath("LogLevel"), "")
        if params.logLevel:
            DIRAC.gLogger.setLevel(params.logLevel)
    else:
        DIRAC.gConfig.setOptionValue(cfgInstallPath("LogLevel"), params.logLevel)

    if not params.gatewayServer:
        newGatewayServer = DIRAC.gConfig.getValue(cfgInstallPath("Gateway"), "")
        if newGatewayServer:
            params.setGateway(newGatewayServer)

    if not params.configurationServer:
        newConfigurationServer = DIRAC.gConfig.getValue(cfgInstallPath("ConfigurationServer"), "")
        if newConfigurationServer:
            params.setServer(newConfigurationServer)

    if not params.includeAllServers:
        newIncludeAllServer = DIRAC.gConfig.getValue(cfgInstallPath("IncludeAllServers"), False)
        if newIncludeAllServer:
            params.setAllServers(True)

    if not params.setup:
        newSetup = DIRAC.gConfig.getValue(cfgInstallPath("Setup"), "")
        if newSetup:
            params.setSetup(newSetup)

    if not params.siteName:
        newSiteName = DIRAC.gConfig.getValue(cfgInstallPath("SiteName"), "")
        if newSiteName:
            params.setSiteName(newSiteName)

    if not params.ceName:
        newCEName = DIRAC.gConfig.getValue(cfgInstallPath("CEName"), "")
        if newCEName:
            params.setCEName(newCEName)

    if not params.useServerCert:
        newUserServerCert = DIRAC.gConfig.getValue(cfgInstallPath("UseServerCertificate"), False)
        if newUserServerCert:
            params.setServerCert(newUserServerCert)

    if not params.skipCAChecks:
        newSkipCAChecks = DIRAC.gConfig.getValue(cfgInstallPath("SkipCAChecks"), False)
        if newSkipCAChecks:
            params.setSkipCAChecks(newSkipCAChecks)

    if not params.skipCADownload:
        newSkipCADownload = DIRAC.gConfig.getValue(cfgInstallPath("SkipCADownload"), False)
        if newSkipCADownload:
            params.setSkipCADownload(newSkipCADownload)

    if not params.architecture:
        newArchitecture = DIRAC.gConfig.getValue(cfgInstallPath("Architecture"), "")
        if newArchitecture:
            params.setArchitecture(newArchitecture)

    if not params.vo:
        newVO = DIRAC.gConfig.getValue(cfgInstallPath("VirtualOrganization"), "")
        if newVO:
            params.setVO(newVO)

    if not params.extensions:
        newExtensions = DIRAC.gConfig.getValue(cfgInstallPath("Extensions"), "")
        if newExtensions:
            params.setExtensions(newExtensions)

    DIRAC.gLogger.notice(f"Executing: {' '.join(sys.argv)} ")
    DIRAC.gLogger.notice(f'Checking DIRAC installation at "{DIRAC.rootPath}"')

    if params.update:
        if params.outputFile:
            DIRAC.gLogger.notice(f"Will update the output file {params.outputFile}")
        else:
            DIRAC.gLogger.notice(f"Will update {DIRAC.gConfig.diracConfigFilePath}")

    if params.vo:
        DIRAC.gLogger.verbose("/DIRAC/VirtualOrganization =", params.vo)
    if params.configurationServer:
        DIRAC.gLogger.verbose("/DIRAC/Configuration/Servers =", params.configurationServer)

    if params.siteName:
        DIRAC.gLogger.verbose("/LocalSite/Site =", params.siteName)
    if params.architecture:
        DIRAC.gLogger.verbose("/LocalSite/Architecture =", params.architecture)
    if params.localSE:
        DIRAC.gLogger.verbose("/LocalSite/localSE =", params.localSE)

    if not params.useServerCert:
        DIRAC.gLogger.verbose("/DIRAC/Security/UseServerCertificate =", "no")
        # Being sure it was not there before
        Script.localCfg.deleteOption("/DIRAC/Security/UseServerCertificate")
        Script.localCfg.addDefaultEntry("/DIRAC/Security/UseServerCertificate", "no")
    else:
        DIRAC.gLogger.verbose("/DIRAC/Security/UseServerCertificate =", "yes")
        # Being sure it was not there before
        Script.localCfg.deleteOption("/DIRAC/Security/UseServerCertificate")
        Script.localCfg.addDefaultEntry("/DIRAC/Security/UseServerCertificate", "yes")

    host = DIRAC.gConfig.getValue(cfgInstallPath("Host"), "")
    if host:
        DIRAC.gConfig.setOptionValue(cfgPath("DIRAC", "Hostname"), host)

    if params.skipCAChecks:
        DIRAC.gLogger.verbose("/DIRAC/Security/SkipCAChecks =", "yes")
        # Being sure it was not there before
        Script.localCfg.deleteOption("/DIRAC/Security/SkipCAChecks")
        Script.localCfg.addDefaultEntry("/DIRAC/Security/SkipCAChecks", "yes")
    else:
        # Necessary to allow initial download of CA's
        if not params.skipCADownload:
            DIRAC.gConfig.setOptionValue("/DIRAC/Security/SkipCAChecks", "yes")
    if not params.skipCADownload:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", urllib3.exceptions.InsecureRequestWarning)
            Script.enableCS()
            try:
                dirName = os.path.join(DIRAC.rootPath, "etc", "grid-security", "certificates")
                mkDir(dirName, 0o755)
            except Exception:
                DIRAC.gLogger.exception()
                DIRAC.gLogger.fatal("Fail to create directory:", dirName)
                DIRAC.exit(-1)
            try:
                bdc = BundleDeliveryClient()
                result = bdc.syncCAs()
                if result["OK"]:
                    result = bdc.syncCRLs()
                if not result["OK"]:
                    DIRAC.gLogger.error("Failed to sync CAs and/or CRLs", result["Message"])
            except Exception as e:
                DIRAC.gLogger.error(f"Failed to sync CAs and CRLs: {str(e)}")

        Script.localCfg.deleteOption("/DIRAC/Security/SkipCAChecks")

    if params.ceName or params.siteName:
        # This is used in the pilot context, we should have a proxy, or a certificate, and access to CS
        if params.useServerCert:
            # Being sure it was not there before
            Script.localCfg.deleteOption("/DIRAC/Security/UseServerCertificate")
            Script.localCfg.addDefaultEntry("/DIRAC/Security/UseServerCertificate", "yes")
        Script.enableCS()
        # Get the site resource section
        gridSections = DIRAC.gConfig.getSections("/Resources/Sites/")
        if not gridSections["OK"]:
            DIRAC.gLogger.warn("Could not get grid sections list")
            grids = []
        else:
            grids = gridSections["Value"]
        # try to get siteName from ceName or Local SE from siteName using Remote Configuration
        for grid in grids:
            siteSections = DIRAC.gConfig.getSections(f"/Resources/Sites/{grid}/")
            if not siteSections["OK"]:
                DIRAC.gLogger.warn(f"Could not get {grid} site list")
                sites = []
            else:
                sites = siteSections["Value"]

            if not params.siteName:
                if params.ceName:
                    for site in sites:
                        res = DIRAC.gConfig.getSections(f"/Resources/Sites/{grid}/{site}/CEs/", [])
                        if not res["OK"]:
                            DIRAC.gLogger.warn(f"Could not get {site} CEs list")
                        if params.ceName in res["Value"]:
                            params.siteName = site
                            break
            if params.siteName:
                DIRAC.gLogger.notice(f"Setting /LocalSite/Site = {params.siteName}")
                Script.localCfg.addDefaultEntry("/LocalSite/Site", params.siteName)
                DIRAC.__siteName = False
                if params.ceName:
                    DIRAC.gLogger.notice(f"Setting /LocalSite/GridCE = {params.ceName}")
                    Script.localCfg.addDefaultEntry("/LocalSite/GridCE", params.ceName)

                if not params.localSE and params.siteName in sites:
                    params.localSE = getSEsForSite(params.siteName)
                    if params.localSE["OK"] and params.localSE["Value"]:
                        params.localSE = ",".join(params.localSE["Value"])
                        DIRAC.gLogger.notice("Setting /LocalSite/LocalSE =", params.localSE)
                        Script.localCfg.addDefaultEntry("/LocalSite/LocalSE", params.localSE)
                    break

    if params.gatewayServer:
        DIRAC.gLogger.verbose(f"/DIRAC/Gateways/{DIRAC.siteName()} =", params.gatewayServer)
        Script.localCfg.addDefaultEntry(f"/DIRAC/Gateways/{DIRAC.siteName()}", params.gatewayServer)

    # Create the local cfg if it is not yet there
    if not params.outputFile:
        params.outputFile = DIRAC.gConfig.diracConfigFilePath
    params.outputFile = os.path.abspath(params.outputFile)
    if not os.path.exists(params.outputFile):
        configDir = os.path.dirname(params.outputFile)
        mkDir(configDir)
        params.update = True
        DIRAC.gConfig.dumpLocalCFGToFile(params.outputFile)
    elif not params.forceUpdate:
        DIRAC.gLogger.notice(f"{params.outputFile} exists, not overwriting it. Or use the '--ForceUpdate' Flag")

    if params.includeAllServers:
        # We need user proxy or server certificate to continue in order to get all the CS URLs
        if not params.useServerCert:
            Script.enableCS()
            result = getProxyInfo()
            if not result["OK"]:
                DIRAC.gLogger.notice("Configuration is not completed because no user proxy is available")
                DIRAC.gLogger.notice("Create one using dirac-proxy-init and execute again with -F option")
                return 1
        else:
            Script.localCfg.deleteOption("/DIRAC/Security/UseServerCertificate")
            # When using Server Certs CA's will be checked, the flag only disables initial download
            # this will be replaced by the use of SkipCADownload
            Script.localCfg.addDefaultEntry("/DIRAC/Security/UseServerCertificate", "yes")
            Script.enableCS()

        DIRAC.gConfig.setOptionValue("/DIRAC/Configuration/Servers", ",".join(DIRAC.gConfig.getServersList()))
        DIRAC.gLogger.verbose("/DIRAC/Configuration/Servers =", ",".join(DIRAC.gConfig.getServersList()))

    if params.useServerCert:
        # always removing before dumping
        Script.localCfg.deleteOption("/DIRAC/Security/UseServerCertificate")
        Script.localCfg.deleteOption("/DIRAC/Security/SkipCAChecks")
        Script.localCfg.deleteOption("/DIRAC/Security/SkipVOMSDownload")

    if params.update:
        DIRAC.gConfig.dumpLocalCFGToFile(params.outputFile)

    # ## LAST PART: do the vomsdir/vomses magic

    # This has to be done for all VOs in the installation

    if params.skipVOMSDownload:
        return 0

    result = Registry.getVOMSServerInfo()
    if not result["OK"]:
        return 1

    error = ""
    vomsDict = result["Value"]
    for vo in vomsDict:
        voName = vomsDict[vo]["VOMSName"]
        vomsDirPath = os.path.join(DIRAC.rootPath, "etc", "grid-security", "vomsdir", voName)
        vomsesDirPath = os.path.join(DIRAC.rootPath, "etc", "grid-security", "vomses")
        for path in (vomsDirPath, vomsesDirPath):
            mkDir(path, 0o755)
        vomsesLines = []
        for vomsHost in vomsDict[vo].get("Servers", {}):
            hostFilePath = os.path.join(vomsDirPath, f"{vomsHost}.lsc")
            try:
                DN = vomsDict[vo]["Servers"][vomsHost]["DN"]
                CA = vomsDict[vo]["Servers"][vomsHost]["CA"]
                port = vomsDict[vo]["Servers"][vomsHost]["Port"]
                if not DN or not CA or not port:
                    DIRAC.gLogger.error(f"DN = {DN}")
                    DIRAC.gLogger.error(f"CA = {CA}")
                    DIRAC.gLogger.error(f"Port = {port}")
                    DIRAC.gLogger.error(f"Missing Parameter for {vomsHost}")
                    continue
                with open(hostFilePath, "w") as fd:
                    fd.write(f"{DN}\n{CA}\n")
                vomsesLines.append(f'"{voName}" "{vomsHost}" "{port}" "{DN}" "{voName}" "24"')
                DIRAC.gLogger.notice(f"Created vomsdir file {hostFilePath}")
            except Exception:
                DIRAC.gLogger.exception("Could not generate vomsdir file for host", vomsHost)
                error = f"Could not generate vomsdir file for VO {voName}, host {vomsHost}"
        try:
            vomsesFilePath = os.path.join(vomsesDirPath, voName)
            with open(vomsesFilePath, "w") as fd:
                fd.write("%s\n" % "\n".join(vomsesLines))
            DIRAC.gLogger.notice(f"Created vomses file {vomsesFilePath}")
        except Exception:
            DIRAC.gLogger.exception("Could not generate vomses file")
            error = f"Could not generate vomses file for VO {voName}"

    if params.useServerCert:
        Script.localCfg.deleteOption("/DIRAC/Security/UseServerCertificate")
        # When using Server Certs CA's will be checked, the flag only disables initial download
        # this will be replaced by the use of SkipCADownload
        Script.localCfg.deleteOption("/DIRAC/Security/SkipCAChecks")

    if error:
        return 1

    return 0


if __name__ == "__main__":
    exitCode = main()
    Script.localCfg.deleteOption("/DIRAC/Security/SkipCAChecks")
    sys.exit(exitCode)
