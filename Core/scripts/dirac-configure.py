#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-configure
# Author :  Ricardo Graciani
########################################################################
"""
  Main script to write dirac.cfg for a new DIRAC installation and initial download of CAs and CRLs
    if necessary.

  To be used by VO specific scripts to configure new DIRAC installations

  There are 2 mandatories arguments:

  -S --Setup=<setup>                               To define the DIRAC setup for the current installation
  -C --ConfigurationServer=<server>|-W --Gateway   To define the reference Configuration Servers/Gateway for the current installation

  others are optional

  -I --IncludeAllServers                           To include all Configuration Servers (by default only those in -C option are included)
  -n --SiteName=<sitename>                         To define the DIRAC Site Name for the installation
  -N --CEName=<cename>                             To determine the DIRAC Site Name from the CE Name
  -V --VO=<vo>                                     To define the VO for the installation
  -U  --UseServerCertificate                       To use Server Certificate for all clients
  -H  --SkipCAChecks                               To skip check of CAs for all clients
  -D  --SkipCADownload                             To skip download of CAs 
  -v --UseVersionsDir                              Use versions directory (This option will properly define RootPath and InstancePath)
  -A --Architecture=<architecture>                 To define /LocalSite/Architecture=<architecture>
  -L --LocalSE=<localse>                           To define /LocalSite/LocalSE=<localse>
  -F --ForceUpdate                                 Forces the update of dirac.cfg, even if it does already exists (use with care)

  Other arguments will take proper defaults if not defined.
  
  Additionally all options can all be passed inside a .cfg file passed as argument. The following options are recognized:

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
  
  As in any other script command line option take precedence over .cfg files passed as arguments. 
  The combination of both is written into the installed dirac.cfg. 
  
  Notice: It will not overwrite exiting info in current dirac.cfg if it exists.
  
  Example: dirac-configure -d -S LHCb-Development -C 'dips://lhcbprod.pic.es:9135/Configuration/Server' -W 'dips://lhcbprod.pic.es:9135' --SkipCAChecks

"""
__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.ConfigurationSystem.Client.Helpers import cfgInstallPath, cfgPath, Resources, Registry

import sys, os

logLevel = None
setup = None
configurationServer = None
includeAllServers = False
gatewayServer = None
siteName = None
useServerCert = False
skipCAChecks = False
skipCADownload = False
useVersionsDir = False
architecture = None
localSE = None
ceName = None
vo = None
update = False

def setGateway( optionValue ):
  global gatewayServer
  gatewayServer = optionValue
  setServer( gatewayServer + '/Configuration/Server' )
  DIRAC.gConfig.setOptionValue( cfgInstallPath( 'Gateway' ), gatewayServer )
  return DIRAC.S_OK()


def setServer( optionValue ):
  global configurationServer
  configurationServer = optionValue
  Script.localCfg.addDefaultEntry( '/DIRAC/Configuration/Servers', configurationServer )
  DIRAC.gConfig.setOptionValue( cfgInstallPath( 'ConfigurationServer' ), configurationServer )
  return DIRAC.S_OK()


def setAllServers( optionValue ):
  global includeAllServers
  includeAllServers = True


def setSetup( optionValue ):
  global setup
  setup = optionValue
  DIRAC.gConfig.setOptionValue( '/DIRAC/Setup', setup )
  DIRAC.gConfig.setOptionValue( cfgInstallPath( 'Setup' ), setup )
  return DIRAC.S_OK()


def setSiteName( optionValue ):
  global siteName
  siteName = optionValue
  Script.localCfg.addDefaultEntry( '/LocalSite/Site', siteName )
  DIRAC.__siteName = False
  DIRAC.gConfig.setOptionValue( cfgInstallPath( 'SiteName' ), siteName )
  return DIRAC.S_OK()


def setCEName( optionValue ):
  global ceName
  ceName = optionValue
  DIRAC.gConfig.setOptionValue( cfgInstallPath( 'CEName' ), ceName )
  return DIRAC.S_OK()


def setServerCert( optionValue ):
  global useServerCert
  useServerCert = True
  DIRAC.gConfig.setOptionValue( cfgInstallPath( 'UseServerCertificate' ), useServerCert )
  return DIRAC.S_OK()


def setSkipCAChecks( optionValue ):
  global skipCAChecks
  skipCAChecks = True
  DIRAC.gConfig.setOptionValue( cfgInstallPath( 'SkipCAChecks' ), skipCAChecks )
  return DIRAC.S_OK()

def setSkipCADownload( optionValue ):
  global skipCADownload
  skipCADownload = True
  DIRAC.gConfig.setOptionValue( cfgInstallPath( 'SkipCADownload' ), skipCADownload )
  return DIRAC.S_OK()

def setUseVersionsDir( optionValue ):
  global useVersionsDir
  useVersionsDir = True
  DIRAC.gConfig.setOptionValue( cfgInstallPath( 'UseVersionsDir' ), useVersionsDir )
  return DIRAC.S_OK()

def setArchitecture( optionValue ):
  global architecture
  architecture = optionValue
  Script.localCfg.addDefaultEntry( '/LocalSite/Architecture', architecture )
  DIRAC.gConfig.setOptionValue( cfgInstallPath( 'Architecture' ), architecture )
  return DIRAC.S_OK()


def setLocalSE( optionValue ):
  global localSE
  localSE = optionValue
  Script.localCfg.addDefaultEntry( '/LocalSite/localSE', localSE )
  DIRAC.gConfig.setOptionValue( cfgInstallPath( 'LocalSE' ), localSE )
  return DIRAC.S_OK()

def setVO( optionValue ):
  global vo
  vo = optionValue
  Script.localCfg.addDefaultEntry( '/DIRAC/VirtualOrganization', vo )
  DIRAC.gConfig.setOptionValue( cfgInstallPath( 'VirtualOrganization' ), vo )
  return DIRAC.S_OK()

def forceUpdate( optionValue ):
  global update
  update = True
  return DIRAC.S_OK()

Script.disableCS()

Script.registerSwitch( "S:", "Setup=", "Set <setup> as DIRAC setup", setSetup )
Script.registerSwitch( "C:", "ConfigurationServer=", "Set <server> as DIRAC configuration server", setServer )
Script.registerSwitch( "I", "IncludeAllServers", "include all Configuration Servers", setAllServers )
Script.registerSwitch( "n:", "SiteName=", "Set <sitename> as DIRAC Site Name", setSiteName )
Script.registerSwitch( "N:", "CEName=", "Determiner <sitename> from <cename>", setCEName )
Script.registerSwitch( "V:", "VO=", "Set the VO name", setVO )

Script.registerSwitch( "W:", "gateway=", "Configure <gateway> as DIRAC Gateway for the site", setGateway )

Script.registerSwitch( "U", "UseServerCertificate", "Configure to use Server Certificate", setServerCert )
Script.registerSwitch( "H", "SkipCAChecks", "Configure to skip check of CAs", setSkipCAChecks )
Script.registerSwitch( "D", "SkipCADownload", "Configure to skip download of CAs", setSkipCADownload )

Script.registerSwitch( "v", "UseVersionsDir", "Use versions directory", setUseVersionsDir )

Script.registerSwitch( "A:", "Architecture=", "Configure /Architecture=<architecture>", setArchitecture )
Script.registerSwitch( "L:", "LocalSE=", "Configure LocalSite/LocalSE=<localse>", setLocalSE )

Script.registerSwitch( "F", "ForceUpdate", "Force Update of dirac.cfg (otherwise nothing happens if dirac.cfg already exists)", forceUpdate )

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                    '\nUsage:',
                                    '  %s [option|cfgfile] ...\n' % Script.scriptName ] ) )

Script.parseCommandLine( ignoreErrors = True )
args = Script.getExtraCLICFGFiles()

if not logLevel:
  logLevel = DIRAC.gConfig.getValue( cfgInstallPath( 'LogLevel' ), '' )
  if logLevel:
    DIRAC.gLogger.setLevel( logLevel )
else:
  DIRAC.gConfig.setOptionValue( cfgInstallPath( 'LogLevel' ), logLevel )

if not gatewayServer:
  newGatewayServer = DIRAC.gConfig.getValue( cfgInstallPath( 'Gateway' ), '' )
  if newGatewayServer:
    setGateway( newGatewayServer )

if not configurationServer:
  newConfigurationServer = DIRAC.gConfig.getValue( cfgInstallPath( 'ConfigurationServer' ), '' )
  if newConfigurationServer:
    setServer( newConfigurationServer )

if not includeAllServers:
  newIncludeAllServer = DIRAC.gConfig.getValue( cfgInstallPath( 'IncludeAllServers' ), False )
  if newIncludeAllServer:
    setAllServers( True )

if not setup:
  newSetup = DIRAC.gConfig.getValue( cfgInstallPath( 'Setup' ), '' )
  if newSetup:
    setSetup( newSetup )

if not siteName:
  newSiteName = DIRAC.gConfig.getValue( cfgInstallPath( 'SiteName' ), '' )
  if newSiteName:
    setSiteName( newSiteName )

if not ceName:
  newCEName = DIRAC.gConfig.getValue( cfgInstallPath( 'CEName' ), '' )
  if newCEName:
    setCEName( newCEName )

if not useServerCert:
  newUserServerCert = DIRAC.gConfig.getValue( cfgInstallPath( 'UseServerCertificate' ), False )
  if newUserServerCert:
    setServerCert( newUserServerCert )

if not skipCAChecks:
  newSkipCAChecks = DIRAC.gConfig.getValue( cfgInstallPath( 'SkipCAChecks' ), False )
  if newSkipCAChecks:
    setSkipCAChecks( newSkipCAChecks )

if not skipCADownload:
  newSkipCADownload = DIRAC.gConfig.getValue( cfgInstallPath( 'SkipCADownload' ), False )
  if newSkipCADownload:
    setSkipCADownload( newSkipCADownload )

if not useVersionsDir:
  newUseVersionsDir = DIRAC.gConfig.getValue( cfgInstallPath( 'UseVersionsDir' ), False )
  if newUseVersionsDir:
    setUseVersionsDir( newUseVersionsDir )
    # Set proper Defaults in configuration (even if they will be properly overwrite by InstallTools
    instancePath = os.path.dirname( os.path.dirname( DIRAC.rootPath ) )
    rootPath = os.path.join( instancePath, 'pro' )
    DIRAC.gConfig.setOptionValue( cfgInstallPath( 'InstancePath' ), instancePath )
    DIRAC.gConfig.setOptionValue( cfgInstallPath( 'RootPath' ), rootPath )

if not architecture:
  newArchitecture = DIRAC.gConfig.getValue( cfgInstallPath( 'Architecture' ), '' )
  if newArchitecture:
    setArchitecture( newArchitecture )

if not vo:
  newVO = DIRAC.gConfig.getValue( cfgInstallPath( 'VirtualOrganization' ), '' )
  if newVO:
    setVO( newVO )

DIRAC.gLogger.notice( 'Executing: %s ' % ( ' '.join( sys.argv ) ) )
DIRAC.gLogger.notice( 'Checking DIRAC installation at "%s"' % DIRAC.rootPath )

if update:
  DIRAC.gLogger.notice( 'Will update dirac.cfg' )

if setup:
  DIRAC.gLogger.verbose( '/DIRAC/Setup =', setup )
if vo:
  DIRAC.gLogger.verbose( '/DIRAC/VirtualOrganization =', vo )
if configurationServer:
  DIRAC.gLogger.verbose( '/DIRAC/Configuration/Servers =', configurationServer )

if siteName:
  DIRAC.gLogger.verbose( '/LocalSite/Site =', siteName )
if architecture:
  DIRAC.gLogger.verbose( '/LocalSite/Architecture =', architecture )
if localSE:
  DIRAC.gLogger.verbose( '/LocalSite/localSE =', localSE )

if not useServerCert:
  DIRAC.gLogger.verbose( '/DIRAC/Security/UseServerCertificate =', 'no' )
  Script.localCfg.addDefaultEntry( '/DIRAC/Security/UseServerCertificate', 'no' )
else:
  # will be removed later but it is necessary to initialized the CS in script mode
  Script.localCfg.addDefaultEntry( '/DIRAC/Security/UseServerCertificate', 'yes' )

host = DIRAC.gConfig.getValue( cfgInstallPath( "Host" ), "" )
if host:
  DIRAC.gConfig.setOptionValue( cfgPath( "DIRAC", "Hostname" ), host )

if skipCAChecks:
  DIRAC.gLogger.verbose( '/DIRAC/Security/SkipCAChecks =', 'yes' )
  Script.localCfg.addDefaultEntry( '/DIRAC/Security/SkipCAChecks', 'yes' )
else:
  # Necessary to allow initial download of CA's
  if not skipCADownload:
    DIRAC.gConfig.setOptionValue( '/DIRAC/Security/SkipCAChecks', 'yes' )
if not skipCADownload:
  Script.enableCS()
  try:
    dirName = os.path.join( DIRAC.rootPath, 'etc', 'grid-security', 'certificates' )
    if not os.path.exists( dirName ):
      os.makedirs( dirName )
  except:
    DIRAC.gLogger.exception()
    DIRAC.gLogger.fatal( 'Fail to create directory:', dirName )
    DIRAC.exit( -1 )
  try:
    from DIRAC.FrameworkSystem.Client.BundleDeliveryClient import BundleDeliveryClient
    bdc = BundleDeliveryClient()
    result = bdc.syncCAs()
    if result[ 'OK' ]:
      result = bdc.syncCRLs()
  except:
    DIRAC.gLogger.exception( 'Could not import BundleDeliveryClient' )
    pass
  if not skipCAChecks:
    Script.localCfg.deleteOption( '/DIRAC/Security/SkipCAChecks' )

if ceName or siteName:
  # This is used in the pilot context, we should have a proxy and access to CS
  Script.enableCS()
  
  resources = Resources.Resources( vo=vo )
  
  if not siteName:
    if ceName:
      result = resources.getSiteForResource( ceName )
      if result['OK']:
        site = result['Value']
        result = resources.getSiteFullName( site )
        if result['OK']:
          siteName = result['Value']    
          
  if siteName:
    DIRAC.gLogger.notice( 'Setting /LocalSite/Site = %s' % siteName )
    Script.localCfg.addDefaultEntry( '/LocalSite/Site', siteName )
    DIRAC.__siteName = False
    if ceName:
      DIRAC.gLogger.notice( 'Setting /LocalSite/GridCE = %s' % ceName )
      Script.localCfg.addDefaultEntry( '/LocalSite/GridCE', ceName )

    if not localSE:
      localSE = resources.getStorageElements( siteName )
      if localSE['OK'] and localSE['Value']:
        localSE = ','.join( localSE['Value'] )
      else:
        localSE = 'None'
      DIRAC.gLogger.notice( 'Setting /LocalSite/LocalSE =', localSE )
      Script.localCfg.addDefaultEntry( '/LocalSite/LocalSE', localSE )

if useServerCert:
  Script.localCfg.deleteOption( '/DIRAC/Security/UseServerCertificate' )
  # When using Server Certs CA's will be checked, the flag only disables initial download
  # this will be replaced by the use of SkipCADownload
  Script.localCfg.deleteOption( '/DIRAC/Security/SkipCAChecks' )

if gatewayServer:
  DIRAC.gLogger.verbose( '/DIRAC/Gateways/%s =' % DIRAC.siteName(), gatewayServer )
  Script.localCfg.addDefaultEntry( '/DIRAC/Gateways/%s' % DIRAC.siteName(), gatewayServer )

# Create the local dirac.cfg if it is not yet there
if not os.path.exists( DIRAC.gConfig.diracConfigFilePath ):
  configDir = os.path.dirname( DIRAC.gConfig.diracConfigFilePath )
  if not os.path.exists( configDir ):
    os.makedirs( configDir )
  update = True
  DIRAC.gConfig.dumpLocalCFGToFile( DIRAC.gConfig.diracConfigFilePath )

# We need user proxy or server certificate to continue
if not useServerCert:
  Script.enableCS()
  result = getProxyInfo()
  if not result['OK']:
    DIRAC.gLogger.notice( 'Configuration is not completed because no user proxy is available' )
    DIRAC.gLogger.notice( 'Create one using dirac-proxy-init and execute again with -F option' )
    sys.exit( 0 )
else:
  Script.localCfg.addDefaultEntry( '/DIRAC/Security/UseServerCertificate', 'yes' )
  Script.enableCS()
  Script.localCfg.deleteOption( '/DIRAC/Security/UseServerCertificate' )

if includeAllServers:
  DIRAC.gConfig.setOptionValue( '/DIRAC/Configuration/Servers', ','.join( DIRAC.gConfig.getServersList() ) )
  DIRAC.gLogger.verbose( '/DIRAC/Configuration/Servers =', ','.join( DIRAC.gConfig.getServersList() ) )

if update:
  DIRAC.gConfig.dumpLocalCFGToFile( DIRAC.gConfig.diracConfigFilePath )


#Do the vomsdir/vomses magic
# This has to be done for all VOs in the installation

result = Registry.getVOMSServerInfo()
if not result['OK']:
  sys.exit( 1 )
  
error = ''  
vomsDict = result['Value']
for vo in vomsDict:
  voName = vomsDict[vo]['VOMSName']
  vomsDirHosts = vomsDict[vo]['Servers'].keys()
  vomsDirPath = os.path.join( DIRAC.rootPath, 'etc', 'grid-security', 'vomsdir', voName )
  vomsesDirPath = os.path.join( DIRAC.rootPath, 'etc', 'grid-security', 'vomses' )
  for path in ( vomsDirPath, vomsesDirPath ):
    if not os.path.isdir( path ):
      try:
        os.makedirs( path )
      except Exception, e:
        DIRAC.gLogger.error( "Could not create directory", str( e ) )
        sys.exit( 1 )
  vomsesLines = []  
  for vomsHost in vomsDirHosts:
    hostFilePath = os.path.join( vomsDirPath, "%s.lsc" % vomsHost )
    if "Servers" in vomsDict[vo]:
      try:
        DN = vomsDict[vo]['Servers'][vomsHost]['DN']
        CA = vomsDict[vo]['Servers'][vomsHost]['CA']
        port = vomsDict[vo]['Servers'][vomsHost]['Port']
        if not DN or not CA or not port:
          DIRAC.gLogger.error( 'DN = %s' % DN )
          DIRAC.gLogger.error( 'CA = %s' % CA )
          DIRAC.gLogger.error( 'Port = %s' % port )
          DIRAC.gLogger.error( 'Missing Parameter for %s' % vomsHost )
          continue
        fd = open( hostFilePath, "wb" )
        fd.write( "%s\n%s\n" % ( DN, CA ) )
        fd.close()
        vomsesLines.append( '"%s" "%s" "%s" "%s" "%s" "24"' % ( voName, vomsHost, port, DN, voName ) )
        DIRAC.gLogger.notice( "Created vomsdir file %s" % hostFilePath )
      except:
        DIRAC.gLogger.exception( "Could not generate vomsdir file for host", vomsHost )
        error = "Could not generate vomsdir file for VO %s, host %s" % (voName, vomsHost)

  try:
    vomsesFilePath = os.path.join( vomsesDirPath, voName )
    fd = open( vomsesFilePath, "wb" )
    fd.write( "%s\n" % "\n".join( vomsesLines ) )
    fd.close()
    DIRAC.gLogger.notice( "Created vomses file %s" % vomsesFilePath )
  except:
    DIRAC.gLogger.exception( "Could not generate vomses file" )
    error = "Could not generate vomses file for VO %s" % voName

if error:
  sys.exit( 1 )

sys.exit( 0 )
