#!/usr/bin/env python
# $HeadURL$
"""
  Main script to write dirac.cfg for a new DIRAC installation. And do initial download of CA's and CRL's
    if necessary.

  To be used by VO specific scripts to configure new DIRAC installations

  There are 2 mandatories arguments:

  -S --Setup=<setup>                               To define the DIRAC setup for the current installation
  -C --ConfigurationServer=<server>|-W --Gateway   To define the reference Configuration Servers/Gateway for the current installation

  Other arguments will take proper defaults if not defined.
  
  Notice: It will not overwrite exiting info in current dirac.cfg if it exists.
  
  Example: dirac-configure -d -S LHCb-Development -C 'dips://lhcbprod.pic.es:9135/Configuration/Server' -W 'dips://lhcbprod.pic.es:9135' --SkipCAChecks

"""
__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Core.Security.Misc import getProxyInfo

import sys, os

setup = None
configurationServer = None
gatewayServer = None
siteName = None
useServerCert = False
skipCAChecks = False
localSE = None
ceName = None
vo = None


def setDebug( optionValue ):
  DIRAC.gLogger.setLevel( 'DEBUG' )
  return DIRAC.S_OK()


def setGateway( optionValue ):
  global gatewayServer
  gatewayServer = optionValue
  setServer( gatewayServer + '/Configuration/Server' )
  return DIRAC.S_OK()


def setServer( optionValue ):
  global configurationServer
  configurationServer = optionValue
  DIRAC.gLogger.debug( '/DIRAC/Configuration/Servers =', configurationServer )
  Script.localCfg.addDefaultEntry( '/DIRAC/Configuration/Servers', configurationServer )
  return DIRAC.S_OK()


def setSetup( optionValue ):
  global setup
  setup = optionValue
  DIRAC.gLogger.debug( '/DIRAC/Setup =', setup )
  Script.localCfg.addDefaultEntry( '/DIRAC/Setup', setup )
  return DIRAC.S_OK()


def setSiteName( optionValue ):
  global siteName
  siteName = optionValue
  DIRAC.gLogger.debug( '/LocalSite/Site =', siteName )
  Script.localCfg.addDefaultEntry( '/LocalSite/Site', siteName )
  DIRAC.__siteName = False
  return DIRAC.S_OK()


def setCEName( optionValue ):
  global ceName
  ceName = optionValue
  return DIRAC.S_OK()


def setServerCert( optionValue ):
  global useServerCert
  useServerCert = True
  return DIRAC.S_OK()


def setSkipCAChecks( optionValue ):
  global skipCAChecks
  skipCAChecks = True
  return DIRAC.S_OK()


def setArchitecture( optionValue ):
  architecture = optionValue
  DIRAC.gLogger.debug( '/LocalSite/Architecture =', architecture )
  Script.localCfg.addDefaultEntry( '/LocalSite/Architecture', architecture )
  return DIRAC.S_OK()


def setLocalSE( optionValue ):
  global localSE
  localSE = optionValue
  DIRAC.gLogger.debug( '/LocalSite/localSE =', localSE )
  Script.localCfg.addDefaultEntry( '/LocalSite/localSE', localSE )
  return DIRAC.S_OK()

def setVO( optionValue ):
  global vo
  vo = optionValue
  DIRAC.gLogger.debug( '/DIRAC/VirtualOrganization =', optionValue )
  Script.localCfg.addDefaultEntry( '/DIRAC/VirtualOrganization', optionValue )
  return DIRAC.S_OK()

Script.disableCS()

Script.registerSwitch( "S:", "Setup=", "Set <setup> as DIRAC setup", setSetup )
Script.registerSwitch( "C:", "ConfigurationServer=", "Set <server> as DIRAC configuration server", setServer )
Script.registerSwitch( "n:", "SiteName=", "Set <sitename> as DIRAC Site Name", setSiteName )
Script.registerSwitch( "N:", "CEName=", "Determiner <sitename> from <cename>", setCEName )
Script.registerSwitch( "V:", "VO=", "Set the VO name", setVO )

Script.registerSwitch( "W:", "gateway=", "Configure <gateway> as DIRAC Gateway for the site", setGateway )

Script.registerSwitch( "", "UseServerCertificate", "Configure to use Server Certificate", setServerCert )
Script.registerSwitch( "", "SkipCAChecks", "Configure to skip check of CAs", setSkipCAChecks )

Script.registerSwitch( "", "Architecture=", "Configure /Architecture=<architecture>", setArchitecture )
Script.registerSwitch( "", "LocalSE=", "Configure LocalSite/LocalSE=<localse>", setLocalSE )

Script.registerSwitch( "d", "debug", "Set debug flag", setDebug )

Script.parseCommandLine( ignoreErrors = True )

DIRAC.gLogger.info( 'Executing: %s ' % ( ' '.join( sys.argv ) ) )

def usage():
  Script.showHelp()
  DIRAC.exit( 2 )

DIRAC.gLogger.info( 'Checking DIRAC installation at "%s"' % DIRAC.rootPath )

if not useServerCert:
  DIRAC.gLogger.debug( '/DIRAC/Security/UseServerCertificate =', 'no' )
  Script.localCfg.addDefaultEntry( '/DIRAC/Security/UseServerCertificate', 'no' )
else:
  # will be removed later but it is necessary to initialized the CS in script mode
  Script.localCfg.addDefaultEntry( '/DIRAC/Security/UseServerCertificate', 'yes' )




if skipCAChecks:
  DIRAC.gLogger.debug( '/DIRAC/Security/SkipCAChecks =', 'yes' )
  Script.localCfg.addDefaultEntry( '/DIRAC/Security/SkipCAChecks', 'yes' )
else:
  # Necessary to allow initial download of CA's
  DIRAC.gConfig.setOptionValue( '/DIRAC/Security/SkipCAChecks', 'yes' )
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
  Script.localCfg.deleteOption( '/DIRAC/Security/SkipCAChecks' )

if useServerCert:
  Script.localCfg.deleteOption( '/DIRAC/Security/UseServerCertificate' )
  # When using Server Certs CA's will be checked, the flag only disables initial download
  Script.localCfg.deleteOption( '/DIRAC/Security/SkipCAChecks' )

if ceName or siteName:
  # This is used in the pilot context, we should have a proxy and access to CS
  Script.enableCS()
  # Get the site resource section
  gridSections = DIRAC.gConfig.getSections( '/Resources/Sites/' )
  if not gridSections['OK']:
    DIRAC.gLogger.error( 'Could not get grid sections list' )
    grids = []
  else:
    grids = gridSections['Value']
  # try to get siteName from ceName or Local SE from siteName using Remote Configuration
  for grid in grids:
    siteSections = DIRAC.gConfig.getSections( '/Resources/Sites/%s/' % grid )
    if not siteSections['OK']:
      DIRAC.gLogger.error( 'Could not get %s site list' % grid )
      sites = []
    else:
      sites = siteSections['Value']

    if not siteName:
      if ceName:
        for site in sites:
          siteCEs = DIRAC.gConfig.getValue( '/Resources/Sites/%s/%s/CE' % ( grid, site ), [] )
          if ceName in siteCEs:
            siteName = site
            break
    if siteName:
      DIRAC.gLogger.info( 'Setting /LocalSite/Site = %s' % siteName )
      Script.localCfg.addDefaultEntry( '/LocalSite/Site', siteName )
      DIRAC.__siteName = False
      if ceName:
        DIRAC.gLogger.info( 'Setting /LocalSite/GridCE = %s' % ceName )
        Script.localCfg.addDefaultEntry( '/LocalSite/GridCE', ceName )

      if not localSE and siteName in sites:
        localSE = DIRAC.gConfig.getValue( '/Resources/Sites/%s/%s/SE' % ( grid, siteName ), 'None' )
        DIRAC.gLogger.info( 'Setting /LocalSite/LocalSE =', localSE )
        Script.localCfg.addDefaultEntry( '/LocalSite/LocalSE', localSE )
        break


if gatewayServer:
  DIRAC.gLogger.debug( '/DIRAC/GateWay/%s =' % DIRAC.siteName(), gatewayServer )
  Script.localCfg.addDefaultEntry( '/DIRAC/GateWay/%s' % DIRAC.siteName(), gatewayServer )

# Create the local dirac.cfg if it is not yet there
if not os.path.exists(DIRAC.gConfig.diracConfigFilePath):
  configDir = os.path.dirname(DIRAC.gConfig.diracConfigFilePath)
  if not os.path.exists(configDir):
    os.makedirs(configDir)
    
  # A.T. csDisabled state drops the CS Server setting, should be restored before dumping  
  csDisabledFlag = bool(Script.localCfg.csDisabled) 
  Script.enableCS()    
  DIRAC.gConfig.dumpLocalCFGToFile( DIRAC.gConfig.diracConfigFilePath )
  if csDisabledFlag:
    Script.disableCS()  
  
# We need user proxy or server certificate to continue
if not useServerCert: 
  result = getProxyInfo()
  if not result['OK']:
    DIRAC.gLogger.info('No user proxy available')
    sys.exit(0)
  else:
    Script.enableCS()  
else:
  Script.enableCS()      

#Do the vomsdir magic
voName = DIRAC.gConfig.getValue( "/DIRAC/VirtualOrganization", "" )
if not voName:
  sys.exit( 0 )
result = DIRAC.gConfig.getSections( "/Registry/VOMS/Servers/%s" % voName )
if not result[ 'OK' ]:
  sys.exit( 0 )
DIRAC.gLogger.info( "Creating VOMSDIR/VOMSES files" )
vomsDirHosts = result[ 'Value' ]
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
  try:
    DN = DIRAC.gConfig.getValue( "/Registry/VOMS/Servers/%s/%s/DN" % ( voName, vomsHost ), "" )
    CA = DIRAC.gConfig.getValue( "/Registry/VOMS/Servers/%s/%s/CA" % ( voName, vomsHost ), "" )
    Port = DIRAC.gConfig.getValue( "/Registry/VOMS/Servers/%s/%s/Port" % ( voName, vomsHost ), 0 )
    if not DN or not CA or not Port:
      continue
    fd = open( hostFilePath, "wb" )
    fd.write( "%s\n%s\n" % ( DN, CA ) )
    fd.close()
    vomsesLines.append( '"%s" "%s" "%s" "%s" "%s" "24"' % ( voName, vomsHost, Port, DN, voName ) )
    DIRAC.gLogger.info( "Created vomsdir file %s" % hostFilePath )
  except:
    DIRAC.gLogger.exception( "Could not generate vomsdir file for host", vomsHost )

try:
  vomsesFilePath = os.path.join( vomsesDirPath, voName )
  fd = open( vomsesFilePath, "wb" )
  fd.write( "%s\n" % "\n".join( vomsesLines ) )
  fd.close()
  DIRAC.gLogger.info( "Created vomses file %s" % vomsesFilePath )
except:
  DIRAC.gLogger.exception( "Could not generate vomses file" )
