#!/usr/bin/env python
# $HeadURL$
"""
  Main script to write dirac.cfg for a new DIRAC installation.
  To be used by VO specific scripts to configure new DIRAC installations
  There are 2 mandatories arguments:

  -S --Setup=<setup>                               To define the DIRAC setup for the current installation
  -C --ConfigurationServer=<server>|-W --Gateway   To define the reference Configuration Servers/Gateway for the current installation

  Other arguments will take proper defaults if not defined.

  Example: dirac-configure -d -S LHCb-Development -C 'dips://lhcbprod.pic.es:9135/Configuration/Server' -W 'dips://lhcbprod.pic.es:9135' --SkipCAChecks

"""
__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base                                         import Script

import sys

setup = None
configurationServer = None
gatewayServer = None
siteName = None
useServerCert = False
skipCAChecks  = False
localSE = None
ceName = None

def setDebug( optionValue ):
  DIRAC.gLogger.setLevel( 'DEBUG' )
  return DIRAC.S_OK()


def setGateway( optionValue ):
  global gatewayServer
  gatewayServer = optionValue
  setServer( gatewayServer+'/Configuration/Server' )
  return DIRAC.S_OK()


def setServer( optionValue ):
  global configurationServer
  configurationServer = optionValue
  DIRAC.gLogger.debug(      '/DIRAC/Configuration/Servers =', configurationServer )
  Script.localCfg.addDefaultEntry( '/DIRAC/Configuration/Servers',   configurationServer )
  return DIRAC.S_OK()


def setSetup( optionValue ):
  global setup
  setup = optionValue
  DIRAC.gLogger.debug(      '/DIRAC/Setup =', setup )
  Script.localCfg.addDefaultEntry( '/DIRAC/Setup',   setup )
  return DIRAC.S_OK()


def setSiteName( optionValue ):
  global siteName
  siteName = optionValue
  DIRAC.gLogger.debug(      '/LocalSite/Site =', siteName )
  Script.localCfg.addDefaultEntry( '/LocalSite/Site',   siteName )
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
  DIRAC.gLogger.debug(      '/LocalSite/Architecture =', architecture )
  Script.localCfg.addDefaultEntry( '/LocalSite/Architecture',   architecture )
  return DIRAC.S_OK()


def setLocalSE( optionValue ):
  global localSE
  localSE = optionValue
  DIRAC.gLogger.debug(      '/LocalSite/localSE =', localSE )
  Script.localCfg.addDefaultEntry( '/LocalSite/localSE',   localSE )
  return DIRAC.S_OK()

Script.disableCS()

Script.registerSwitch( "S:", "Setup=",                "Set <setup> as DIRAC setup", setSetup )
Script.registerSwitch( "C:", "ConfigurationServer=",  "Set <server> as DIRAC configuration server", setServer )
Script.registerSwitch( "N:", "SiteName=",             "Set <sitename> as DIRAC Site Name", setSiteName )
Script.registerSwitch( "n:", "CEName=",               "Determiner <sitename> from <cename>", setCEName )

Script.registerSwitch( "W:", "gateway=",              "Configure <gateway> as DIRAC Gateway for the site", setGateway )

Script.registerSwitch("", "UseServerCertificate",     "Configure to use Server Certificate", setServerCert)
Script.registerSwitch("", "SkipCAChecks",             "Configure to skip check of CAs", setSkipCAChecks )

Script.registerSwitch("", "Architecture=",            "Configure LocalSite/Architecture=<architecture>", setArchitecture )
Script.registerSwitch("", "LocalSE=",                 "Configure LocalSite/LocalSE=<localse>", setLocalSE )

Script.registerSwitch( "d",  "debug",   "Set debug flag", setDebug )

Script.parseCommandLine( ignoreErrors = True )

DIRAC.gLogger.info( 'Executing: %s ' % ( ' '.join(sys.argv) ) )

def usage():
  Script.showHelp()
  DIRAC.exit(2)

DIRAC.gLogger.info( 'Checking DIRAC installation at "%s"' % DIRAC.rootPath )

if not useServerCert:
  DIRAC.gLogger.debug(      '/DIRAC/Security/UseServerCertificate =', 'no' )
  Script.localCfg.addDefaultEntry( '/DIRAC/Security/UseServerCertificate',   'no' )

if skipCAChecks:
  DIRAC.gLogger.debug(      '/DIRAC/Security/SkipCAChecks =', 'yes' )
  Script.localCfg.addDefaultEntry( '/DIRAC/Security/SkipCAChecks',   'yes' )

Script.enableCS()

gridSections = DIRAC.gConfig.getSections('/Resources/Sites/')
if not gridSections['OK']:
  DIRAC.gLogger.error('Could not get grid sections list')
  grids = []
else:
  grids = gridSections['Value']

for grid in grids:  
  print grid
  siteSections = DIRAC.gConfig.getSections('/Resources/Sites/%s/' % grid)
  if not siteSections['OK']: 
    DIRAC.gLogger.error('Could not get %s site list' % grid)
    sites = []
  else:
    sites = siteSections['Value']

  if not siteName:
    if ceName:
      for site in sites:
        siteCEs = DIRAC.gConfig.getValue('/Resources/Sites/%s/%s/CE' % (grid,site),[])
        if ceName in siteCEs:
          siteName = site
          break
  if siteName:
    DIRAC.gLogger.info(       'Setting /LocalSite/Site = %s' % siteName )
    Script.localCfg.addDefaultEntry( '/LocalSite/Site', siteName )
    DIRAC.__siteName = False
    if ceName:
      DIRAC.gLogger.info(       'Setting /LocalSite/GridCE = %s' % ceName )
      Script.localCfg.addDefaultEntry( '/LocalSite/GridCE', ceName )
    if not localSE:
      localSE = DIRAC.gConfig.getValue( '/Resources/Sites/%s/%s/SE' % ( grid, siteName ), 'None' )
      if not localSE == 'None':
        DIRAC.gLogger.info(       'Setting /LocalSite/LocalSE =', localSE )
        Script.localCfg.addDefaultEntry( '/LocalSite/LocalSE', localSE )

    break    


if gatewayServer:
  DIRAC.gLogger.debug(      '/DIRAC/GateWay/%s =' % DIRAC.siteName(), gatewayServer )
  Script.localCfg.addDefaultEntry( '/DIRAC/GateWay/%s' % DIRAC.siteName(),   gatewayServer )



DIRAC.gConfig.dumpLocalCFGToFile( 'dirac.cfg' )

DIRAC.gLogger.info( '' )
usage()


