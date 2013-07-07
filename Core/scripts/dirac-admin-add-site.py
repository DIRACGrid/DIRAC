#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :   dirac-admin-add-site
# Author : Andrew C. Smith
########################################################################
"""
  Add a new DIRAC SiteName to DIRAC Configuration, including one or more CEs
"""
__RCSID__ = "$Id$"

from DIRAC.Core.Base                                   import Script
Script.setUsageMessage('\n'.join( [ __doc__.split( '\n' )[1],
                                    'Usage:',
                                    '  %s [option|cfgfile] ... DIRACSiteName GridSiteName CE [CE] ...' % Script.scriptName,
                                    'Arguments:',
                                    '  DIRACSiteName: Name of the site for DIRAC in the form DOMAIN.LOCATION.COUNTRY (ie:LCG.CERN.ch)',
                                    '  GridSiteName: Name of the site in the Grid (ie: CERN-PROD)',
                                    '  CE: Name of the CE to be included in the site (ie: ce111.cern.ch)'] ) )
Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

from DIRAC.ConfigurationSystem.Client.CSAPI                                      import CSAPI
from DIRAC.FrameworkSystem.Client.NotificationClient                             import NotificationClient
from DIRAC.Core.Security.ProxyInfo                                               import getProxyInfo
from DIRAC                                                                       import exit as DIRACExit, gConfig, gLogger
from DIRAC.Core.Utilities.List                                                   import intListToString
from DIRAC.ConfigurationSystem.Client.Helpers.Registry                           import getPropertiesForGroup
csAPI = CSAPI()

if len( args ) < 3:
  Script.showHelp()
  DIRACExit( -1 )

diracSiteName = args[0]
gridSiteName = args[1]
ces = args[2:]
try:
  diracGridType, place, country = diracSiteName.split( '.' )
except:
  gLogger.error( "The DIRACSiteName should be of the form GRID.LOCATION.COUNTRY for example LCG.CERN.ch" )
  DIRACExit( -1 )

res = getProxyInfo()
if not res['OK']:
  gLogger.error( "Failed to get proxy information", res['Message'] )
  DIRACExit( 2 )
userName = res['Value']['username']
group = res['Value']['group']

if not 'CSAdministrator' in getPropertiesForGroup( group ):
   gLogger.error( "You must be CSAdministrator user to execute this script" )
   gLogger.notice( "Please issue 'dirac-proxy-init -g [group with CSAdministrator Property]'" )
   DIRACExit( 2 )

siteDict = {}
siteDict['Name'] = gridSiteName
siteDict['Domains'] = diracGridType
siteName = '.'.join( [place, country] )  
result = csAPI.addSite( siteName, siteDict )
if not result['OK']:
  gLogger.error( "Error: %s" % result['Message'] )
  DIRACExit( -1 )

for ce in ces:
  result = csAPI.addResource( siteName, 'Computing', ce, { 'CEType': "LCG" } )
  if not result['OK']:
    gLogger.error( "Error: %s" % result['Message'] )
    DIRACExit( -1 )

res = csAPI.commitChanges()
if not res['OK']:
  gLogger.error( "Failed to commit changes to CS", res['Message'] )
  DIRACExit( -1 )
else:
  gLogger.notice( "Successfully added site %s to the CS with name %s and CEs: %s" % ( diracSiteName, gridSiteName, ','.join( ces ) ) )
