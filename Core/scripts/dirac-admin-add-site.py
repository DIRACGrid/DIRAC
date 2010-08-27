#!/usr/bin/env python
########################################################################
# $HeadURL$
########################################################################
__RCSID__ = "$Id$"
import DIRAC
from DIRAC.Core.Base                                   import Script
Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

from DIRAC.ConfigurationSystem.Client.CSAPI           import CSAPI
from DIRAC.FrameworkSystem.Client.NotificationClient  import NotificationClient
from DIRAC.Core.Security.Misc                         import getProxyInfo
from DIRAC                                            import gConfig, gLogger
from DIRAC.Core.Utilities.List                        import intListToString
from DIRAC.Core.Security.CS                           import getPropertiesForGroup
csAPI = CSAPI()

def usage():
  gLogger.info( '%s DIRACSiteName GridSiteName CE [CE ... ]' % Script.scriptName )
  gLogger.info( ' Type "%s --help" for the available options and syntax' % Script.scriptName )
  DIRAC.exit( -1 )

if len( args ) < 3:
  usage()
  DIRAC.exit( -1 )

diracSiteName = args[0]
gridSiteName = args[1]
ces = args[2:]
try:
  diracGridType, place, country = diracSiteName.split( '.' )
except:
  gLogger.error( "The DIRACSiteName should be of the form GRID.LOCATION.COUNTRY for example LCG.CERN.ch" )
  DIRAC.exit( -1 )

res = getProxyInfo()
if not res['OK']:
  gLogger.error( "Failed to get proxy information", res['Message'] )
  DIRAC.exit( 2 )
userName = res['Value']['username']
group = res['Value']['group']

if not 'CSAdministrator' in getPropertiesForGroup( group ):
   gLogger.error( "You must be CSAdministrator user to execute this script" )
   gLogger.info( "Please issue 'dirac-proxy-init -g [group with CSAdministrator Property]'" )
   DIRAC.exit( 2 )

cfgBase = "/Resources/Sites/%s/%s" % ( diracGridType, diracSiteName )
res = gConfig.getOptionsDict( cfgBase )
if res['OK'] and res['Value']:
  gLogger.error( "The site %s is already defined:" % diracSiteName )
  for key, value in res['Value'].items():
    gLogger.info( "%s = %s" % ( key, value ) )
  DIRAC.exit( 2 )

csAPI.setOption( "%s/Name" % cfgBase, gridSiteName )
csAPI.setOption( "%s/CE" % cfgBase, ','.join( ces ) )
res = csAPI.commitChanges()
if not res['OK']:
  gLogger.error( "Failed to commit changes to CS", res['Message'] )
  DIRAC.exit( -1 )
else:
  gLogger.info( "Successfully added site %s to the CS with name %s and CEs: %s" % ( diracSiteName, gridSiteName, ','.join( ces ) ) )
