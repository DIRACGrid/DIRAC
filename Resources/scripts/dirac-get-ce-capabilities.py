__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script
from DIRAC import gConfig, gLogger

Script.setUsageMessage( '\n'.join( ['Get the Tag of a CE',
                                    'Usage:',
                                    '%s [option]... [cfgfile]' % Script.scriptName,
                                    'Arguments:',
                                    ' cfgfile: DIRAC Cfg with description of the configuration (optional)'] ) )

ceName = ''
ceType = ''

def setCEName( args ):
  global ceName
  ceName = args

def setSite( args ):
  global Site
  Site = args

Script.registerSwitch( "N:", "Name=", "Computing Element Name (Mandatory)", setCEName )
Script.registerSwitch( "S:", "Site=", "Site Name (Mandatory)", setSite )

Script.parseCommandLine( ignoreErrors = True )
args = Script.getExtraCLICFGFiles()

if len( args ) > 1:
  Script.showHelp()
  exit( -1 )

siteType = Site.split( '.' )[0]
section = '/Resources/Sites/%s/%s/CEs/%s' % ( siteType, Site, ceName )
result = gConfig.getOptionsDict( section )
if result['OK']:
  if 'Tag' in result['Value']:
    gLogger.notice( result['Value']['Tag'] )


