#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-config-ce
# Author :  Ricardo Graciani
########################################################################
__RCSID__ = "$Id$"
import DIRAC
from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( ['Configure a Local CE to be used in a DIRAC Site',
                                    'Usage:',
                                    '%s [option]... [cfgfile]' % Script.scriptName,
                                    'Arguments:',
                                    ' cfgfile: DIRAC Cfg with description of the configuration (optional)'] ) )

ceName = ''
ceType = ''
setupDirector = False

def setCEName( args ):
  global ceName
  ceName = args

def setCEType( args ):
  global ceType
  ceType = args

def setDirector( args ):
  global setupDirector
  setupDirector = True

Script.registerSwitch( "N:", "Name=", "Computing Element Name (Mandatory)", setCEName )
Script.registerSwitch( "T:", "Type=", "Computing Element Type (Mandatory)", setCEType )
Script.registerSwitch( "D", "Director", "Setup a Director Using this CE", setDirector )

Script.parseCommandLine( ignoreErrors = True )
args = Script.getExtraCLICFGFiles()
#
if len( args ) > 1:
  Script.showHelp()
  exit( -1 )
#
cfg = None
if len( args ):
  cfg = args[0]

from DIRAC.Core.Utilities import InstallTools
result = InstallTools.configureCE( ceName, ceType, cfg, Script.localCfg.currentSectionPath )
if not result['OK']:
  Script.showHelp()
  DIRAC.exit( -1 )

ceNameList = result['Value']

if setupDirector:
  print InstallTools.configureLocalDirector( ceNameList )
