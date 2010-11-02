#!/usr/bin/env python
# $HeadURL:  $
"""
Do the initial installation and configuration of a DIRAC service
"""
__RCSID__ = "$Id:  $"
#
from DIRAC.Core.Utilities import InstallTools
#
from DIRAC import gConfig
InstallTools.exitOnError = True
#
from DIRAC.Core.Base import Script
Script.setUsageMessage('\n'.join( ['Do the initial installation and configuration of a DIRAC service',
                                    'Usage:',
                                    '  %s [option|cfgfile] ... System Service' % Script.scriptName,
                                    'Arguments:',
                                    '  System:  Name of the DIRAC system (ie: WorkloadManagement)',
                                    '  Service: Name of the DIRAC service (ie: Matcher)'] ) )

Script.parseCommandLine()
args = Script.getPositionalArgs()

if len( args ) == 1 and args[0].find('/') != -1 :
  args = args[0].split('/')

if len( args ) != 2:
  Script.showHelp( )
  exit( -1 )
#
system = args[0]
service = args[1]
extensions = gConfig.getValue( '/DIRAC/Extensions', [] )

InstallTools.addDefaultOptionsToCS( gConfig, 'service', system, service, extensions, True )

InstallTools.installComponent( 'service', system, service, extensions )
