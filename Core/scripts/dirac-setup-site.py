#!/usr/bin/env python
# $HeadURL$
"""
Do the initial installation and configuration of a new DIRAC site
"""
__RCSID__ = "$Id$"
#
from DIRAC.Core.Base import Script
Script.disableCS()
Script.setUsageMessage( '\n'.join( ['Setup a DIRAC server (DBs, Services, Agents, Web Portal,...)',
                                    'Usage:',
                                    '  %s [option] ... [cfgfile]' % Script.scriptName,
                                    'Arguments:',
                                    '  cfgfile: DIRAC Cfg with description of the configuration (optional)'] ) )
Script.addDefaultOptionValue( '/DIRAC/Security/UseServerCertificate', 'yes' )
Script.parseCommandLine()
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
#
InstallTools.exitOnError = True
#
InstallTools.setupSite( Script.localCfg, cfg )
#
result = InstallTools.getStartupComponentStatus( [] )
if not result['OK']:
  print 'ERROR:', result['Message']
  exit( -1 )

InstallTools.printStartupStatus( result['Value'] )

