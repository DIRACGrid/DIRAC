#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-setup-site
# Author :  Ricardo Graciani
########################################################################
"""
Initial installation and configuration of a new DIRAC server (DBs, Services, Agents, Web Portal,...)
"""
__RCSID__ = "$Id$"
#
from DIRAC.Core.Base import Script
Script.disableCS()
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option] ... [cfgfile]' % Script.scriptName,
                                     'Arguments:',
                                     '  cfgfile: DIRAC Cfg with description of the configuration (optional)' ] ) )

Script.addDefaultOptionValue( '/DIRAC/Security/UseServerCertificate', 'yes' )
Script.addDefaultOptionValue( 'LogLevel', 'INFO' )
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
result = InstallTools.setupSite( Script.localCfg, cfg )
if not result['OK']:
  print "ERROR:", result['Message']
  exit( -1 )
#
result = InstallTools.getStartupComponentStatus( [] )
if not result['OK']:
  print 'ERROR:', result['Message']
  exit( -1 )

print "\nStatus of installed components:\n"
result = InstallTools.printStartupStatus( result['Value'] )
if not result['OK']:
  print 'ERROR:', result['Message']
  exit( -1 )
