#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-fix-mysql-script
# Author :  Ricardo Graciani
########################################################################
"""
Fixes the mysql.server script, it requires a proper /LocalInstallation section
"""
__RCSID__ = "$Id$"
#
from DIRAC.Core.Base import Script
Script.disableCS()
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option] ... [cfgfile]' % Script.scriptName ] ) )

Script.addDefaultOptionValue( '/DIRAC/Security/UseServerCertificate', 'yes' )
Script.addDefaultOptionValue( 'LogLevel', 'INFO' )
Script.parseCommandLine()
from DIRAC.Core.Utilities import InstallTools
#
InstallTools.exitOnError = True
#
result = InstallTools.fixMySQLScripts()
if not result['OK']:
  print "ERROR:", result['Message']
  exit( -1 )
