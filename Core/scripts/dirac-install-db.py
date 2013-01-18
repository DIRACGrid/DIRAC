#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-install-db
# Author :  Ricardo Graciani
########################################################################
"""
Create a new DB on the local MySQL server
"""
__RCSID__ = "$Id$"
#
from DIRAC.Core.Utilities import InstallTools
#
from DIRAC import gConfig
InstallTools.exitOnError = True
#
from DIRAC.Core.Base import Script
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgFile] ... DB ...' % Script.scriptName,
                                     'Arguments:',
                                     '  DB: Name of the Database (mandatory)'] ) )
Script.parseCommandLine()
args = Script.getPositionalArgs()
#

if len( args ) < 1:
  Script.showHelp()
  exit( -1 )

InstallTools.getMySQLPasswords()
for db in args:
  result = InstallTools.installDatabase( db )
  if not result['OK']:
    print "ERROR: failed to correctly install %s" % db, result['Message']
  else:
    extension, system = result['Value']
    InstallTools.addDatabaseOptionsToCS( gConfig, system, db, overwrite = True )
