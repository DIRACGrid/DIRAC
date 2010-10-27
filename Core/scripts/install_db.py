#!/usr/bin/env python
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/DIRAC/trunk/DIRAC/Core/scripts/dirac-install.py $
"""
Create a new DB on the local MySQL server
"""
__RCSID__ = "$Id: dirac-install.py 26844 2010-07-16 08:44:22Z rgracian $"
#
from DIRAC.Core.Utilities import InstallTools
#
from DIRAC import gConfig
InstallTools.exitOnError = True
#
from DIRAC.Core.Base import Script
Script.setUsageMessage( '\n'.join( ['Create a new DB on the local MySQL server',
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
  extension, system = InstallTools.installDatabase( db )['Value']
  InstallTools.addDatabaseOptionsToCS( gConfig, system, db, True )
