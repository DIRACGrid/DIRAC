#!/usr/bin/env python
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/DIRAC/trunk/DIRAC/Core/scripts/dirac-install.py $
"""
Do the initial installation and configuration of the DIRAC MySQL server
"""
__RCSID__ = "$Id: dirac-install.py 26844 2010-07-16 08:44:22Z rgracian $"
#
from DIRAC.Core.Utilities import InstallTools
#
from DIRAC import gConfig
InstallTools.exitOnError = True
#
from DIRAC.Core.Base import Script
Script.setUsageMessage( '\n'.join( ['Usage:',
                                    '%s [option|cfgFile] ... DB ...' % Script.scriptName,
                                    'Arguments:',
                                    ' DB: Name of the Database (mandatory)'] ) )
Script.parseCommandLine()
args = Script.getPositionalArgs()
#
def usage():
  Script.showHelp()
  exit( -1 )

if len( args ) < 1:
  usage()
  exit( -1 )

InstallTools.getMySQLPasswords()
for db in args:
  extension, system = InstallTools.installDatabase( db )['Value']
  InstallTools.addDatabaseOptionsToCS( gConfig, system, db, True )
