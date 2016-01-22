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
from DIRAC.FrameworkSystem.Client.ComponentInstaller import gComponentInstaller
from DIRAC.FrameworkSystem.Utilities import MonitoringUtilities
#
from DIRAC import gConfig
gComponentInstaller.exitOnError = True
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

gComponentInstaller.getMySQLPasswords()
for db in args:
  result = gComponentInstaller.installDatabase( db )
  if not result['OK']:
    print "ERROR: failed to correctly install %s" % db, result['Message']
  else:
    extension, system = result['Value']
    gComponentInstaller.addDatabaseOptionsToCS( gConfig, system, db, overwrite = True )

    if db != 'InstalledComponentsDB':
      result = MonitoringUtilities.monitorInstallation( 'DB', system, db )
      if not result[ 'OK' ]:
        print "ERROR: failed to register installation in database: %s" % result[ 'Message' ]
