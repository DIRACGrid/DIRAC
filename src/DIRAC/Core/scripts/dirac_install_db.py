#!/usr/bin/env python
"""
Create a new DB in the MySQL server
"""
# Script initialization and parseCommandLine
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.DIRACScript import DIRACScript


@DIRACScript()
def main():
  Script.registerArgument(["DB: Name of the Database"])
  Script.parseCommandLine()
  args = Script.getPositionalArgs()

  # Script imports
  from DIRAC import gConfig
  from DIRAC.FrameworkSystem.Client.ComponentInstaller import gComponentInstaller
  from DIRAC.FrameworkSystem.Utilities import MonitoringUtilities

  gComponentInstaller.exitOnError = True
  gComponentInstaller.getMySQLPasswords()
  for db in args:
    result = gComponentInstaller.installDatabase(db)
    if not result['OK']:
      print("ERROR: failed to correctly install %s" % db, result['Message'])
    else:
      extension, system = result['Value']
      gComponentInstaller.addDatabaseOptionsToCS(gConfig, system, db, overwrite=True)

      if db != 'InstalledComponentsDB':
        result = MonitoringUtilities.monitorInstallation('DB', system, db)
        if not result['OK']:
          print("ERROR: failed to register installation in database: %s" % result['Message'])


if __name__ == "__main__":
  main()
