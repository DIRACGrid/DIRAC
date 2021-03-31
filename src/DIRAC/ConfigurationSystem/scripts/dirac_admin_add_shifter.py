#!/usr/bin/env python
########################################################################
# File :   dirac_admin_add_shifter
# Author : Federico Stagni
########################################################################
"""
Adds or modify a shifter, in the operations section of the CS

Usage:
  dirac-admin-add-shifter [options] ... ShifterRole UserName DIRACGroup ...

Arguments:
  ShifterRole:  Name of the shifter role, e.g. DataManager
  UserName:     A user name, as registered in Registry section
  DIRACGroup:   DIRAC Group, e.g. diracAdmin (the user has to have this role)
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.DIRACScript import DIRACScript
from DIRAC.ConfigurationSystem.Client.CSAPI import CSAPI
from DIRAC import exit as DIRACExit, gLogger


@DIRACScript()
def main():
  Script.parseCommandLine(ignoreErrors=True)
  args = Script.getPositionalArgs()

  csAPI = CSAPI()

  if len(args) < 3:
    Script.showHelp(exitCode=1)

  shifterRole = args[0]
  userName = args[1]
  diracGroup = args[2]

  res = csAPI.addShifter({shifterRole: {'User': userName, 'Group': diracGroup}})
  if not res['OK']:
    gLogger.error("Could not add shifter", ": " + res['Message'])
    DIRACExit(1)
  res = csAPI.commit()
  if not res['OK']:
    gLogger.error("Could not add shifter", ": " + res['Message'])
    DIRACExit(1)
  gLogger.notice("Added shifter %s as user %s with group %s" % (shifterRole, userName, diracGroup))


if __name__ == "__main__":
  main()
