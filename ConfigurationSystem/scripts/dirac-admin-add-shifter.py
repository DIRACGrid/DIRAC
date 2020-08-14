#!/usr/bin/env python
########################################################################
# File :   dirac-admin-add-shifter
# Author : Federico Stagni
########################################################################
""" Adds or modify a shifter, in the operations section of the CS
"""
__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script
from DIRAC.ConfigurationSystem.Client.CSAPI import CSAPI
from DIRAC import exit as DIRACExit, gLogger

if __name__ == "__main__":

  Script.setUsageMessage('\n'.join([__doc__.split('\n')[1],
                                    'Usage:',
                                    '  %s [option|cfgfile] ... ShifterRole UserName DIRACGroup ...' % Script.scriptName,
                                    'Arguments:',
                                    '  ShifterRole: Name of the shifter role, e.g. DataManager',
                                    '  UserName: A user name, as registered in Registry section',
                                    '  DIRACGroup: DIRAC Group, e.g. diracAdmin (the user has to have this role)']))
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
