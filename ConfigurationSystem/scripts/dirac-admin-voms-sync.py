#!/usr/bin/env python
########################################################################
# File :   dirac-admin-voms-sync
# Author : Andrei Tsaregorodtsev
########################################################################
"""
  Synchronize VOMS user data with the DIRAC Registry
"""

__RCSID__ = "$Id$"

from DIRAC import gLogger, exit as DIRACExit, S_OK
from DIRAC.Core.Base import Script

dryRun = False


def setDryRun(value):
  global dryRun
  dryRun = True
  return S_OK()


voName = None


def setVO(value):
  global voName
  voName = value
  return S_OK()


Script.registerSwitch("V:", "vo=", "VO name", setVO)
Script.registerSwitch("D", "dryRun", "Dry run", setDryRun)
Script.setUsageMessage('\n'.join([__doc__.split('\n')[1],
                                  'Usage:',
                                  '  %s [option|cfgfile] ...' % Script.scriptName]
                                 ))

Script.parseCommandLine(ignoreErrors=True)

from DIRAC.ConfigurationSystem.Client.VOMS2CSSyncronizer import VOMS2CSSynchronizer

vomsSync = VOMS2CSSynchronizer(voName)
result = vomsSync.syncCSWithVOMS()
if not result['OK']:
  gLogger.error("Failed to synchronize user data")
  DIRACExit(-1)

resultDict = result['Value']
newUsers = resultDict.get("NewUsers", [])
modUsers = resultDict.get("ModifiedUsers", [])
delUsers = resultDict.get("DeletedUsers", [])
susUsers = resultDict.get("SuspendedUsers", [])
gLogger.notice("\nUser results: new %d, modified %d, deleted %d, new/suspended %d" %
               (len(newUsers), len(modUsers), len(delUsers), len(susUsers)))

for msg in resultDict["AdminMessages"]["Info"]:
  gLogger.notice(msg)

csapi = resultDict.get("CSAPI")
if csapi and csapi.csModified:
  if dryRun:
    gLogger.notice("There are changes to Registry ready to commit, skipped because of dry run")
  else:
    yn = raw_input("There are changes to Registry ready to commit, do you want to proceed [default yes] [yes|no]:")
    if yn == '' or yn.lower() == 'y':
      result = csapi.commitChanges()
      if not result['OK']:
        gLogger.error("Could not commit configuration changes", result['Message'])
      else:
        gLogger.notice("Registry changes committed for VO %s" % voName)
else:
  gLogger.notice("No changes to Registry for VO %s" % voName)

result = vomsSync.getVOUserReport()
if not result['OK']:
  gLogger.error('Failed to generate user data report')
  DIRACExit(-1)

gLogger.notice("\n" + result['Value'])
