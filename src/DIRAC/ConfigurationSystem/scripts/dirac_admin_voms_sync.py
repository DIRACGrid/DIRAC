#!/usr/bin/env python
########################################################################
# File :   dirac-admin-voms-sync
# Author : Andrei Tsaregorodtsev
########################################################################
"""
Synchronize VOMS user data with the DIRAC Registry
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import six

from DIRAC import gLogger, exit as DIRACExit, S_OK
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.DIRACScript import DIRACScript
from DIRAC.ConfigurationSystem.Client.VOMS2CSSynchronizer import VOMS2CSSynchronizer
from DIRAC.Core.Utilities.Proxy import executeWithUserProxy
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOOption


dryRun = False
voName = None


def setDryRun(value):
  global dryRun
  dryRun = True
  return S_OK()


def setVO(value):
  global voName
  voName = value
  return S_OK()


@DIRACScript()
def main():
  Script.registerSwitch("V:", "vo=", "VO name", setVO)
  Script.registerSwitch("D", "dryRun", "Dry run", setDryRun)
  Script.parseCommandLine(ignoreErrors=True)

  @executeWithUserProxy
  def syncCSWithVOMS(vomsSync):
    return vomsSync.syncCSWithVOMS()

  def getVOAdmin(voName):
    voAdminUser = getVOOption(voName, "VOAdmin")
    voAdminGroup = getVOOption(voName, "VOAdminGroup", getVOOption(voName, "DefaultGroup"))
    return voAdminUser, voAdminGroup

  voAdminUser, voAdminGroup = getVOAdmin(voName)

  vomsSync = VOMS2CSSynchronizer(voName)
  result = syncCSWithVOMS(vomsSync,  # pylint: disable=unexpected-keyword-arg
                          proxyUserName=voAdminUser,
                          proxyUserGroup=voAdminGroup)
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
      yn = six.moves.input("There are changes to Registry ready to commit, do you want to proceed ? [Y|n]:")
      if yn == '' or yn[0].lower() == 'y':
        result = csapi.commitChanges()
        if not result['OK']:
          gLogger.error("Could not commit configuration changes", result['Message'])
        else:
          gLogger.notice("Registry changes committed for VO %s" % voName)
      else:
        gLogger.notice("Registry changes are not committed")
  else:
    gLogger.notice("No changes to Registry for VO %s" % voName)

  result = vomsSync.getVOUserReport()
  if not result['OK']:
    gLogger.error('Failed to generate user data report')
    DIRACExit(-1)

  gLogger.notice("\n" + result['Value'])


if __name__ == "__main__":
  main()
