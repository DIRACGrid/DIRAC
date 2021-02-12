#!/usr/bin/env python
"""
This is a simple script that can be used for synchronizing pilot files
to the current directory.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import os
import json
import hashlib


from DIRAC import S_OK
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.DIRACScript import DIRACScript


includeMasterCS = True


def setNoMasterCS(optVal):
  global includeMasterCS
  includeMasterCS = False
  return S_OK()


@DIRACScript()
def main():
  global includeMasterCS
  Script.registerSwitch("n", "noMasterCS", "do not include master CS", setNoMasterCS)
  Script.parseCommandLine()

  from DIRAC import gLogger, exit as DIRACExit
  from DIRAC.WorkloadManagementSystem.Utilities.PilotCStoJSONSynchronizer import PilotCStoJSONSynchronizer

  ps = PilotCStoJSONSynchronizer()

  gLogger.verbose("Parameters for this sync:")
  gLogger.verbose("repo=" + ps.pilotRepo)
  gLogger.verbose("VO repo=" + ps.pilotVORepo)
  gLogger.verbose("projectDir=" + ps.projectDir)
  gLogger.verbose("pilotScriptsPath=" + ps.pilotScriptPath)
  gLogger.verbose("pilotVOScriptsPath=" + ps.pilotVOScriptPath)
  gLogger.verbose("pilotRepoBranch=" + ps.pilotRepoBranch)
  gLogger.verbose("pilotVORepoBranch=" + ps.pilotVORepoBranch)

  # pilot.json
  res = ps.getCSDict(includeMasterCS=includeMasterCS)
  if not res['OK']:
    DIRACExit(1)
  pilotDict = res['Value']
  print(json.dumps(pilotDict, indent=4, sort_keys=True))  # just print here as formatting is important
  with open('pilot.json', 'w') as jf:
    json.dump(pilotDict, jf)

  # pilot files
  res = ps.syncScripts()
  if not res['OK']:
    DIRACExit(1)
  gLogger.always(res['Value'])
  tarPath, tarFiles = res['Value']

  allFiles = [tarPath] + tarFiles + ['pilot.json']

  # checksums
  checksumDict = {}
  for pFile in allFiles:
    filename = os.path.basename(pFile)
    with open(pFile, 'rb') as fp:
      checksumDict[filename] = hashlib.sha512(fp.read()).hexdigest()
    cksPath = 'checksums.sha512'
  with open(cksPath, 'wt') as chksums:
    for filename, chksum in sorted(checksumDict.items()):
      # same as the output from sha512sum commands
      chksums.write('%s  %s\n' % (chksum, filename))

  allFiles = allFiles + [cksPath]

  print(allFiles)


if __name__ == "__main__":
  main()
