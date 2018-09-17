#!/usr/bin/env python
########################################################################
# File :   dirac-externals-requirements
# Author : Adri/Federico/Andrei
########################################################################
""" If RequiredExternals section is found in releases.cfg of any extension,
    then some python packages to install with pip may be found. This script
    will install the requested modules.

    The command is called from the dirac-install general installation command.
"""

import os
import sys
import commands

from DIRAC.Core.Base import Script
Script.disableCS()

from DIRAC import gLogger, rootPath, S_OK
from DIRAC.Core.Utilities.CFG import CFG

__RCSID__ = "$Id$"

# Default installation type
instType = "server"


def setInstallType(val):
  global instType
  instType = val
  return S_OK()


Script.registerSwitch("t:", "type=", "Installation type. 'server' by default.", setInstallType)
Script.parseCommandLine(ignoreErrors=True)


def pipInstall(package, switches=""):
  # The right pip should be in the PATH, which is the case after sourcing the DIRAC bashrc
  cmd = "pip install --trusted-host pypi.python.org %s %s" % (switches, package)
  gLogger.notice("Executing %s" % cmd)
  return commands.getstatusoutput(cmd)


# Collect all the requested python modules to install
reqDict = {}


for entry in os.listdir(rootPath):
  if len(entry) < 5 or entry.find("DIRAC") != len(entry) - 5:
    continue
  reqFile = os.path.join(rootPath, entry, "releases.cfg")
  try:
    with open(reqFile, "r") as extfd:
      reqCFG = CFG().loadFromBuffer(extfd.read())
  except BaseException:
    gLogger.verbose("%s not found" % reqFile)
    continue
  reqList = reqCFG.getOption("/RequiredExternals/%s" % instType.capitalize(), [])
  if not reqList:
    gLogger.verbose("%s does not have requirements for %s installation" % (entry, instType))
    continue
  for req in reqList:
    reqName = False
    reqCond = ""
    for cond in ("==", ">="):
      iP = cond.find(req)
      if iP > 0:
        reqName = req[:iP]
        reqCond = req[iP:]
        break
    if not reqName:
      reqName = req
    if reqName not in reqDict:
      reqDict[reqName] = (reqCond, entry)
    else:
      gLogger.notice("Skipping %s, it's already requested by %s" % (reqName, reqDict[reqName][1]))

if not reqDict:
  gLogger.notice("No extra python module requested to be installed")
  sys.exit(0)

for reqName in reqDict:
  package = "%s%s" % (reqName, reqDict[reqName][0])
  gLogger.notice("Requesting installation of %s" % package)
  status, output = pipInstall(package)
  if status != 0:
    gLogger.error(output)
  else:
    gLogger.notice("Successfully installed %s" % package)
