#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/DataManagementSystem/scripts/dirac-dms-remove-files.py,v 1.3 2009/09/29 15:27:45 acsmith Exp $
########################################################################
__RCSID__   = "$Id: dirac-dms-remove-files.py,v 1.3 2009/09/29 15:27:45 acsmith Exp $"
__VERSION__ = "$Revision: 1.3 $"
import sys,os
import DIRAC
from DIRAC import gLogger
from DIRAC.Core.Base import Script
Script.parseCommandLine()
args = Script.getPositionalArgs()
lfns = []
for inputFileName in args:
  if os.path.exists(inputFileName):
    inputFile = open(inputFileName,'r')
    string = inputFile.read()
    inputFile.close()
    lfns.extend(string.splitlines())
  else:
    lfns.append(inputFileName)

from DIRAC.Core.Utilities.List import sortList,breakListIntoChunks
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
rm = ReplicaManager()

errorReasons = {}
successfullyRemoved = 0
for lfnList in breakListIntoChunks(lfns,100):
  res = rm.removeFile(lfnList)
  if not res['OK']:
    gLogger.error("Failed to remove data",res['Message'])
    DIRAC.exit(-2)
  for lfn,reason in res['Value']['Failed'].items():
    if not reason in errorReasons.keys():
      errorReasons[reason] = []
    errorReasons[reason].append(lfn)
  successfullyRemoved+=len(res['Value']['Successful'].keys())

for reason,lfns in errorReasons.keys():
  gLogger.info("Failed to remove %d files with error: %s" % (len(lfns),reason))
gLogger.info("Successfully removed %d files" % successfullyRemoved)
DIRAC.exit(0)
