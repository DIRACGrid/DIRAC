#!/usr/bin/env python
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/DataManagementSystem/scripts/dirac-dms-remove-files.py,v 1.2 2009/08/31 16:22:56 acsmith Exp $
########################################################################
__RCSID__   = "$Id: dirac-dms-remove-files.py,v 1.2 2009/08/31 16:22:56 acsmith Exp $"
__VERSION__ = "$ $"

from DIRAC.Core.Utilities.List import sortList,breakListIntoChunks
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
rm = ReplicaManager()
import os,sys

if len(sys.argv) < 2:
  print 'Usage: ./dirac-dms-remove-replicas.py <LFN | fileContainingLFNs>'
  sys.exit()
else:
  inputFileName = sys.argv[1]

if os.path.exists(inputFileName):
  inputFile = open(inputFileName,'r')
  string = inputFile.read()
  lfns = string.splitlines()
  inputFile.close()
else:
  lfns = [inputFileName]

for lfnList in breakListIntoChunks(lfns,100):
  res = rm.removeFile(lfnList)
  if not res['OK']:
    print res['Message']
    sys.exit()
  for lfn in sortList(res['Value']['Successful'].keys()):
    print 'Successfully removed %s' % (lfn)
  for lfn in sortList(res['Value']['Failed'].keys()):
    message = res['Value']['Failed'][lfn]
    print 'Failed to remove %s: %s' % (lfn,message)
