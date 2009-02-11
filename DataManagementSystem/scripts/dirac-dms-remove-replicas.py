#!/usr/bin/env python
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/DataManagementSystem/scripts/dirac-dms-remove-replicas.py,v 1.1 2009/02/11 20:05:32 acsmith Exp $
########################################################################
__RCSID__   = "$Id: dirac-dms-remove-replicas.py,v 1.1 2009/02/11 20:05:32 acsmith Exp $"
__VERSION__ = "$ $"

from DIRAC.Core.Utilities.List import sortList
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
rm = ReplicaManager()
import os,sys

if len(sys.argv) < 3:
  print 'Usage: ./dirac-dms-remove-replicas.py <LFN | fileContainingLFNs> SE'
  sys.exit()
else:
  inputFileName = sys.argv[1]
  storageElementName = sys.argv[2]

if os.path.exists(inputFileName):
  inputFile = open(inputFileName,'r')
  string = inputFile.read()
  lfns = string.splitlines()
  inputFile.close()
else:
  lfns = [inputFileName]

res = rm.removeReplica(storageElementName,lfns)
if not res['OK']:
  print res['Message']
  sys.exit()
for lfn in sortList(res['Value']['Successful'].keys()):
  print 'Successfully removed %s replica of %s' % (storageElementName,lfn)
for lfn in sortList(res['Value']['Failed'].keys()):
  message = res['Value']['Failed'][lfn]
  print 'Failed to remove %s replica of %s: %s' (storageElementName,lfn,message)
