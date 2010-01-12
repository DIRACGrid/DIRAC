#!/usr/bin/env python
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()
########################################################################
# $Header: /local/reps/dirac/DIRAC3/DIRAC/DataManagementSystem/scripts/dirac-dms-remove-catalog-replicas.py,v 1.2 2009/03/19 17:45:06 acsmith Exp $
########################################################################
__RCSID__   = "$Id$"
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

res = rm.removeReplicaFromCatalog(storageElementName,lfns)
if not res['OK']:
  print res['Message']
  sys.exit()
for lfn in sortList(res['Value']['Failed'].keys()):
  message = res['Value']['Failed'][lfn]
  print 'Failed to remove %s replica of %s: %s' % (storageElementName,lfn,message)
print 'Successfully remove %d catalog replicas at %s' % (len(res['Value']['Successful']),storageElementName)
