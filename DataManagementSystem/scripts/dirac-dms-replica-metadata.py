#!/usr/bin/env python
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/DataManagementSystem/scripts/dirac-dms-replica-metadata.py,v 1.1 2009/06/09 12:58:36 acsmith Exp $
########################################################################
__RCSID__   = "$Id: dirac-dms-replica-metadata.py,v 1.1 2009/06/09 12:58:36 acsmith Exp $"
__VERSION__ = "$ $"

from DIRAC import gLogger
gLogger.setLevel('ALWAYS')
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
import os,sys

if not len(sys.argv) == 3:
  print 'Usage: ./dirac-dms-replica-metadata <lfn | fileContainingLfns> SE'
  sys.exit()
else:
  inputFileName = sys.argv[1]
  storageElement = sys.argv[2]

if os.path.exists(inputFileName):
  inputFile = open(inputFileName,'r')
  string = inputFile.read()
  lfns = string.splitlines()
  inputFile.close()
else:
  lfns = [inputFileName]

rm = ReplicaManager()
res = rm.getReplicaMetadata(lfns,storageElement)
if not res['OK']:
  print res['Message']
print '%s %s %s %s' % ('File'.ljust(100),'Migrated'.ljust(8),'Cached'.ljust(8),'Size (bytes)'.ljust(10))
for lfn,metadata in res['Value']['Successful'].items():
  print '%s %s %s %s' % (lfn.ljust(100),str(metadata['Migrated']).ljust(8),str(metadata['Cached']).ljust(8),str(metadata['Size']).ljust(10))
for lfn,reason in res['Value']['Failed'].items():
  print '%s %s' % (lfn.ljust(100),reason.ljust(8))
