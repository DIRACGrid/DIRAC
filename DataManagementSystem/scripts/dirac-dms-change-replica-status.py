#! /usr/bin/env python
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC.DataManagementSystem.Client.ReplicaManager import CatalogInterface
catalog = CatalogInterface()
import sys,os

if not len(sys.argv) == 4:
  print 'Usage: ./changeStatusForReplica.py <lfn | fileContainingLfns> SE Status'
  sys.exit()
else:
  inputFileName = sys.argv[1]
  se = sys.argv[2]
  newStatus = sys.argv[3]

if os.path.exists(inputFileName):
  inputFile = open(inputFileName,'r')
  string = inputFile.read()
  lfns = string.splitlines()
  inputFile.close()
else:
  lfns = [inputFileName]

res = catalog.getCatalogReplicas(lfns,True)
if not res['OK']:
  print res['Message']
  sys.exit()
replicas = res['Value']['Successful']

lfnDict = {}
for lfn in lfns:
  lfnDict[lfn] = {}
  lfnDict[lfn]['SE'] = se
  lfnDict[lfn]['Status'] = newStatus
  lfnDict[lfn]['PFN'] = replicas[lfn][se]

res = catalog.setCatalogReplicaStatus(lfnDict)
if not res['OK']:
  print res['Message']
if res['Value']['Failed']:
  print "Failed to update %d replica status" % len(res['Value']['Failed'])
if res['Value']['Successful']:
  print "Successfully updated %d replica status" % len(res['Value']['Successful'])
