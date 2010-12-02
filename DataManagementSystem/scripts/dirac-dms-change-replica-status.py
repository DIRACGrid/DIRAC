#! /usr/bin/env python
########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/DIRAC/trunk/DIRAC/DataManagementSystem/scripts/dirac-dms-catalog-metadata.py $
########################################################################
__RCSID__   = "$Id: dirac-dms-catalog-metadata.py 27909 2010-08-11 09:08:28Z acsmith $"

from DIRAC.Core.Base import Script 

Script.setUsageMessage("""
Change status of replica of a given file or a list of files at a given Storage Element 

Usage:
   %s <lfn | fileContainingLfns> <SE> <status>
""" % Script.scriptName)

Script.parseCommandLine()

from DIRAC.DataManagementSystem.Client.ReplicaManager import CatalogInterface
catalog = CatalogInterface()
import sys,os

if not len(sys.argv) == 4:
  Script.showHelp()
  DIRAC.exit( -1 )
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
  print "ERROR:",res['Message']
if res['Value']['Failed']:
  print "Failed to update %d replica status" % len(res['Value']['Failed'])
if res['Value']['Successful']:
  print "Successfully updated %d replica status" % len(res['Value']['Successful'])
