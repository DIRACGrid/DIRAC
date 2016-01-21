#! /usr/bin/env python
########################################################################
# $HeadURL$
########################################################################
__RCSID__   = "$Id$"

from DIRAC           import exit as DIRACExit
from DIRAC.Core.Base import Script 

Script.setUsageMessage("""
Change status of replica of a given file or a list of files at a given Storage Element 

Usage:
   %s <lfn | fileContainingLfns> <SE> <status>
""" % Script.scriptName)

Script.parseCommandLine()

from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
catalog = FileCatalog()
import os
args = Script.getPositionalArgs()
if not len(args) == 3:
  Script.showHelp()
  DIRACExit( -1 )
else:
  inputFileName = args[0]
  se = args[1]
  newStatus = args[2]

if os.path.exists(inputFileName):
  inputFile = open(inputFileName,'r')
  string = inputFile.read()
  lfns = string.splitlines()
  inputFile.close()
else:
  lfns = [inputFileName]

res = catalog.getReplicas( lfns, True )
if not res['OK']:
  print res['Message']
  DIRACExit( -1 )
replicas = res['Value']['Successful']

lfnDict = {}
for lfn in lfns:
  lfnDict[lfn] = {}
  lfnDict[lfn]['SE'] = se
  lfnDict[lfn]['Status'] = newStatus
  lfnDict[lfn]['PFN'] = replicas[lfn][se]

res = catalog.setReplicaStatus( lfnDict )
if not res['OK']:
  print "ERROR:",res['Message']
if res['Value']['Failed']:
  print "Failed to update %d replica status" % len(res['Value']['Failed'])
if res['Value']['Successful']:
  print "Successfully updated %d replica status" % len(res['Value']['Successful'])
