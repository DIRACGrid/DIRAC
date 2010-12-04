#!/usr/bin/env python
########################################################################
# $HeadURL$
########################################################################
__RCSID__   = "$Id$"

from DIRAC.Core.Base import Script

Script.setUsageMessage("""
Set the status of the replicas of given files at the provided SE 

Usage: 
  %s [<options>] <lfn|fileContainingLFNs> SE Status
""" % Script.scriptName)

Script.parseCommandLine( ignoreErrors = False )

import DIRAC
from DIRAC                                                          import gConfig, gLogger
from DIRAC.DataManagementSystem.Client.ReplicaManager               import ReplicaManager
from DIRAC.Core.Utilities.List                                      import sortList
import os

def usage():
  Script.showHelp()
  DIRAC.exit( 2 )

args = Script.getPositionalArgs()
if not len( args ) == 3:
  usage()
inputFileName = args[0]
storageElement = args[1]
status = args[2]

if os.path.exists( inputFileName ):
  inputFile = open( inputFileName, 'r' )
  string = inputFile.read()
  inputFile.close()
  lfns = sortList( string.splitlines() )
else:
  lfns = [inputFileName]

rm = ReplicaManager()

replicaDict = {}
res = rm.getCatalogReplicas( lfns, allStatus = True )
if not res['OK']:
  gLogger.error( "Failed to get catalog replicas.", res['Message'] )
  DIRAC.exit( -1 )
lfnDict = {}
for lfn, error in res['Value']['Failed'].items():
  gLogger.error( "Failed to get replicas for file.", "%s:%s" % ( lfn, error ) )
for lfn, replicas in res['Value']['Successful'].items():
  if not storageElement in replicas.keys():
    gLogger.error( "LFN not registered at provided storage element." , "%s %s" % ( lfn, storageElement ) )
  else:
    lfnDict[lfn] = {'SE':storageElement, 'PFN':replicas[storageElement], 'Status':status}
if not lfnDict:
  gLogger.error( "No files found at the supplied storage element." )
  DIRAC.exit( 2 )

res = rm.setCatalogReplicaStatus( lfnDict )
if not res['OK']:
  gLogger.error("Failed to set catalog replica status.",res['Message'])
  DIRAC.exit(-1)
for lfn,error in res['Value']['Failed'].items():
  gLogger.error("Failed to set replica status for file.","%s:%s" % (lfn,error))
gLogger.notice("Successfully updated the status of %d files at %s." % (len(res['Value']['Successful'].keys()),storageElement))
DIRAC.exit(0)

