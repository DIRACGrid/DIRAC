#!/usr/bin/env python
########################################################################
# $HeadURL$
########################################################################
"""
Set the status of the replicas of given files at the provided SE
"""
__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     '\nUsage:',
                                     '  %s [option|cfgfile] ... <LFN|File> SE Status' % Script.scriptName,
                                     'Arguments:',
                                     '  LFN:      LFN',
                                     '  File:     File name containing a list of affected LFNs',
                                     '  SE:       Name of Storage Element',
                                     '  Status:   New Status for the replica'    ] ) )

Script.parseCommandLine( ignoreErrors = False )

import DIRAC
from DIRAC                                                          import gConfig, gLogger
from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
from DIRAC.Core.Utilities.List                                      import sortList
import os

args = Script.getPositionalArgs()
if not len( args ) == 3:
  Script.showHelp()

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

fc = FileCatalog()

replicaDict = {}
res = fc.getReplicas( lfns, allStatus = True )
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

res = fc.setReplicaStatus( lfnDict )
if not res['OK']:
  gLogger.error( "Failed to set catalog replica status.", res['Message'] )
  DIRAC.exit( -1 )
for lfn, error in res['Value']['Failed'].items():
  gLogger.error( "Failed to set replica status for file.", "%s:%s" % ( lfn, error ) )
gLogger.notice( "Successfully updated the status of %d files at %s." % ( len( res['Value']['Successful'].keys() ), storageElement ) )
DIRAC.exit( 0 )
