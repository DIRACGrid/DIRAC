#! /usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-stager-stage-files
# Author :  Daniela Remenska
########################################################################
from DIRAC.Core.Base import Script
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... SE FileName [...]' % Script.scriptName,
                                     'Arguments:',
                                     '  SE:       Name of Storage Element',
                                     '  FileName: LFN to Stage (or local file with list of LFNs)' ] ) )

Script.parseCommandLine( ignoreErrors = True )

args = Script.getPositionalArgs()

if len( args ) < 2:
  Script.showHelp()

seName = args[0]
fileName = args[1]

import os
import DIRAC
from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC.StorageManagementSystem.Client.StorageManagerClient import StorageManagerClient


if os.path.exists( fileName ):
  try:
    lfnFile = open( fileName )
    lfns = [ k.strip() for k in lfnFile.readlines() ]
    lfnFile.close()
  except Exception:
    DIRAC.gLogger.exception( 'Can not open file', fileName )
    DIRAC.exit( -1 )

else:
  lfns = args[1:]

dirac = Dirac()
res = dirac.getReplicas( lfns[0:], active = True, printOutput = False )

if not res['OK']:
  DIRAC.gLogger.error( res['Message'] )
  DIRAC.exit( -1 )

stagerClient = StorageManagerClient()
stageLfns = []

for lfn, replicas in res['Value']['Successful'].items():
  if seName in replicas:
    stageLfns.append( lfn )
    if len( stageLfns ) >= 10:
      # Use a fake JobID = 0
      request = stagerClient.setRequest( { seName : stageLfns }, 'WorkloadManagement',
                                         'updateJobFromStager@WorkloadManagement/JobStateUpdate', 0 )
      if request['OK']:
        DIRAC.gLogger.notice( 'Stage Request submitted for %s replicas:' % len( stageLfns ), request['Value'] )
        stageLfns = []
      else:
        DIRAC.gLogger.error( 'Failed to submit Stage Request' )
        DIRAC.gLogger.error( request['Message'] )
        if len( stageLfns ) >= 20:
          # if after 10 attempts we do not manage to submit a request, abort execution
          DIRAC.exit( -1 )


if stageLfns:
  request = stagerClient.setRequest( { seName : stageLfns }, 'WorkloadManagement',
                                     'updateJobFromStager@WorkloadManagement/JobStateUpdate', 0 )
  if request['OK']:
    DIRAC.gLogger.notice( 'Stage Request submitted for %s replicas:' % len( stageLfns ), request['Value'] )
  else:
    DIRAC.gLogger.error( 'Failed to submit Stage Request' )
    DIRAC.gLogger.error( request['Message'] )
    DIRAC.exit( -1 )

DIRAC.exit()
