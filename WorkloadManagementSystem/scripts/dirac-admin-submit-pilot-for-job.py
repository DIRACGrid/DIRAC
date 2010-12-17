#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-admin-submit-pilot-for-job
# Author :  Ricardo Graciani
########################################################################
"""
  Submit a DIRAC pilot for the given DIRAC job. Requires access to taskQueueDB and PilotAgentsDB
"""
__RCSID__ = "$Id$"
import DIRAC
from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... JobID ...' % Script.scriptName,
                                     'Arguments:',
                                     '  JobID:    DIRAC Job ID' ] ) )
Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

if len( args ) < 1:
  Script.showHelp()

from DIRAC.WorkloadManagementSystem.Agent.TaskQueueDirector import taskQueueDB, gLitePilotDirector

result = taskQueueDB.retrieveTaskQueues()
if not result['OK']:
  DIRAC.gLogger.error( 'Can not retrieve TaskQueue Definititions', result['Message'] )
  DIRAC.exit( 1 )

taskQueueDict = result['Value']

director = None
pilots = 0

for jobID in args:
  DIRAC.gLogger.info( 'Retrieving info for Job', jobID )
  result = taskQueueDB.getTaskQueueForJob( jobID )
  if not result['OK']:
    DIRAC.gLogger.error( 'Can not retrieve TaskQueue for Job', result['Message'] )
    continue
  tqID = result['Value']
  if not tqID in taskQueueDict:
    DIRAC.gLogger.error( 'Can not retrieve TaskQueue for Job' )
    continue
  taskQueueDict[tqID]['TaskQueueID'] = tqID
  if not director:
    director = gLitePilotDirector( 'gLite' )
    director.configure( '/', 'gLite' )

  result = director.submitPilots( taskQueueDict[tqID], 1 )

  if not result['OK']:
    DIRAC.gLogger.error( result['Message'] )
    continue

  pilots += result['Value']

  DIRAC.gLogger.info( 'Pilot Submitted' )

DIRAC.gLogger.info( '%s Pilots Submitted' % pilots )

DIRAC.exit()
