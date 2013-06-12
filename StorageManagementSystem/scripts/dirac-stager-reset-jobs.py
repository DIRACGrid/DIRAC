#! /usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-stager-reset-jobs
# Author :  Daniela Remenska
########################################################################
"""
  Reset any file staging requests that belong to particular job(s)
"""
_RCSID__ = "$Id$"
import DIRAC
from DIRAC.Core.Base import Script
from DIRAC                                     import gConfig, gLogger, exit as DIRACExit, S_OK, version

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s  jobID [jobID] [jobID] ...' % Script.scriptName,
                                     'Arguments:',
                                     '  JobID: DIRAC job ID \n'
                                       ] ) )
Script.parseCommandLine( ignoreErrors = False )

args = Script.getPositionalArgs()

if len( args ) < 1:
  Script.showHelp()

try:
  jobIDs = [int( arg ) for arg in args]
except:
  print 'DIRAC Job IDs must be integers'
  DIRAC.exit( 2 )

# jobIDs = [49892427,49664248,49664242]
from DIRAC.StorageManagementSystem.Client.StorageManagerClient import StorageManagerClient
client = StorageManagerClient()

from DIRAC.WorkloadManagementSystem.Client.JobMonitoringClient import JobMonitoringClient
jobClient = JobMonitoringClient()

taskIDs = []
for jobID in jobIDs:
  params = jobClient.getJobParameters(jobID)
  if params['Value']['StageRequest']:
    taskID = params['Value']['StageRequest']
    taskIDs.append(taskID)

cond = {}
cond['TaskID'] = taskIDs
res = client.getCacheReplicas( cond )

if not res['OK']:
  print res['Message']
cacheReplicaIDs = res['Value'].keys()
outStr = "\n--------------------"

if cacheReplicaIDs:
  outStr = "%s\n%s: %s" % ( outStr, "Resetting the status of all files for these jobs to \"New\"")
  res = client.updateReplicaStatus( cacheReplicaIDs, 'New' )
  if not res['OK']:
    print res['Message']
  outStr = "%s\n%s: %s" % ( outStr, "Resetting done!")
  
  outStr = "%s\n%s: %s" % ( outStr, "Removing actual stage request (to site) information from the stager....")
  res = client.removeStageRequests( cacheReplicaIDs )
  if not res['OK']:
    print res['Message']
  outStr = "%s\n%s: %s" % ( outStr, "Removed stage requests!") 
else:
  outStr = "\nNo files found in the stager, to reset for these jobs...\n"
 
print outStr
DIRAC.exit( 0 )