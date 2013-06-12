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

taskIDs = []
for jobID in jobIDs:
  taskID = self._getTaskIDForJob( jobID, connection = connection ) # cannot call this!
  taskIDs.append(taskID)
# taskIDs = [5678094,5678099,5680538]
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
  else:
    outStr = "%s\n%s: %s" % ( outStr, "Resetting done!")
  
  outStr = "%s\n%s: %s" % ( outStr, "Removing actual stage request (to site) information from the stager....")
  
  #TODO DELETE FROM StageRequests WHERE ReplicaID in (%s);
else:
  outStr = "\nNo files found in the stager, to reset for these jobs...\n"
  #print "No files found in the stager, to reset for these jobs..."
print outStr
DIRAC.exit( 0 )