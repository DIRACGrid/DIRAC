#! /usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-stager-monitor-jobs
# Author :  Daniela Remenska
########################################################################
"""
  Report the summary of the staging progress of jobs
"""
__RCSID__ = "$Id$"
import DIRAC
from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s  jobID [jobID] [jobID] ...' % Script.scriptName,
                                     'Arguments:',
                                     '  JobID: DIRAC job ID \n'
                                       ] ) )
Script.parseCommandLine( ignoreErrors = False )

args = Script.getPositionalArgs()

if not len( args ) < 2:
  Script.showHelp()

try:
  jobIDs = [int( arg ) for arg in sys.argv[1:]]
except:
  print 'DIRAC Job IDs must be integers'
  DIRAC.exit( 2 )

from DIRAC.StorageManagementSystem.Client.StorageManagerClient import StorageManagerClient
client = StorageManagerClient()


for jobID in jobIDs:
  res = client.getTaskSummary( jobID )
  if not res['OK']:
    print res['Message']
    continue
  taskInfo = res['Value']['TaskInfo']
  replicaInfo = res['Value']['ReplicaInfo']
  outStr = "%s: %s" % ( 'JobID'.ljust( 20 ), jobID )
  outStr = "%s\n%s: %s" % ( outStr, 'Status'.ljust( 20 ), taskInfo[jobID]['Status'] )
  outStr = "%s\n%s: %s" % ( outStr, 'SubmitTime'.ljust( 20 ), taskInfo[jobID]['SubmitTime'] )
  outStr = "%s\n%s: %s" % ( outStr, 'CompleteTime'.ljust( 20 ), taskInfo[jobID]['CompleteTime'] )
  outStr = "%s\nStaging files for this job:" % outStr
  for lfn, metadata in replicaInfo.items():
    outStr = "%s\n\t--------------------" % outStr
    outStr = "%s\n\t%s: %s" % ( outStr, 'LFN'.ljust( 8 ), lfn.ljust( 100 ) )
    outStr = "%s\n\t%s: %s" % ( outStr, 'SE'.ljust( 8 ), metadata['StorageElement'].ljust( 100 ) )
    outStr = "%s\n\t%s: %s" % ( outStr, 'PFN'.ljust( 8 ), str( metadata['PFN'] ).ljust( 100 ) )
    outStr = "%s\n\t%s: %s" % ( outStr, 'Status'.ljust( 8 ), metadata['Status'].ljust( 100 ) )
    outStr = "%s\n\t%s: %s" % ( outStr, 'Reason'.ljust( 8 ), str( metadata['Reason'] ).ljust( 100 ) )
  outStr = "%s\n----------------------" % outStr
  print outStr
DIRAC.exit( 0 )

''' Example:
JobID               : 5384634
Status              : StageSubmitted
SubmitTime          : 2013-05-14 14:25:19
CompleteTime        : None
Staging files for this job:
    --------------------
    LFN     : /lhcb/LHCb/Collision12/FULL.DST/00020846/0006/00020846_00060073_1.full.dst                          
    SE      : GRIDKA-RDST                                                                                         
    PFN     : srm://gridka-dCache.fzk.de/pnfs/gridka.de/lhcb/LHCb/Collision12/FULL.DST/00020846/0006/00020846_00060073_1.full.dst
    Status  : StageSubmitted                                                                                      
    Reason  : None                                                                                                
    --------------------
    LFN     : /lhcb/LHCb/Collision12/FULL.DST/00020846/0002/00020846_00024661_1.full.dst                          
    SE      : GRIDKA-RDST                                                                                         
    PFN     : srm://gridka-dCache.fzk.de/pnfs/gridka.de/lhcb/LHCb/Collision12/FULL.DST/00020846/0002/00020846_00024661_1.full.dst
    Status  : Staged                                                                                              
    Reason  : None                                                                                                                                                                                          
----------------------                                                                                                  
'''