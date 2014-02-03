#! /usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-stager-monitor-file
# Author :  Daniela Remenska
########################################################################
"""
-gives monitoring information regarding a staging file uniquely identified with (LFN,SE):
-   - status
-   - last update
-   - jobs requesting this file to be staged
-   - SRM requestID
-   - pin expiry time
-   - pin length
"""
__RCSID__ = "$Id$"
from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s  LFN SE ...' % Script.scriptName,
                                     'Arguments:',
                                     '  LFN: LFN of the staging file \n',
                                     '  SE: Storage Element for the staging file \n'
                                       ] ) )
Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()
if len( args ) < 2:
  Script.showHelp()

from DIRAC import exit as DIRACExit

lfn = args[0]
se = args[1]

from DIRAC.StorageManagementSystem.Client.StorageManagerClient import StorageManagerClient
client = StorageManagerClient()
res = client.getCacheReplicas( {'LFN':lfn,'SE':se} )
if not res['OK']:
  print res['Message']
cacheReplicaInfo = res['Value']
if cacheReplicaInfo:
  replicaID = cacheReplicaInfo.keys()[0]
  outStr = "\n--------------------"
  outStr = "%s\n%s: %s" % ( outStr, 'LFN'.ljust( 8 ), cacheReplicaInfo[replicaID]['LFN'].ljust( 100 ) )
  outStr = "%s\n%s: %s" % ( outStr, 'SE'.ljust( 8 ), cacheReplicaInfo[replicaID]['SE'].ljust( 100 ) )
  outStr = "%s\n%s: %s" % ( outStr, 'PFN'.ljust( 8 ), cacheReplicaInfo[replicaID]['PFN'].ljust( 100 ) )
  outStr = "%s\n%s: %s" % ( outStr, 'Status'.ljust( 8 ), cacheReplicaInfo[replicaID]['Status'].ljust( 100 ) )
  outStr = "%s\n%s: %s" % ( outStr, 'LastUpdate'.ljust( 8 ), str(cacheReplicaInfo[replicaID]['LastUpdate']).ljust( 100 ) )
  outStr = "%s\n%s: %s" % ( outStr, 'Reason'.ljust( 8 ), str( cacheReplicaInfo[replicaID]['Reason']).ljust( 100 ) )
  
  resTasks = client.getTasks({'ReplicaID':replicaID})

  if resTasks['OK']:
    #print resTasks['Message']
    outStr = '%s\nJob IDs requesting this file to be staged:'.ljust( 8) % outStr
    tasks = resTasks['Value']
    for tid in tasks.keys():
      outStr = '%s %s ' % (outStr, tasks[tid]['SourceTaskID'])  

  resStageRequests = client.getStageRequests({'ReplicaID':replicaID})

  if not resStageRequests['OK']:
    print resStageRequests['Message']

  if resStageRequests['Records']:
    stageRequests = resStageRequests['Value']
    outStr = "%s\n------SRM staging request info--------------" % outStr
    for srid in stageRequests.keys():
      outStr = "%s\n%s: %s" % ( outStr, 'SRM RequestID'.ljust( 8 ), stageRequests[srid]['RequestID'].ljust( 100 ) )
      outStr = "%s\n%s: %s" % ( outStr, 'SRM StageStatus'.ljust( 8 ), stageRequests[srid]['StageStatus'].ljust( 100 ) )
      outStr = "%s\n%s: %s" % ( outStr, 'SRM StageRequestSubmitTime'.ljust( 8 ), str(stageRequests[srid]['StageRequestSubmitTime']).ljust( 100 ) )
      outStr = "%s\n%s: %s" % ( outStr, 'SRM StageRequestCompletedTime'.ljust( 8 ), str(stageRequests[srid]['StageRequestCompletedTime']).ljust( 100 ) )
      outStr = "%s\n%s: %s" % ( outStr, 'SRM PinExpiryTime'.ljust( 8 ), str(stageRequests[srid]['PinExpiryTime']).ljust( 100 ) )
      outStr = "%s\n%s: %s sec" % ( outStr, 'SRM PinLength'.ljust( 8 ), str(stageRequests[srid]['PinLength']).ljust( 100 ) )
  else:
    outStr = '%s\nThere are no staging requests submitted to the site yet.'.ljust( 8) % outStr
else:
  outStr = "\nThere is no such file requested for staging. Check for typo's!"   
  #Script.showHelp() 
print outStr

DIRACExit( 0 )

''' Example:
dirac-stager-monitor-file.py /lhcb/LHCb/Collision12/FULL.DST/00020846/0005/00020846_00056603_1.full.dst GRIDKA-RDST
--------------------
LFN     : /lhcb/LHCb/Collision12/FULL.DST/00020846/0005/00020846_00056603_1.full.dst                          
SE      : GRIDKA-RDST                                                                                         
PFN     : srm://gridka-dCache.fzk.de/pnfs/gridka.de/lhcb/LHCb/Collision12/FULL.DST/00020846/0005/00020846_00056603_1.full.dst
Status  : StageSubmitted                                                                                      
LastUpdate: 2013-06-11 18:13:40                                                                                 
Reason  : None                                                                                                
Jobs requesting this file to be staged: 48518896 
------SRM staging request info--------------
SRM RequestID: -1768636375                                                                                         
SRM StageStatus: StageSubmitted                                                                                      
SRM StageRequestSubmitTime: 2013-06-11 18:13:38                                                                                 
SRM StageRequestCompletedTime: None                                                                                                
SRM PinExpiryTime: None                                                                                                
SRM PinLength: 43200         
'''
