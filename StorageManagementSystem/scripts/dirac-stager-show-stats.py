#! /usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-stager-show-stats
# Author :  Daniela Remenska
########################################################################
"""
Reports breakdown of file(s) number/size in different staging states across Storage Elements.
Currently used Cache per SE is also reported. (active pins)
"""

__RCSID__ = "$Id$"
from DIRAC.Core.Base import Script
from DIRAC                                     import gConfig, gLogger, exit as DIRACExit, S_OK, version

Script.parseCommandLine( ignoreErrors = False )
from DIRAC.StorageManagementSystem.Client.StorageManagerClient import StorageManagerClient
client = StorageManagerClient()

res = client.getCacheReplicasSummary()
if not res['OK']:
  print res['Message']
  DIRACExit( 2 )      
stagerInfo = res['Value']
outStr ="\n"
outStr = "%s %s" %(outStr, "Status".ljust(20)) 
outStr = "%s %s" %(outStr, "SE".ljust(20))
outStr = "%s %s" %(outStr, "NumberOfFiles".ljust(20))  
outStr = "%s %s" %(outStr, "Size(GB)".ljust(20))   
outStr = "%s\n--------------------------------------------------------------------------\n" % outStr    
if stagerInfo:
  for sid in stagerInfo:
    outStr = "%s %s" %(outStr, stagerInfo[sid]['Status'].ljust( 20 ))
    outStr = "%s %s" %(outStr, stagerInfo[sid]['SE'].ljust( 20 ))
    outStr = "%s %s" %(outStr, str(stagerInfo[sid]['NumFiles']).ljust( 20 ))
    outStr = "%s %s\n" %(outStr, str(stagerInfo[sid]['SumFiles']).ljust( 20 ))
else:
  outStr ="%s %s" % (outStr, "Nothing to see here...Bye")
outStr ="%s %s" % (outStr, "\nWARNING: the Size for files with Status=New is not yet determined at the point of selection!\n\n")
outStr ="%s %s" %(outStr,"--------------------- current status of the SE Caches from the DB-----------")
res = client.getSubmittedStagePins()
if not res['OK']:
  print res['Message']
  DIRACExit(2)
storageElementUsage = res['Value']
if storageElementUsage:
  for storageElement in storageElementUsage.keys():
    seDict = storageElementUsage[storageElement]
    seDict['TotalSize'] = seDict['TotalSize'] / ( 1000 * 1000 * 1000.0 )
    outStr ="%s\n %s: %s replicas with a size of %.3f GB." % (outStr, storageElement.ljust( 15 ), str( seDict['Replicas'] ).rjust( 6 ), seDict['TotalSize'] ) 
else:
  outStr ="%s %s" % (outStr, "StageRequest.getStorageUsage: No active stage/pin requests found." )    
print outStr
DIRACExit( 0 )
'''Example:
dirac-stager-show-stats.py 

 Status               SE                   NumberOfFiles        Size(GB)            
--------------------------------------------------------------------------
 Staged               GRIDKA-RDST          1                    4.5535              
 StageSubmitted       GRIDKA-RDST          5                    22.586              
 Waiting              PIC-RDST             3                    13.6478             
 
WARNING: the Size for files with Status=New is not yet determined at the point of selection!

 --------------------- current status of the SE Caches from the DB-----------
 GRIDKA-RDST    :      6 replicas with a size of 29.141 GB.
'''
