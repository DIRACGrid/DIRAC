#! /usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-stager-show-stats
# Author :  Daniela Remenska
########################################################################
"""
Reports breakdown of file(s) number/size in different staging states across storage elements 
"""

__RCSID__ = "$Id$"
import DIRAC
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
outStr ="%s %s" % (outStr, "\nWARNING: the Size for files with Status=New is not yet determined at the point of selection!\n")
print outStr
DIRACExit( 0 )
'''Example:
 dirac-stager-show-stats.py 

 Status               SE                   NumberOfFiles        Size(GB)            
--------------------------------------------------------------------------
 Staged               GRIDKA-RDST          1                    4.5535              
 StageSubmitted       GRIDKA-RDST          5                    22.586  
 '''      