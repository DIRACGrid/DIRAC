#! /usr/bin/env python
import DIRAC
from DIRAC.Core.Base import Script

#unit = 'Something'
#Script.registerSwitch( "u:", "Unit=","   Unit to use [%s] (MB,GB,TB,PB)" % unit)
Script.parseCommandLine( ignoreErrors = False )
#for switch in Script.getUnprocessedSwitches():
#  if switch[0].lower() == "u" or switch[0].lower() == "unit":
#    unit = switch[1]

args = Script.getPositionalArgs()

def usage():
  print 'Usage: %s [<options>] [requestID]' % (Script.scriptName)
  print ' Get a summary of the stage request.'
  print ' Type "%s --help" for the available options and syntax' % Script.scriptName
  DIRAC.exit(2)

if not len(args) == 1:
  usage()
  DIRAC.exit(2)
else:
  try:
    taskID = int(args[0])
  except:
    print 'Stage requestID must be an integer'
    DIRAC.exit(2)

import os,sys,re

from DIRAC.StorageManagementSystem.Client.StorageManagerClient import StorageManagerClient
client = StorageManagerClient()

res = client.getTaskSummary(taskID)
if not res['OK']:
  print res['Message']
  DIRAC.exit(2)
taskInfo = res['Value']['TaskInfo']
replicaInfo = res['Value']['ReplicaInfo']
outStr = "%s: %s" % ('TaskID'.ljust(20),taskID)
outStr = "%s\n%s: %s" % (outStr,'Status'.ljust(20),taskInfo[taskID]['Status'])
outStr = "%s\n%s: %s" % (outStr,'Source'.ljust(20),taskInfo[taskID]['Source'])
outStr = "%s\n%s: %s" % (outStr,'SourceTaskID'.ljust(20),taskInfo[taskID]['SourceTaskID'])
outStr = "%s\n%s: %s" % (outStr,'CallBackMethod'.ljust(20),taskInfo[taskID]['CallBackMethod'])
outStr = "%s\n%s: %s" % (outStr,'SubmitTime'.ljust(20),taskInfo[taskID]['SubmitTime'])
outStr = "%s\n%s: %s" % (outStr,'CompleteTime'.ljust(20),taskInfo[taskID]['CompleteTime'])
for lfn,metadata in replicaInfo.items():
  outStr = "%s\n" % outStr
  outStr = "%s\n\t%s: %s" % (outStr,'LFN'.ljust(8),lfn.ljust(100))
  outStr = "%s\n\t%s: %s" % (outStr,'SE'.ljust(8),metadata['StorageElement'].ljust(100))  
  outStr = "%s\n\t%s: %s" % (outStr,'PFN'.ljust(8),str(metadata['PFN']).ljust(100))
  outStr = "%s\n\t%s: %s" % (outStr,'Size'.ljust(8),str(metadata['FileSize']).ljust(100))
  outStr = "%s\n\t%s: %s" % (outStr,'Status'.ljust(8),metadata['Status'].ljust(100))  
  outStr = "%s\n\t%s: %s" % (outStr,'Reason'.ljust(8),str(metadata['Reason']).ljust(100))  
print outStr   
