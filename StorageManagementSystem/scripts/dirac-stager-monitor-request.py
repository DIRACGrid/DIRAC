#! /usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-stager-monitor
# Author :  Andrew C. Smith
########################################################################
"""
  Report the summary of the stage task from the DB.
"""
__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... Request ...' % Script.scriptName,
                                     'Arguments:',
                                     '  Request:  ID of the Stage request in the StorageManager' ] ) )
Script.parseCommandLine( ignoreErrors = False )

args = Script.getPositionalArgs()

if not len( args ) == 1:
  Script.showHelp()

from DIRAC import exit as DIRACExit

try:
  taskID = int( args[0] )
except:
  print 'Stage requestID must be an integer'
  DIRACExit( 2 )

from DIRAC.StorageManagementSystem.Client.StorageManagerClient import StorageManagerClient
client = StorageManagerClient()

res = client.getTaskSummary( taskID )
if not res['OK']:
  print res['Message']
  DIRACExit( 2 )
taskInfo = res['Value']['TaskInfo']
replicaInfo = res['Value']['ReplicaInfo']
outStr = "%s: %s" % ( 'TaskID'.ljust( 20 ), taskID )
outStr = "%s\n%s: %s" % ( outStr, 'Status'.ljust( 20 ), taskInfo[taskID]['Status'] )
outStr = "%s\n%s: %s" % ( outStr, 'Source'.ljust( 20 ), taskInfo[taskID]['Source'] )
outStr = "%s\n%s: %s" % ( outStr, 'SourceTaskID'.ljust( 20 ), taskInfo[taskID]['SourceTaskID'] )
outStr = "%s\n%s: %s" % ( outStr, 'CallBackMethod'.ljust( 20 ), taskInfo[taskID]['CallBackMethod'] )
outStr = "%s\n%s: %s" % ( outStr, 'SubmitTime'.ljust( 20 ), taskInfo[taskID]['SubmitTime'] )
outStr = "%s\n%s: %s" % ( outStr, 'CompleteTime'.ljust( 20 ), taskInfo[taskID]['CompleteTime'] )
for lfn, metadata in replicaInfo.items():
  outStr = "%s\n" % outStr
  outStr = "%s\n\t%s: %s" % ( outStr, 'LFN'.ljust( 8 ), lfn.ljust( 100 ) )
  outStr = "%s\n\t%s: %s" % ( outStr, 'SE'.ljust( 8 ), metadata['StorageElement'].ljust( 100 ) )
  outStr = "%s\n\t%s: %s" % ( outStr, 'PFN'.ljust( 8 ), str( metadata['PFN'] ).ljust( 100 ) )
  outStr = "%s\n\t%s: %s" % ( outStr, 'Size'.ljust( 8 ), str( metadata['FileSize'] ).ljust( 100 ) )
  outStr = "%s\n\t%s: %s" % ( outStr, 'Status'.ljust( 8 ), metadata['Status'].ljust( 100 ) )
  outStr = "%s\n\t%s: %s" % ( outStr, 'Reason'.ljust( 8 ), str( metadata['Reason'] ).ljust( 100 ) )
print outStr
