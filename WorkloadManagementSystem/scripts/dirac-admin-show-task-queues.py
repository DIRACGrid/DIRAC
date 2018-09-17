#!/usr/bin/env python
########################################################################
# File :    dirac-admin-show-task-queues
# Author :  Ricardo Graciani
########################################################################
"""
   Show details of currently active Task Queues
"""
__RCSID__ = "$Id$"

import sys

from DIRAC import S_OK
from DIRAC.Core.Base import Script
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities.PrettyPrint import printTable

from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getUsernameForDN

verbose = False

def setVerbose( optVal ):
  global verbose
  verbose = True
  return S_OK()

taskQueueID = 0
def setTaskQueueID( optVal ):
  global taskQueueID
  taskQueueID = long( optVal )
  return S_OK()

Script.registerSwitch( "v", "verbose", "give max details about task queues", setVerbose )
Script.registerSwitch( "t:", "taskQueue=", "show this task queue only", setTaskQueueID )

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ' % Script.scriptName ] ) )

Script.parseCommandLine( initializeMonitor = False )
rpcClient = RPCClient( "WorkloadManagement/Matcher" )

result = rpcClient.getActiveTaskQueues()
if not result[ 'OK' ]:
  print 'ERROR: %s' % result['Message']
  sys.exit( 1 )

tqDict = result[ 'Value' ]

if not verbose:
  fields = ['TaskQueue','Jobs','CPUTime','Owner','OwnerGroup','Sites',
            'Platforms','SubmitPools','Setup','Priority']
  records = []

  print
  for tqId in sorted( tqDict ):
    if taskQueueID and tqId != taskQueueID:
      continue
    record = [str(tqId)]
    tqData = tqDict[ tqId ]
    for key in fields[1:]:
      if key == 'Owner':
        value = tqData.get( 'OwnerDN', '-' )
        if value != '-':
          result = getUsernameForDN( value )
          if not result['OK']:
            value = 'Unknown'
          else:
            value = result['Value']
      else:
        value = tqData.get( key, '-' )
      if isinstance(value, list):
        if len( value ) > 1:
          record.append( str( value[0] ) + '...' )
        else:
          record.append( str( value[0] ) )
      else:
        record.append( str( value ) )
    records.append( record )

  printTable( fields, records )
else:
  fields = ['Key','Value']
  for tqId in sorted( tqDict ):
    if taskQueueID and tqId != taskQueueID:
      continue
    print "\n==> TQ %s" % tqId
    records = []
    tqData = tqDict[ tqId ]
    for key in sorted( tqData ):
      value = tqData[ key ]
      if isinstance( value, list ):
        records.append( [key, { "Value": value, 'Just': 'L'} ] )
      else:
        value = str( value )
        records.append( [key, { "Value": value, 'Just': 'L' } ] )

    printTable( fields, records, numbering = False )
