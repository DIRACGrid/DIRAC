#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-admin-show-task-queues
# Author :  Ricardo Graciani
########################################################################
"""
   Show details of currently active Task Queues
"""
__RCSID__ = "$Id$"
import sys
import DIRAC

import sys
import time
import random
import types

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Base.Script import parseCommandLine
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities.PrettyPrint import printTable
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getUsernameForDN
from DIRAC.Core.Base import Script

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

parseCommandLine( initializeMonitor = False )
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
      if type( value ) == types.ListType:
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
      if type( value ) == types.ListType:
        value = ",".join( value )
      else:
        value = str( value )
      records.append( [key, value] )
    printTable( fields, records )    
    
