#! /usr/bin/env python
"""
  Get Pilots Logging for specific Pilot UUID or Job ID.
"""
__RCSID__ = "$Id$"

import sys
from DIRAC import S_OK
from DIRAC.Core.Base import Script
from DIRAC.WorkloadManagementSystem.Client.PilotsLoggingClient import PilotsLoggingClient
from DIRAC.Core.Utilities.PrettyPrint import printTable

def getByUUID( optVal ):
  pilotsLogging = PilotsLoggingClient()
  result = pilotsLogging.getPilotsLogging( optVal )
  if not result['OK']:
    print 'ERROR: %s' % result['Message']
    sys.exit( 1 )
  labels = ['pilotUUID', 'timestamp', 'source', 'phase', 'status', 'messageContent']
  content = []
  for log in result['Value']:
    content.append( [ log['pilotUUID'], log['timestamp'], log['source'], log['phase'], log['status'], log['messageContent'] ] )
  printTable( labels, content, numbering = False, columnSeparator=' | ' )
  return S_OK()

Script.registerSwitch( 'u:', 'uuid=', 'get PilotsLogging for given Pilot UUID', getByUUID )
#Script.registerSwitch( 'j:', 'jobid=', 'get PilotsLogging for given Job ID', getByJobID )

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s option value ' % Script.scriptName ] ) )

Script.parseCommandLine()
