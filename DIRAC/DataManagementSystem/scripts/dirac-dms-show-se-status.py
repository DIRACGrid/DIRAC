#!/usr/bin/env python

from DIRAC.Core.Base import Script

Script.setUsageMessage( """
Get status of the available Storage Elements

Usage:
  %s [<options>]
""" % Script.scriptName )

Script.parseCommandLine()

import DIRAC
from DIRAC                                            import gConfig, gLogger
from DIRAC.ResourceStatusSystem.Client.ResourceStatus import ResourceStatus
from DIRAC.Core.Utilities.PrettyPrint                 import printTable
from DIRAC.Core.Security.ProxyInfo                    import getVOfromProxyGroup

storageCFGBase = "/Resources/StorageElements"

res = gConfig.getSections( storageCFGBase, True )
if not res[ 'OK' ]:
  gLogger.error( 'Failed to get storage element info' )
  gLogger.error( res[ 'Message' ] )
  DIRAC.exit( -1 )

gLogger.info( "%s %s %s" % ( 'Storage Element'.ljust( 25 ), 'Read Status'.rjust( 15 ), 'Write Status'.rjust( 15 ) ) )

seList = sorted( res[ 'Value' ] )

resourceStatus = ResourceStatus()

res = resourceStatus.getStorageElementStatus( seList )
if not res[ 'OK' ]:
  gLogger.error( "Failed to get StorageElement status for %s" % str( seList ) )
  DIRAC.exit( 1 )
  
fields = ['SE','ReadAccess','WriteAccess','RemoveAccess','CheckAccess']  
records = []

result = getVOfromProxyGroup()
if not result['OK']:
  gLogger.error( 'Failed to determine the user VO' )
  DIRAC.exit( -1 )
vo = result['Value']

for se, statusDict in res[ 'Value' ].items():

  # Check if the SE is allowed for the user VO
  voList = gConfig.getValue( '/Resources/StorageElements/%s/VO' % se, [] )
  if voList and not vo in voList:
    continue 
  
  record = [se]
  for status in fields[1:]:
    value = statusDict.get( status, 'Unknown' )
    record.append( value )
  records.append( record )    
    
printTable( fields, records, numbering=False, sortField = 'SE' ) 

DIRAC.exit( 0 )

################################################################################
# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
