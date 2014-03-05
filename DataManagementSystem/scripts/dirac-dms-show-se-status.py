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
from DIRAC.Core.Utilities.List                        import sortList
from DIRAC.Core.Utilities.PrettyPrint                 import printTable

storageCFGBase = "/Resources/StorageElements"

res = gConfig.getSections( storageCFGBase, True )
if not res[ 'OK' ]:
  gLogger.error( 'Failed to get storage element info' )
  gLogger.error( res[ 'Message' ] )
  DIRAC.exit( -1 )

gLogger.info( "%s %s %s" % ( 'Storage Element'.ljust( 25 ), 'Read Status'.rjust( 15 ), 'Write Status'.rjust( 15 ) ) )

seList = sortList( res[ 'Value' ] )

resourceStatus = ResourceStatus()

res = resourceStatus.getStorageElementStatus( seList )
if not res[ 'OK' ]:
  gLogger.error( "Failed to get StorageElement status for %s" % str( seList ) )
  DIRAC.exit( 1 )
  
fields = ['SE','ReadAccess','WriteAccess','RemoveAccess','CheckAccess']  
records = []

for se, statusDict in res[ 'Value' ].items():
  record = [se]
  for status in fields[1:]:
    value = statusDict.get( status, 'Unknown' )
    record.append( value )
  records.append( record )    
    
printTable( fields, records, numbering=False ) 

DIRAC.exit( 0 )

################################################################################
# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
