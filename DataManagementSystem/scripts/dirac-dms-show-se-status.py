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

for k, v in res[ 'Value' ].items():

  readState, writeState = 'Active', 'Active'

  if v.has_key( 'ReadAccess' ):
    readState = v[ 'ReadAccess' ]

  if v.has_key( 'WriteAccess' ):
    writeState = v[ 'WriteAccess']
  gLogger.notice( "%s %s %s" % ( k.ljust( 25 ), readState.rjust( 15 ), writeState.rjust( 15 ) ) )

DIRAC.exit( 0 )

################################################################################
# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
