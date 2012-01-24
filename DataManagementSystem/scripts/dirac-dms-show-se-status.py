#!/usr/bin/env python
########################################################################
# $HeadURL$
########################################################################
__RCSID__   = "$Id$"

from DIRAC.Core.Base import Script 

Script.setUsageMessage("""
Get status of the available Storage Elements

Usage: 
  %s [<options>] 
""" % Script.scriptName)

Script.parseCommandLine()

import DIRAC
from DIRAC                                    import gConfig,gLogger
from DIRAC.ConfigurationSystem.Client.Helpers import getStorageElementStatus
from DIRAC.Core.Utilities.List                import sortList

storageCFGBase = "/Resources/StorageElements"

res = gConfig.getSections( storageCFGBase, True )
if not res[ 'OK' ]:
  gLogger.error( 'Failed to get storage element info' )
  gLogger.error( res[ 'Message' ] )
  DIRAC.exit( -1 )
  
gLogger.info( "%s %s %s" % ( 'Storage Element'.ljust( 25 ), 'Read Status'.rjust( 15 ), 'Write Status'.rjust( 15 ) ) )

for se in sortList( res[ 'Value' ] ):
  res = getStorageElementStatus( se )
  #res = gConfig.getOptionsDict( "%s/%s" % ( storageCFGBase, se ) )
  if not res[ 'OK' ]:
    gLogger.error( "Failed to get options dict for %s" % se )
  else:
    readState, writeState = 'Active', 'Active'
    #if res['Value'].has_key( "ReadAccess" ):
    if res[ 'Value' ].has_key( 'Read' ):
      readState = res[ 'Value' ][ 'Read' ]
    #if res[ 'Value' ].has_key( "WriteAccess" ):  
    if res[ 'Value' ].has_key( 'Write' ):
      writeState = res[ 'Value' ][ 'Write']
    gLogger.notice("%s %s %s" % (se.ljust(25),readState.rjust(15),writeState.rjust(15)))
DIRAC.exit(0)

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF