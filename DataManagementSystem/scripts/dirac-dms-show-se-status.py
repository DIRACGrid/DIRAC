#!/usr/bin/env python

from DIRAC.Core.Base import Script

Script.setUsageMessage( """
Get status of the available Storage Elements

Usage:
  %s [<options>]
""" % Script.scriptName )

Script.parseCommandLine()

import DIRAC
from DIRAC                                              import gConfig,gLogger
from DIRAC.ResourceStatusSystem.Client.ResourceStatus   import ResourceStatus
from DIRAC.Core.Utilities.List                          import sortList
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import Resources
from DIRAC.Core.Security.ProxyInfo                      import getVOfromProxyGroup

if __name__ == "__main__":
  
  result = getVOfromProxyGroup()
  if not result['OK']:
    gLogger.notice( 'Error:', result['Message'] )
    DIRAC.exit( 1 )
  vo = result['Value']  
  resources = Resources( vo = vo )
  result = resources.getEligibleStorageElements()
  if not result['OK']:
    gLogger.notice( 'Error:', result['Message'] )
    DIRAC.exit( 2 )
  seList = sortList( result[ 'Value' ] )

  resourceStatus = ResourceStatus()
 
  result = resourceStatus.getStorageStatus( seList )
  if not result['OK']:
    gLogger.notice( 'Error:', result['Message'] )
    DIRAC.exit( 3 )

  for k,v in result[ 'Value' ].items():
    
    readState, writeState = 'Active', 'Active'
    
    if v.has_key( 'ReadAccess' ):
      readState = v[ 'ReadAccess' ]  
    
    if v.has_key( 'WriteAccess' ):
      writeState = v[ 'WriteAccess']
    gLogger.notice("%s %s %s" % ( k.ljust(25),readState.rjust(15),writeState.rjust(15)) )

  DIRAC.exit(0)

################################################################################
# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
