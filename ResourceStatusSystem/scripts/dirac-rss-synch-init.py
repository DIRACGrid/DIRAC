#!/usr/bin/env python
################################################################################
# $HeadURL $
################################################################################
__RCSID__  = "$Id$"

""" 
  Set the status for the given element.
"""

import DIRAC
from DIRAC           import gConfig, gLogger 
from DIRAC.Core.Base import Script

Script.parseCommandLine()

from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient

rsc = ResourceStatusClient()

ses = gConfig.getSections( '/Resources/StorageElements' )
if not ses[ 'OK' ]:
  gLogger.error( ses[ 'Message' ] )
  DIRAC.exit( 2 )  

statuses = rsc.getValidStatuses()[ 'Value' ]
  
for se in ses[ 'Value' ]:
  gLogger.info( se )
  opts = gConfig.getOptions( '/Resources/StorageElements/%s' % se )[ 'Value' ]
  for opt in opts:
    if not opt.endswith( 'Access' ):
      continue
    
    status = gConfig.getValue( '/Resources/StorageElements/%s/%s' % ( se, opt ) )
    
    if status in [ 'NotAllowed', 'InActive' ]:
      status = 'Banned'  
    
    if not status in statuses:
      gLogger.error( '%s not a valid status for %s - %s' % ( status, se, statusType ) )
      continue
    
    statusType = opt.replace( 'Access', '' )
    
    rsc.modifyElementStatus( 'StorageElement', se, statusType, 
                              status = status, reason = 'Init sync' )

DIRAC.exit( 0 )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF