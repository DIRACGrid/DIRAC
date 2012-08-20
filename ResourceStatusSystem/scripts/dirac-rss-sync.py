#!/usr/bin/env python
################################################################################
# $HeadURL $
################################################################################
""" 
  Set the token for the given element.
"""
__RCSID__  = "$Id$"

import DIRAC
from DIRAC           import gLogger, gConfig  
from DIRAC.Core.Base import Script

Script.registerSwitch( "i" , "Init",         "      Initialize statuses" )
Script.registerSwitch( "g:", "Granularity=", "      Granularity of the element" )

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     '\nUsage:',
                                     '  %s [option|cfgfile] <granularity>' % Script.scriptName,
                                     '\nArguments:',
                                     '  granularity (string): granularity of the resource, e.g. "Site"\n'] ) )
Script.parseCommandLine()

DEFAULT_DURATION = 24

granularity = None
init        = None

for switch in Script.getUnprocessedSwitches():
  if switch[0].lower() == "g" or switch[0].lower() == "granularity":
    granularity = switch[ 1 ]
  elif switch[0].lower() == "i" or switch[0].lower() == "init":
    init = True

from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
from DIRAC.ResourceStatusSystem.Utilities.Synchronizer      import Synchronizer

rsc = ResourceStatusClient()
s   = Synchronizer()

def syncSites(): 
  gLogger.notice( 'Synchronizing SITES --------------------->' )
  s._syncSites()  
def syncServices():
  gLogger.notice( 'Synchronizing SERVICES ------------------>' )
  s._syncServices()
def syncResources():
  gLogger.notice( 'Synchronizing RESOURCES ----------------->' )
  s._syncResources()
def syncStorageElements():  
  gLogger.notice( 'Synchronizing STORAGE ELEMENTS ---------->' )
  s._syncStorageElements()
  if init is not None:
    gLogger.notice( 'Initializing STORAGE ELEMENTS ------------>' )
    syncStorageElementsInit()
def syncRegistryUsers():
  gLogger.notice( 'Synchronizing REGISTRY USERS ------------>' )
  s._syncRegistryUsers()

def syncStorageElementsInit():
  ses = gConfig.getSections( '/Resources/StorageElements' )
  if not ses[ 'OK' ]:
    gLogger.error( ses[ 'Message' ] )
    DIRAC.exit( 2 )  

  statuses = rsc.getValidStatuses()[ 'Value' ]
  
  for se in ses[ 'Value' ]:
    opts = gConfig.getOptions( '/Resources/StorageElements/%s' % se )[ 'Value' ]

    statusTypes = [ 'Read', 'Write', 'Check', 'Remove' ]
  
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
      statusTypes.remove( statusType )   
    
      rsc.modifyElementStatus( 'StorageElement', se, statusType, 
                                status = status, reason = 'Init sync' )

    for sType in statusTypes:
    
      # If there is nothing on the CS, we set the statusType to DEFAULT_STATUS
      DEFAULT_STATUS = 'Active'
    
      rsc.modifyElementStatus( 'StorageElement', se, sType, 
                                status = DEFAULT_STATUS, reason = 'Default status' )

if granularity is None:
  
  gLogger.notice( 'Please, be patient. This will take some time.' )
  syncSites()
  syncServices()
  syncResources()
  syncStorageElements()
  syncRegistryUsers()

elif granularity == 'Site':
  
  syncSites()
  
elif granularity == 'Service':
  
  syncServices()

elif granularity == 'Resource':
  
  syncResources()

elif granularity == 'StorageElement':
  
  syncStorageElements()
  
else:
  
  gLogger.error( '%s is not a valid granularity' % granularity )
  DIRAC.exit( 2 )
  
DIRAC.exit( 0 )  

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF