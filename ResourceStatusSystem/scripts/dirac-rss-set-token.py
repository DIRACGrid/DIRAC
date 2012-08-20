#!/usr/bin/env python
################################################################################
# $HeadURL $
################################################################################
""" 
  Set the token for the given element.
"""
__RCSID__  = "$Id$"

import DIRAC
from DIRAC           import gLogger  
from DIRAC.Core.Base import Script

Script.registerSwitch( "g:", "Granularity=", "      Granularity of the element" )
Script.registerSwitch( "n:", "ElementName=", "      Name of the element" )
Script.registerSwitch( "k:", "Token="      , "      Token of the element ( write 'RS_SVC' to give it back to RSS )" )
Script.registerSwitch( "r:", "Reason="     , "      Reason for the change" )
Script.registerSwitch( "t:", "StatusType=" , "      StatusType of the element" )
Script.registerSwitch( "u:", "Duration="   , "      Duration(hours) of the token" )

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     '\nUsage:',
                                     '  %s [option|cfgfile] <granularity> <element_name> <token> [<reason>] [<status_type>] [<duration>]' % Script.scriptName,
                                     '\nArguments:',
                                     '  granularity (string): granularity of the resource, e.g. "Site"',
                                     '  element_name (string): name of the resource, e.g. "LCG.CERN.ch"',
                                     '  token (string, optional): token to be assigned ( "RS_SVC" gives it back to RSS ), e.g. "ubeda"',
                                     '  reason (string, optional): reason for the change, e.g. "I dont like the site admin"',
                                     '  statusType ( string, optional ): defines the status type, otherwise it applies to all',
                                     '  duration( integer, optional ): duration of the token.\n'] ) )
Script.parseCommandLine()

DEFAULT_DURATION = 24

params = {}

for switch in Script.getUnprocessedSwitches():
  if switch[0].lower() == "g" or switch[0].lower() == "granularity":
    params['g'] = switch[ 1 ]
  elif switch[0].lower() == "n" or switch[0].lower() == "elementname":
    params['n'] = switch[ 1 ]  
  elif switch[0].lower() == "k" or switch[0].lower() == "token":
    params['k'] = switch[ 1 ]
  elif switch[0].lower() == "r" or switch[0].lower() == "reason":
    params['r'] = switch[ 1 ]    
  elif switch[0].lower() == "t" or switch[0].lower() == "statustype":
    params['t'] = switch[ 1 ]
  elif switch[0].lower() == "u" or switch[0].lower() == "duration":
    params['u'] = switch[ 1 ]
    
if not params.has_key( 'g' ):
  gLogger.error( 'Granularity not found')
  Script.showHelp()
  DIRAC.exit( 2 )          

if not params.has_key( 'n' ):
  gLogger.error( 'Name not found')
  Script.showHelp()
  DIRAC.exit( 2 )          

#  gLogger.error( 'Token not found')
#  Script.showHelp()
#  DIRAC.exit( 2 )     

from DIRAC.Core.Security.ProxyInfo                              import getProxyInfo
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient     import ResourceStatusClient
from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient
from DIRAC.ResourceStatusSystem.Utilities                       import RssConfiguration 

res = getProxyInfo()
if not res['OK']:
  gLogger.error( "Failed to get proxy information", res['Message'] )
  DIRAC.exit( 2 )
userName = res['Value']['username']

if not params.has_key( 'k' ):
  params[ 'k' ] = userName

validElements = RssConfiguration.getValidElements()
if not params[ 'g' ] in validElements:
  gLogger.error( '"%s" is not a valid granularity' % params[ 'g' ] )
  DIRAC.exit( 2 )

if params[ 'k' ] != 'RS_SVC':
  
  rmc = ResourceManagementClient()
  u   = rmc.getUserRegistryCache( login = params[ 'k' ] )
  
  if not u[ 'OK' ] or not u[ 'Value' ]:
    gLogger.error( '"%s" is not a known user' % params[ 'k' ] )
    DIRAC.exit( 2 )

if not params.has_key( 't' ):
  params[ 't' ] = None

if not params.has_key( 'r' ):
  params[ 'r' ] = 'Status set by %s' % userName
  
if not params.has_key( 'u' ):
  params[ 'u' ] = DEFAULT_DURATION  
else:
  if not isintance( params[ 'u' ], int ):
    gLogger.error( 'Expecting integer for duration, got "%s"' % params[ 'u' ])
    DIRAC.exit( 2 )  


rsCl = ResourceStatusClient()
element = rsCl.getElementStatus( params[ 'g' ], elementName = params[ 'n' ], 
                                 statusType = params[ 't' ], meta = { 'columns' : [ 'StatusType', 'TokenOwner', 'TokenExpiration' ]} )

if not element['OK']:
  gLogger.error( 'Error trying to get (%s,%s,%s)' % ( params['g'], params['n'], params['t']) )
  DIRAC.exit( 2 )  
  
if not element[ 'Value' ]:
  gLogger.notice( 'Not found any record for this element (%s,%s,%s)' % ( params['g'], params['n'], params['t']) )  
  DIRAC.exit( 0 )

from datetime import datetime, timedelta

for lst in element[ 'Value' ]:
    
  sType = lst[0]
  tOwn  = lst[1]   
  tExp  = lst[2] + timedelta( hours = params[ 'u' ] )
    
  if params[ 't' ] == tOwn:
    gLogger.notice( 'TokenOwner for %s (%s) is already %s. Extending period.' % ( params['n'], sType, tOwn ))
    #continue
    
  res = rsCl.modifyElementStatus( params['g'], params['n'], sType, reason = params['r'], 
                                  tokenOwner = params['k'], tokenExpiration = tExp )
    
  if not res['OK']:
    gLogger.error( res[ 'Message' ] )
    DIRAC.exit( 2 )
  
  _msg = '%s is responsible for %s ( %s ) until %s' % ( userName, params['n'], stype, tExp )
  gLogger.notice( _msg )
  
DIRAC.exit( 0 )  

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF