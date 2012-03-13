#!/usr/bin/env python
################################################################################
# $HeadURL $
################################################################################
""" 
  Set the status for the given element.
"""
__RCSID__  = "$Id$"

import DIRAC
from DIRAC           import gLogger  
from DIRAC.Core.Base import Script

Script.registerSwitch( "g:", "Granularity=", "      Granularity of the element" )
Script.registerSwitch( "n:", "ElementName=", "      Name of the element" )
Script.registerSwitch( "a:", "Status="     , "      Status of the element" )
Script.registerSwitch( "r:", "Reason="     , "      Reason for the change" )
Script.registerSwitch( "t:", "StatusType=" , "      StatusType of the element" )

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     '\nUsage:',
                                     '  %s [option|cfgfile] <granularity> <element_name> <status> [<reason>] [<status_type>]' % Script.scriptName,
                                     '\nArguments:',
                                     '  granularity (string): granularity of the resource, e.g. "Site"',
                                     '  element_name (string): name of the resource, e.g. "LCG.CERN.ch"',
                                     '  status (string): status to be assigned, e.g. "Active"',
                                     '  reason (string, optional): reason for the change, e.g. "I dont like the site admin"',
                                     '  statusType ( string, optional ): defines the status type, otherwise it applies to all\n'] ) )
Script.parseCommandLine()

#args = Script.getPositionalArgs()
#if not args:
#  Script.showHelp()

params = {}

for switch in Script.getUnprocessedSwitches():
  if switch[0].lower() == "g" or switch[0].lower() == "granularity":
    params['g'] = switch[ 1 ]
  elif switch[0].lower() == "n" or switch[0].lower() == "elementname":
    params['n'] = switch[ 1 ]  
  elif switch[0].lower() == "a" or switch[0].lower() == "status":
    params['a'] = switch[ 1 ]
  elif switch[0].lower() == "r" or switch[0].lower() == "reason":
    params['r'] = switch[ 1 ]    
  elif switch[0].lower() == "t" or switch[0].lower() == "statustype":
    params['t'] = switch[ 1 ]
    
if not params.has_key( 'g' ):
  gLogger.error( 'Granularity not found')
  Script.showHelp()
  DIRAC.exit( 2 )          

if not params.has_key( 'n' ):
  gLogger.error( 'Name not found')
  Script.showHelp()
  DIRAC.exit( 2 )          

if not params.has_key( 'a' ):
  gLogger.error( 'Status not found')
  Script.showHelp()
  DIRAC.exit( 2 )     

from DIRAC.Core.Security.ProxyInfo                          import getProxyInfo
from DIRAC.ResourceStatusSystem                             import ValidRes, ValidStatus
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient

res = getProxyInfo()
if not res['OK']:
  gLogger.error( "Failed to get proxy information", res['Message'] )
  DIRAC.exit( 2 )
userName = res['Value']['username']

if not params[ 'g' ] in ValidRes:
  gLogger.error( '"%s" is not a valid granularity' % params[ 'g' ] )
  DIRAC.exit( 2 )

if not params[ 'a' ] in ValidStatus:
  gLogger.error( '"%s" is not a valid status' % params[ 'a' ] )
  DIRAC.exit( 2 )

if not params.has_key( 't' ):
  params[ 't' ] = None

if not params.has_key( 'r' ):
  params[ 'r' ] = 'Status forced by %s' % userName
  
rsCl = ResourceStatusClient()
element = rsCl.getElementStatus( params[ 'g' ], elementName = params[ 'n' ], 
                                 statusType = params[ 't' ], meta = { 'columns' : [ 'Status', 'StatusType' ]} )

if not element['OK']:
  gLogger.error( 'Error trying to get (%s,%s,%s)' % ( params['g'], params['n'], params['t']) )
  DIRAC.exit( 2 )  
  
if not element[ 'Value' ]:
  gLogger.notice( 'Not found any record for this element (%s,%s,%s)' % ( params['g'], params['n'], params['t']) )  
  DIRAC.exit( 0 )

from datetime import datetime, timedelta

_tomorrow = datetime.utcnow().replace( microsecond = 0 ) + timedelta( days = 1 )

for lst in element[ 'Value' ]:
    
  status = lst[0]
  stype  = lst[1]   
    
  if params[ 'a' ] == status:
    gLogger.notice( 'Status for %s (%s) is already %s. Ignoring..' % ( params['n'], stype, status ))
    continue
    
  res = rsCl.modifyElementStatus( params['g'], params['n'], stype, status = params['a'],
                                  reason = params['r'], tokenOwner = userName )
    
  if not res['OK']:
    gLogger.error( res[ 'Message' ] )
    DIRAC.exit( 2 )
  
  _msg = '%s is responsible for %s ( %s ) until %s' % ( userName, params['n'], stype, _tomorrow )
  gLogger.notice( _msg )
  
DIRAC.exit( 0 )  
  
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF