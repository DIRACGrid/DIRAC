#!/usr/bin/env python
################################################################################
# $HeadURL $
################################################################################
__RCSID__  = "$Id$"

""" 
  Set the status for the given element.
"""

import DIRAC
from DIRAC.Core.Base import Script

Script.registerSwitch( "g:", "Granularity=", "      Granularity of the element" )
Script.registerSwitch( "n:", "ElementName=", "      Name of the element" )
Script.registerSwitch( "s:", "Status="     , "      Status of the element" )
Script.registerSwitch( "t:", "StatusType="     , "      StatusType of the element" )

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     '\nUsage:',
                                     '  %s [option|cfgfile] <granularity> <element_name> <status> [<status_type>]' % Script.scriptName,
                                     '\nArguments:',
                                     '  granularity (string): granularity of the resource, e.g. "Site"',
                                     '  element_name (string): name of the resource, e.g. "LCG.CERN.ch"',
                                     '  status (string): status to be assigned, e.g. "Active"',
                                     '  statusType ( string, optional ): defines the status type, otherwise it applies to all\n'] ) )
Script.parseCommandLine()

args = Script.getPositionalArgs()
if not args:
  Script.showHelp()

params = {}

for switch in Script.getUnprocessedSwitches():
  if switch[0].lower() == "g" or switch[0].lower() == "granularity":
    params['g'] = switch[ 1 ]
  elif switch[0].lower() == "n" or switch[0].lower() == "elementname":
    params['n'] = switch[ 1 ]  
  elif switch[0].lower() == "s" or switch[0].lower() == "status":
    params['s'] = switch[ 1 ]
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

if not params.has_key( 's' ):
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

if not params[ 'g' ] in ValidRes.keys():
  gLogger.error( '"%s" is not a valid granularity' % params[ 'g' ] )
  DIRAC.exit( 2 )

if not params[ 's' ] in ValidStatus:
  gLogger.error( '"%s" is not a valid status' % params[ 's' ] )
  DIRAC.exit( 2 )

if not params.has_key( 't' ):
  params[ 't' ] = None
  
rsCl = ResourceStatusClient()
element = rsCl.getElementStatus( params[ 'g' ], elementName = params[ 'n' ], 
                                 statusType = params[ 't' ], meta = { 'columns' : [ 'Status', 'StatusType' ]} )

if not element['OK']:
  gLogger.error( 'Error trying to get (%s,%s,%s)' % ( params['g'], params['n'], params['t']) )
  DIRAC.exit( 2 )
  
if element[ 'Value' ]:
  
  reason = 'Status forced by script'
  
  for lst in element[ 'Value' ]:
    
    status = lst[0]
    stype  = lst[1]
    
    if params[ 's' ] == status:
      gLogger.info( 'Status for %s (%s) is already %s. Ignoring..' % ( params['n'], stype, status ))
      continue
    rsCl.modifyElementStatus( params['g'], params['n'], stype, status = status,
                              reason = reason, tokenOwner = userName )
  
else:
  gLogger.error( 'Not found any record for this element (%s,%s,%s)' % ( params['g'], params['n'], params['t']) )  
  
  
  