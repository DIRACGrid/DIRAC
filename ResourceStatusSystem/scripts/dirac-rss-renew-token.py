#!/usr/bin/env python
########################################################################
# $HeadURL$
# File:     dirac-rss-renew-token
# Author:   Federico Stagni
########################################################################
"""
  Extend the duration of given token
"""
__RCSID__ = "$Id$"
import DIRAC
from DIRAC.Core.Base import Script

hours = 24

Script.registerSwitch( "e:", "Extension=", "      Number of hours of token renewal (will be 24 if unspecified)" )
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     '\nUsage:',
                                     '  %s [option|cfgfile] <resource_name> <token_name> [<hours>]' % Script.scriptName,
                                     '\nArguments:',
                                     '  resource_name (string): name of the resource, e.g. "lcg.cern.ch"',
                                     '  token_name (string): name of a token, e.g. "RS_SVC"',
                                     '  hours (int, optional): number of hours (default: 24)\n'] ) )
Script.parseCommandLine()

args = Script.getPositionalArgs()
if not args:
  Script.showHelp()

for switch in Script.getUnprocessedSwitches():
  if switch[0].lower() == "e" or switch[0].lower() == "extension":
    hours = int( switch[1] )

from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient
from DIRAC.Core.Security.ProxyInfo                   import getProxyInfo
from DIRAC                                           import gLogger
from DIRAC.Core.DISET.RPCClient                      import RPCClient
from DIRAC.ResourceStatusSystem.Utilities.CS         import getMailForUser

nc = NotificationClient()

s = RPCClient( "ResourceStatus/ResourceStatus" )

# Check credentials
res = getProxyInfo()
if not res['OK']:
  gLogger.error( "Failed to get proxy information", res['Message'] )
  DIRAC.exit( 2 )
userName = res['Value']['username']
group = res['Value']['group']
if group not in ( 'diracAdmin', 'lhcb_prod' ):
  gLogger.error( "You must be lhcb_prod or diracAdmin to execute this script" )
  gLogger.info( "Please issue 'lhcb-proxy-init -g lhcb_prod' or 'lhcb-proxy-init -g diracAdmin'" )
  DIRAC.exit( 2 )

for arg in args:
  g = s.whatIs( arg )
  res = s.extendToken( g['Value'], arg, hours )

  if not res['OK']:
    gLogger.error( "Problem with extending: %s" % res['Message'] )
    DIRAC.exit( 2 )
  mailMessage = "The token for %s %s has been successfully renewed for others %i hours" % ( g, arg, hours )
  nc.sendMail( getMailForUser( userName )['Value'][0], 'Token for %s renewed' % arg, mailMessage )

DIRAC.exit( 0 )
