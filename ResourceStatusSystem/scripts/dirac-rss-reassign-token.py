#!/usr/bin/env python
########################################################################
# $HeadURL$
# File:     dirac-rss-reassign-token
# Author:   Federico Stagni
########################################################################
"""
  Re-assign a token: if it was assigned to a human, assign it to 'RS_SVC' and viceversa.
"""
__RCSID__ = "$Id$"
import DIRAC
from DIRAC.Core.Base import Script
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     '\nUsage:',
                                     '  %s [option|cfgfile] <resource_name> <token_name> <username>' % Script.scriptName,
                                     '\nArguments:',
                                     '  resource_name (string): name of the resource, e.g. "lcg.cern.ch"',
                                     '  token_name (string): name of a token, e.g. "RS_SVC"',
                                     '  username (string): username to reassign the token to\n',] ) )
Script.parseCommandLine()

hours = 24

args = Script.getPositionalArgs()
if not args:
  Script.showHelp()

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
  res = s.reAssignToken( g['Value'], arg, userName )
  if not res['OK']:
    gLogger.error( "Problem with re-assigning token for %s: " % res['Message'] )
    DIRAC.exit( 2 )
  mailMessage = "The token for %s %s has been successfully re-assigned." % ( g, arg )
  nc.sendMail( getMailForUser( userName )['Value'][0], 'Token for %s reassigned' % arg, mailMessage )

DIRAC.exit( 0 )
