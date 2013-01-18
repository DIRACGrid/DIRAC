#!/usr/bin/env python
########################################################################
# $HeadURL$
########################################################################
""" Enable usage of the File Catalog mirrors at given sites
"""
__RCSID__ = "$Id$"
import DIRAC
from DIRAC.Core.Base                                   import Script

Script.setUsageMessage( """
Enable usage of the File Catalog mirrors at given sites

Usage:
   %s site1 [site2 ...]
""" % Script.scriptName )

Script.parseCommandLine( ignoreErrors = True )

sites = Script.getPositionalArgs()

from DIRAC.ConfigurationSystem.Client.CSAPI              import CSAPI
from DIRAC.FrameworkSystem.Client.NotificationClient     import NotificationClient
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.ConfigurationSystem.Client.Helpers.Registry   import getUserOption
from DIRAC.Core.Security.ProxyInfo                       import getProxyInfo
from DIRAC                                               import gConfig, gLogger
csAPI = CSAPI()

res = getProxyInfo()
if not res['OK']:
  gLogger.error( "Failed to get proxy information", res['Message'] )
  DIRAC.exit( 2 )
userName = res['Value']['username']
group = res['Value']['group']

if not sites:
  Script.showHelp()
  DIRAC.exit( -1 )

catalogCFGBase = "/Resources/FileCatalogs/LcgFileCatalogCombined"
allowed = []
for site in sites:
  res = gConfig.getOptionsDict( '%s/%s' % ( catalogCFGBase, site ) )
  if not res['OK']:
    gLogger.error( "The provided site (%s) does not have an associated catalog." % site )
    continue

  res = csAPI.setOption( "%s/%s/Status" % ( catalogCFGBase, site ), "Active" )
  if not res['OK']:
    gLogger.error( "Failed to update %s catalog status to Active" % site )
  else:
    gLogger.debug( "Successfully updated %s catalog status to Active" % site )
    allowed.append( site )

if not allowed:
  gLogger.error( "Failed to allow any catalog mirrors" )
  DIRAC.exit( -1 )

res = csAPI.commitChanges()
if not res['OK']:
  gLogger.error( "Failed to commit changes to CS", res['Message'] )
  DIRAC.exit( -1 )

subject = '%d catalog instance(s) allowed for use' % len( allowed )
addressPath = 'EMail/Production'
address = Operations().getValue( addressPath, '' )

body = 'The catalog mirrors at the following sites were allowed'
for site in allowed:
  body = "%s\n%s" % ( body, site )

if not address:
  gLogger.notice( "'%s' not defined in Operations, can not send Mail\n" % addressPath, body )
  DIRAC.exit( 0 )

NotificationClient().sendMail( address, subject, body, getUserOption( userName, 'Email', '' ) )
DIRAC.exit( 0 )

