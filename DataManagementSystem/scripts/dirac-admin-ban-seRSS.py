#!/usr/bin/env python
########################################################################
# $HeadURL$
########################################################################
__RCSID__ = "$Id$"
import DIRAC
from DIRAC.Core.Base                                   import Script

read = True
write = True
site = ''

Script.setUsageMessage( """
Ban one or more Storage Elements for usage

Usage:
   %s SE1 [SE2 ...]
""" % Script.scriptName )

Script.registerSwitch( "r", "BanRead", "      Ban only reading from the storage element" )
Script.registerSwitch( "w", "BanWrite", "     Ban writing to the storage element" )
Script.registerSwitch( "S:", "Site=", "      Ban all SEs associate to site" )
Script.parseCommandLine( ignoreErrors = True )

ses = Script.getPositionalArgs()
for switch in Script.getUnprocessedSwitches():
  if switch[0].lower() == "r" or switch[0].lower() == "banread":
    write = False
  if switch[0].lower() == "w" or switch[0].lower() == "banwrite":
    read = False
  if switch[0] == "S" or switch[0].lower() == "site":
    site = switch[1]

#from DIRAC.ConfigurationSystem.Client.CSAPI           import CSAPI
from DIRAC.FrameworkSystem.Client.NotificationClient  import NotificationClient
from DIRAC.Core.Security.Misc                         import getProxyInfo
from DIRAC                                            import gConfig, gLogger
from DIRAC.Core.Utilities.List                        import intListToString
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
#csAPI = CSAPI()
rssClient = ResourceStausClient()

res = getProxyInfo()
if not res['OK']:
  gLogger.error( "Failed to get proxy information", res['Message'] )
  DIRAC.exit( 2 )
userName = res['Value']['username']
group = res['Value']['group']

if not type( ses ) == type( [] ):
  Script.showHelp()
  DIRAC.exit( -1 )

if site:
  res = gConfig.getOptionsDict( '/Resources/Sites/LCG/%s' % site )
  if not res['OK']:
    gLogger.error( "The provided site (%s) is not known." % site )
    DIRAC.exit( -1 )
  ses.extend( res['Value']['SE'].replace( ' ', '' ).split( ',' ) )
if not ses:
  gLogger.error( "There were no SEs provided" )
  DIRAC.exit()

readBanned = []
writeBanned = []
#storageCFGBase = "/Resources/StorageElements"
for se in ses:

  resR = rssClient.getStorageElement( se, 'Read' )
  if resR[ 'OK' ]:
    resR = resR[ 'Value' ]    
    if not resR:
      gLogger.error( "Storage Element (R) %s does not exist" % se )      
      continue    
  
  resW = rssClient.getStorageElement( se, 'Write' )
  if resW[ 'OK' ]:
    resW = resW[ 'Value' ]    
    if not resW:
      gLogger.error( "Storage Element (W) %s does not exist" % se )      
      continue    

  if read and ( resR[1] == 'Active' or resR[1] == 'Bad' ):
    res = rssClient.setStorageElementStatus( se, 'Banned', 'dirac-admin-allow-se', userName, 'Read')     
    if not res['OK']:
      gLogger.error( "Failed to update %s read access to Banned" % se )
    else:
      gLogger.debug( "Successfully updated %s read access to Banned" % se )
      readAllowed.append( se )

  if write and ( resW[1] == 'Active' or resW[1] == 'Bad' ):
    res = rssClient.setStorageElementStatus( se, 'Banned', 'dirac-admin-allow-se', userName, 'Write')     
    if not res['OK']:
      gLogger.error( "Failed to update %s write access to Banned" % se )
    else:
      gLogger.debug( "Successfully updated %s write access to Banned" % se )
      writeAllowed.append( se )
    
#  res = gConfig.getOptionsDict( "%s/%s" % ( storageCFGBase, se ) )
#  if not res['OK']:
#    gLogger.error( "Storage Element %s does not exist" % se )
#    continue
#  existingOptions = res['Value']
#  if read and existingOptions['ReadAccess'] == "Active":
#    res = csAPI.setOption( "%s/%s/ReadAccess" % ( storageCFGBase, se ), "InActive" )
#    if not res['OK']:
#      gLogger.error( "Failed to update %s read access to InActive" % se )
#    else:
#      gLogger.debug( "Successfully updated %s read access to InActive" % se )
#      readBanned.append( se )
#  if write and existingOptions['WriteAccess'] == "Active":
#    res = csAPI.setOption( "%s/%s/WriteAccess" % ( storageCFGBase, se ), "InActive" )
#    if not res['OK']:
#      gLogger.error( "Failed to update %s write access to InActive" % se )
#    else:
#      gLogger.debug( "Successfully updated %s write access to InActive" % se )
#      writeBanned.append( se )
#res = csAPI.commitChanges()
#if not res['OK']:
#  gLogger.error( "Failed to commit changes to CS", res['Message'] )
#  DIRAC.exit( -1 )

if not ( writeBanned or readBanned ):
  gLogger.notice( "No storage elements were banned" )
  DIRAC.exit( -1 )

subject = '%s storage elements banned for use' % len( ses )
address = gConfig.getValue( '/Operations/EMail/Production', 'lhcb-grid@cern.ch' )
body = ''
if read:
  body = "%s\n\nThe following storage elements were banned for reading:" % body
  for se in readBanned:
    body = "%s\n%s" % ( body, se )
if write:
  body = "%s\n\nThe following storage elements were banned for writing:" % body
  for se in writeBanned:
    body = "%s\n%s" % ( body, se )

NotificationClient().sendMail( address, subject, body, '%s@cern.ch' % userName )
DIRAC.exit( 0 )
