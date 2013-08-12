#!/usr/bin/env python
########################################################################
# $HeadURL$
########################################################################
""" Ban one or more Storage Elements for usage
"""
__RCSID__ = "$Id$"
import DIRAC
from DIRAC.Core.Base                                   import Script

read = True
write = True
check = True
site = ''
mute = False

Script.setUsageMessage( """
Ban one or more Storage Elements for usage

Usage:
   %s SE1 [SE2 ...]
""" % Script.scriptName )

Script.registerSwitch( "r" , "BanRead" , "     Ban only reading from the storage element" )
Script.registerSwitch( "w" , "BanWrite", "     Ban writing to the storage element" )
Script.registerSwitch( "k" , "BanCheck", "     Ban check access to the storage element" )
Script.registerSwitch( "m" , "Mute"    , "     Do not send email" )
Script.registerSwitch( "S:", "Site="   , "     Ban all SEs associate to site (note that if writing is allowed, check is always allowed)" )
Script.parseCommandLine( ignoreErrors = True )

ses = Script.getPositionalArgs()
for switch in Script.getUnprocessedSwitches():
  if switch[0].lower() == "r" or switch[0].lower() == "banread":
    write = False
    check = False
  if switch[0].lower() == "w" or switch[0].lower() == "banwrite":
    read = False
    check = False
  if switch[0].lower() == "k" or switch[0].lower() == "bancheck":
    read = False
    write = False
  if switch[0].lower() == "m" or switch[0].lower() == "mute":
    mute = True
  if switch[0] == "S" or switch[0].lower() == "site":
    site = switch[1]

#from DIRAC.ConfigurationSystem.Client.CSAPI           import CSAPI
from DIRAC.Interfaces.API.DiracAdmin                     import DiracAdmin
from DIRAC                                               import gConfig, gLogger
from DIRAC.ResourceStatusSystem.Client.ResourceStatus    import ResourceStatus
from DIRAC.ConfigurationSystem.Client.Helpers.Resources  import Resources
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Security.ProxyInfo                       import getProxyInfo
#csAPI = CSAPI()

diracAdmin = DiracAdmin()
exitCode = 0
errorList = []
setup = gConfig.getValue( '/DIRAC/Setup', '' )
if not setup:
  print 'ERROR: Could not contact Configuration Service'
  exitCode = 2
  DIRAC.exit( exitCode )

res = getProxyInfo()
if not res[ 'OK' ]:
  gLogger.error( 'Failed to get proxy information', res[ 'Message' ] )
  DIRAC.exit( 2 )

userName = res['Value'].get( 'username' )
if not userName:
  gLogger.error( 'Failed to get username for proxy' )
  DIRAC.exit( 2 )

if site:
  res = Resources().getStorageElements( site )
  if not res[ 'OK' ]:
    gLogger.error( 'The provided site (%s) is not known.' % site )
    DIRAC.exit( -1 )
  ses.extend( res[ 'Value' ][ 'SE' ].replace( ' ', '' ).split( ',' ) )

if not ses:
  gLogger.error( 'There were no SEs provided' )
  DIRAC.exit( -1 )

readBanned = []
writeBanned = []
checkBanned = []

resourceStatus = ResourceStatus()

res = resourceStatus.getStorageStatus( ses )
if not res['OK']:
  gLogger.error( "Storage Element %s does not exist" % ses )
  DIRAC.exit( -1 )

reason = 'Forced with dirac-admin-ban-se by %s' % userName

for se, seOptions in res[ 'Value' ].items():

  resW = resC = resR = { 'OK' : False }

  # Eventually, we will get rid of the notion of InActive, as we always write Banned.
  if read and seOptions.has_key( 'ReadAccess' ):

    if not seOptions[ 'ReadAccess' ] in [ 'Active', 'Degraded', 'Probing' ]:
      gLogger.notice( 'Read option for %s is %s, instead of %s' % ( se, seOptions[ 'ReadAccess' ], [ 'Active', 'Degraded', 'Probing' ] ) )
      gLogger.notice( 'Try specifying the command switches' )
    else:

      resR = resourceStatus.setStorageElementStatus( se, 'ReadAccess', 'Banned', reason, userName )
      #res = csAPI.setOption( "%s/%s/ReadAccess" % ( storageCFGBase, se ), "InActive" )
      if not resR['OK']:
        gLogger.error( 'Failed to update %s read access to Banned' % se )
      else:
        gLogger.notice( 'Successfully updated %s read access to Banned' % se )
        readBanned.append( se )

  # Eventually, we will get rid of the notion of InActive, as we always write Banned.
  if write and seOptions.has_key( 'WriteAccess' ):

    if not seOptions[ 'WriteAccess' ] in [ 'Active', 'Degraded', 'Probing' ]:
      gLogger.notice( 'Write option for %s is %s, instead of %s' % ( se, seOptions[ 'WriteAccess' ], [ 'Active', 'Degraded', 'Probing' ] ) )
      gLogger.notice( 'Try specifying the command switches' )
    else:

      resW = resourceStatus.setStorageElementStatus( se, 'WriteAccess', 'Banned', reason, userName )
      #res = csAPI.setOption( "%s/%s/WriteAccess" % ( storageCFGBase, se ), "InActive" )
      if not resW['OK']:
        gLogger.error( "Failed to update %s write access to Banned" % se )
      else:
        gLogger.notice( "Successfully updated %s write access to Banned" % se )
        writeBanned.append( se )

  # Eventually, we will get rid of the notion of InActive, as we always write Banned.
  if check and seOptions.has_key( 'CheckAccess' ):

    if not seOptions[ 'CheckAccess' ] in [ 'Active', 'Degraded', 'Probing' ]:
      gLogger.notice( 'Check option for %s is %s, instead of %s' % ( se, seOptions[ 'CheckAccess' ], [ 'Active', 'Degraded', 'Probing' ] ) )
      gLogger.notice( 'Try specifying the command switches' )
    else:

      resC = resourceStatus.setStorageElementStatus( se, 'CheckAccess', 'Banned', reason, userName )
      #res = csAPI.setOption( "%s/%s/CheckAccess" % ( storageCFGBase, se ), "InActive" )
      if not resC['OK']:
        gLogger.error( "Failed to update %s check access to Banned" % se )
      else:
        gLogger.notice( "Successfully updated %s check access to Banned" % se )
        checkBanned.append( se )

  if not( resR['OK'] or resW['OK'] or resC['OK'] ):
    DIRAC.exit( -1 )

if not ( writeBanned or readBanned or checkBanned ):
  gLogger.notice( "No storage elements were banned" )
  DIRAC.exit( -1 )

if mute:
  gLogger.notice( 'Email is muted by script switch' )
  DIRAC.exit( 0 )

subject = '%s storage elements banned for use' % len( writeBanned + readBanned + checkBanned )
addressPath = 'EMail/Production'
address = Operations().getValue( addressPath, '' )

body = ''
if read:
  body = "%s\n\nThe following storage elements were banned for reading:" % body
  for se in readBanned:
    body = "%s\n%s" % ( body, se )
if write:
  body = "%s\n\nThe following storage elements were banned for writing:" % body
  for se in writeBanned:
    body = "%s\n%s" % ( body, se )
if check:
  body = "%s\n\nThe following storage elements were banned for check access:" % body
  for se in checkBanned:
    body = "%s\n%s" % ( body, se )

if not address:
  gLogger.notice( "'%s' not defined in Operations, can not send Mail\n" % addressPath, body )
  DIRAC.exit( 0 )

res = diracAdmin.sendMail( address, subject, body )
gLogger.notice( 'Notifying %s' % address )
if res[ 'OK' ]:
  gLogger.notice( res[ 'Value' ] )
else:
  gLogger.notice( res[ 'Message' ] )
DIRAC.exit( 0 )
