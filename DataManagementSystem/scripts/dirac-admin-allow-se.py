#!/usr/bin/env python
########################################################################
# $HeadURL$
########################################################################
""" Enable using one or more Storage Elements
"""
__RCSID__ = "$Id$"
import DIRAC
from DIRAC.Core.Base import Script

read = True
write = True
check = True
site = ''
mute = False

Script.setUsageMessage( """
Enable using one or more Storage Elements

Usage:
   %s SE1 [SE2 ...]
""" % Script.scriptName )

Script.registerSwitch( "r" , "AllowRead" , "     Allow only reading from the storage element" )
Script.registerSwitch( "w" , "AllowWrite", "     Allow only writing to the storage element" )
Script.registerSwitch( "k" , "AllowCheck", "     Allow only check access to the storage element" )
Script.registerSwitch( "m" , "Mute"      , "     Do not send email" )
Script.registerSwitch( "S:", "Site="     , "     Allow all SEs associated to site" )

Script.parseCommandLine( ignoreErrors = True )

ses = Script.getPositionalArgs()
for switch in Script.getUnprocessedSwitches():
  if switch[0].lower() == "r" or switch[0].lower() == "allowread":
    write = False
    check = False
  if switch[0].lower() == "w" or switch[0].lower() == "allowwrite":
    read = False
    check = False
  if switch[0].lower() == "k" or switch[0].lower() == "allowcheck":
    read = False
    write = False
  if switch[0].lower() == "m" or switch[0].lower() == "mute":
    mute = True
  if switch[0] == "S" or switch[0].lower() == "site":
    site = switch[1]

#from DIRAC.ConfigurationSystem.Client.CSAPI           import CSAPI
from DIRAC.Interfaces.API.DiracAdmin                     import DiracAdmin
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC                                               import gConfig, gLogger
from DIRAC.ResourceStatusSystem.Client.ResourceStatus    import ResourceStatus
from DIRAC.ConfigurationSystem.Client.Helpers.Resources  import Resources
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
  ses.extend( res[ 'Value' ] )
if not ses:
  gLogger.error( 'There were no SEs provided' )
  DIRAC.exit()

readAllowed = []
writeAllowed = []
checkAllowed = []

resourceStatus = ResourceStatus()

res = resourceStatus.getStorageStatus( ses )
if not res[ 'OK' ]:
  gLogger.error( 'Storage Element %s does not exist' % ses )
  DIRAC.exit( -1 )

reason = 'Forced with dirac-admin-allow-se by %s' % userName

for se, seOptions in res[ 'Value' ].items():

  resW = resC = resR = { 'OK' : False }


  # InActive is used on the CS model, Banned is the equivalent in RSS
  if read and seOptions.has_key( 'ReadAccess' ):

    if not seOptions[ 'ReadAccess' ] in [ "InActive", "Banned", "Probing", "Degraded" ]:
      gLogger.notice( 'Read option for %s is %s, instead of %s' %
                      ( se, seOptions[ 'ReadAccess' ], [ "InActive", "Banned", "Probing", "Degraded" ] ) )
      gLogger.notice( 'Try specifying the command switches' )
      continue

    if 'ARCHIVE' in se:
      gLogger.notice( '%s is not supposed to change Read status to Active' % se )
      resR[ 'OK' ] = True
    else:  

      resR = resourceStatus.setStorageElementStatus( se, 'ReadAccess', 'Active', reason, userName )
      if not resR['OK']:
        gLogger.error( "Failed to update %s read access to Active" % se )
      else:
        gLogger.notice( "Successfully updated %s read access to Active" % se )
        readAllowed.append( se )

  # InActive is used on the CS model, Banned is the equivalent in RSS
  if write and seOptions.has_key( 'WriteAccess' ):

    if not seOptions[ 'WriteAccess' ] in [ "InActive", "Banned", "Probing", "Degraded" ]:
      gLogger.notice( 'Write option for %s is %s, instead of %s' %
                      ( se, seOptions[ 'WriteAccess' ], [ "InActive", "Banned", "Probing", "Degraded" ] ) )
      gLogger.notice( 'Try specifying the command switches' )
      continue

    resW = resourceStatus.setStorageElementStatus( se, 'WriteAccess', 'Active', reason, userName )
    if not resW['OK']:
      gLogger.error( "Failed to update %s write access to Active" % se )
    else:
      gLogger.notice( "Successfully updated %s write access to Active" % se )
      writeAllowed.append( se )

  # InActive is used on the CS model, Banned is the equivalent in RSS
  if check and seOptions.has_key( 'CheckAccess' ):

    if not seOptions[ 'CheckAccess' ] in [ "InActive", "Banned", "Probing", "Degraded" ]:
      gLogger.notice( 'Check option for %s is %s, instead of %s' %
                      ( se, seOptions[ 'CheckAccess' ], [ "InActive", "Banned", "Probing", "Degraded" ] ) )
      gLogger.notice( 'Try specifying the command switches' )
      continue

    resC = resourceStatus.setStorageElementStatus( se, 'CheckAccess', 'Active', reason, userName )
    if not resC['OK']:
      gLogger.error( "Failed to update %s check access to Active" % se )
    else:
      gLogger.notice( "Successfully updated %s check access to Active" % se )
      checkAllowed.append( se )

  if not( resR['OK'] or resW['OK'] or resC['OK'] ):
    DIRAC.exit( -1 )

if not ( writeAllowed or readAllowed or checkAllowed ):
  gLogger.info( "No storage elements were allowed" )
  DIRAC.exit( -1 )

if mute:
  gLogger.notice( 'Email is muted by script switch' )
  DIRAC.exit( 0 )

subject = '%s storage elements allowed for use' % len( writeAllowed + readAllowed + checkAllowed )
addressPath = 'EMail/Production'
address = Operations().getValue( addressPath, '' )


body = ''
if read:
  body = "%s\n\nThe following storage elements were allowed for reading:" % body
  for se in readAllowed:
    body = "%s\n%s" % ( body, se )
if write:
  body = "%s\n\nThe following storage elements were allowed for writing:" % body
  for se in writeAllowed:
    body = "%s\n%s" % ( body, se )
if check:
  body = "%s\n\nThe following storage elements were allowed for checking:" % body
  for se in checkAllowed:
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

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
