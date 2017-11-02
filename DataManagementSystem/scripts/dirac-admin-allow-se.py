#!/usr/bin/env python
########################################################################
# $HeadURL$
########################################################################
""" Enable using one or more Storage Elements
"""
__RCSID__ = "$Id$"
import DIRAC
from DIRAC.Core.Base import Script

read = False
write = False
check = False
remove = False
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
Script.registerSwitch( "v" , "AllowRemove", "    Allow only remove access to the storage element" )
Script.registerSwitch( "m" , "Mute"      , "     Do not send email" )
Script.registerSwitch( "S:", "Site="     , "     Allow all SEs associated to site" )

Script.parseCommandLine( ignoreErrors = True )

ses = Script.getPositionalArgs()
for switch in Script.getUnprocessedSwitches():
  if switch[0].lower() == "r" or switch[0].lower() == "allowread":
    read = True
  if switch[0].lower() == "w" or switch[0].lower() == "allowwrite":
    write = True
  if switch[0].lower() == "k" or switch[0].lower() == "allowcheck":
    check = True
  if switch[0].lower() == "v" or switch[0].lower() == "allowremove":
    remove = True
  if switch[0].lower() == "m" or switch[0].lower() == "mute":
    mute = True
  if switch[0] == "S" or switch[0].lower() == "site":
    site = switch[1]

# from DIRAC.ConfigurationSystem.Client.CSAPI           import CSAPI
from DIRAC.Interfaces.API.DiracAdmin                     import DiracAdmin
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC                                               import gConfig, gLogger
from DIRAC.ResourceStatusSystem.Client.ResourceStatus    import ResourceStatus
from DIRAC.Core.Security.ProxyInfo                       import getProxyInfo
from DIRAC.DataManagementSystem.Utilities.DMSHelpers import resolveSEGroup

if not ( read or write or check or remove ):
  # No switch was specified, means we need all of them
  gLogger.notice( "No option given, all accesses will be allowed if they were not" )
  read = True
  write = True
  check = True
  remove = True

ses = resolveSEGroup( ses )
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
  res = gConfig.getOptionsDict( '/Resources/Sites/LCG/%s' % site )
  if not res[ 'OK' ]:
    gLogger.error( 'The provided site (%s) is not known.' % site )
    DIRAC.exit( -1 )
  ses.extend( res[ 'Value' ][ 'SE' ].replace( ' ', '' ).split( ',' ) )
if not ses:
  gLogger.error( 'There were no SEs provided' )
  DIRAC.exit()

STATUS_TYPES = [ "ReadAccess", "WriteAccess", "CheckAccess", "RemoveAccess" ]
ALLOWED_STATUSES = [ "Unknown", "InActive", "Banned", "Probing", "Degraded" ]

statusAllowedDict = {}
for statusType in STATUS_TYPES:
  statusAllowedDict[statusType] = []

statusFlagDict = {}
statusFlagDict['ReadAccess'] = read
statusFlagDict['WriteAccess'] = write
statusFlagDict['CheckAccess'] = check
statusFlagDict['RemoveAccess'] = remove

resourceStatus = ResourceStatus()

res = resourceStatus.getElementStatus( ses, "StorageElement" )
if not res[ 'OK' ]:
  gLogger.error( 'Storage Element %s does not exist' % ses )
  DIRAC.exit( -1 )

reason = 'Forced with dirac-admin-allow-se by %s' % userName

for se, seOptions in res[ 'Value' ].iteritems():

  # InActive is used on the CS model, Banned is the equivalent in RSS
  for statusType in STATUS_TYPES:
    if statusFlagDict[statusType]:
      if seOptions.get( statusType ) == "Active":
        gLogger.notice( '%s status of %s is already Active' % ( statusType, se ) )
        continue
      if statusType in seOptions:
        if not seOptions[ statusType ] in ALLOWED_STATUSES:
          gLogger.notice( '%s option for %s is %s, instead of %s' %
                          ( statusType, se, seOptions[ 'ReadAccess' ], ALLOWED_STATUSES ) )
          gLogger.notice( 'Try specifying the command switches' )
        else:
          resR = resourceStatus.setElementStatus( se, "StorageElement", statusType, 'Active', reason, userName )
          if not resR['OK']:
            gLogger.fatal( "Failed to update %s %s to Active, exit -" % ( se, statusType ), resR['Message'] )
            DIRAC.exit( -1 )
          else:
            gLogger.notice( "Successfully updated %s %s to Active" % ( se, statusType ) )
            statusAllowedDict[statusType].append( se )

totalAllowed = 0
totalAllowedSEs = []
for statusType in STATUS_TYPES:
  totalAllowed += len( statusAllowedDict[statusType] )
  totalAllowedSEs += statusAllowedDict[statusType]
totalAllowedSEs = list( set( totalAllowedSEs ) )

if not totalAllowed:
  gLogger.info( "No storage elements were allowed" )
  DIRAC.exit( -1 )

if mute:
  gLogger.notice( 'Email is muted by script switch' )
  DIRAC.exit( 0 )

subject = '%s storage elements allowed for use' % len( totalAllowedSEs )
addressPath = 'EMail/Production'
address = Operations().getValue( addressPath, '' )


body = ''
if read:
  body = "%s\n\nThe following storage elements were allowed for reading:" % body
  for se in statusAllowedDict['ReadAccess']:
    body = "%s\n%s" % ( body, se )
if write:
  body = "%s\n\nThe following storage elements were allowed for writing:" % body
  for se in statusAllowedDict['WriteAccess']:
    body = "%s\n%s" % ( body, se )
if check:
  body = "%s\n\nThe following storage elements were allowed for checking:" % body
  for se in statusAllowedDict['CheckAccess']:
    body = "%s\n%s" % ( body, se )
if remove:
  body = "%s\n\nThe following storage elements were allowed for removing:" % body
  for se in statusAllowedDict['RemoveAccess']:
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
# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
