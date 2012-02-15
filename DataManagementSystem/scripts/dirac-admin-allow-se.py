#!/usr/bin/env python
########################################################################
# $HeadURL$
########################################################################
__RCSID__ = "$Id$"
import DIRAC
from DIRAC.Core.Base import Script

read  = True
write = True
check = True
site  = ''

Script.setUsageMessage( """
Enable using one or more Storage Elements

Usage:
   %s SE1 [SE2 ...]
""" % Script.scriptName )

Script.registerSwitch( "r" , "AllowRead" , "      Allow only reading from the storage element" )
Script.registerSwitch( "w" , "AllowWrite", "     Allow only writing to the storage element" )
Script.registerSwitch( "k" , "AllowCheck", "     Allow only check access to the storage element" )
Script.registerSwitch( "S:", "Site="     , "        Allow all SEs associated to site" )
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
  if switch[0] == "S" or switch[0].lower() == "site":
    site = switch[1]

#from DIRAC.ConfigurationSystem.Client.CSAPI           import CSAPI
from DIRAC                                           import gConfig, gLogger
from DIRAC.ConfigurationSystem.Client.Helpers        import ResourceStatus
from DIRAC.Core.Security.ProxyInfo                   import getProxyInfo
from DIRAC.Core.Utilities.List                       import intListToString
from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient

#csAPI = CSAPI()

res = getProxyInfo()
if not res[ 'OK' ]:
  gLogger.error( 'Failed to get proxy information', res[ 'Message' ] )
  DIRAC.exit( 2 )

userName = res[ 'Value' ][ 'username' ]
group    = res[ 'Value' ][ 'group' ]

if not type( ses ) == type( [] ):
  Script.showHelp()
  DIRAC.exit( -1 )

if site:
  res = gConfig.getOptionsDict( '/Resources/Sites/LCG/%s' % site )
  if not res[ 'OK' ]:
    gLogger.error( 'The provided site (%s) is not known.' % site )
    DIRAC.exit( -1 )
  ses.extend( res[ 'Value' ][ 'SE' ].replace( ' ', '' ).split( ',' ) )
if not ses:
  gLogger.error( 'There were no SEs provided' )
  DIRAC.exit()

readAllowed  = []
writeAllowed = []
checkAllowed = []

res = ResourceStatus.getStorageElementStatus( ses )
if not res[ 'OK' ]:
  gLogger.error( 'Storage Element %s does not exist' % ses )
  DIRAC.exit( -1 )

reason = 'Forced with dirac-admin-allow-se by %s' % userName

for se,seOptions in res[ 'Value' ].items():
  
  resW = resC = resR = { 'OK' : False }  
    
  # InActive is used on the CS model, Banned is the equivalent in RSS
  if read and seOptions.has_key( 'Read' ):
    
    if not seOptions[ 'Read' ] in [ "InActive", "Banned", "Probing" ]:
      gLogger.notice( 'Read option for %s is %s, instead of %s' % ( se, seOptions[ 'Read' ], [ "InActive", "Banned", "Probing" ] ) )    
      continue
     
    resR = ResourceStatus.setStorageElementStatus( se, 'Read', 'Active', reason, userName )
    if not resR['OK']:
      gLogger.error( "Failed to update %s read access to Active" % se )
    else:
      gLogger.notice( "Successfully updated %s read access to Active" % se )
      readAllowed.append( se )

  # InActive is used on the CS model, Banned is the equivalent in RSS
  if write and seOptions.has_key( 'Write' ):
    
    if not seOptions[ 'Write' ] in [ "InActive", "Banned", "Probing" ]:
      gLogger.notice( 'Write option for %s is %s, instead of %s' % ( se, seOptions[ 'Write' ], [ "InActive", "Banned", "Probing" ] ) )    
      continue
    
    resW = ResourceStatus.setStorageElementStatus( se, 'Write', 'Active', reason, userName )
    if not resW['OK']:
      gLogger.error( "Failed to update %s write access to Active" % se )
    else:
      gLogger.notice( "Successfully updated %s write access to Active" % se )
      writeAllowed.append( se )

  # InActive is used on the CS model, Banned is the equivalent in RSS 
  if check and seOptions.has_key( 'Check' ):

    if not seOptions[ 'Check' ] in [ "InActive", "Banned", "Probing" ]:
      gLogger.notice( 'Check option for %s is %s, instead of %s' % ( se, seOptions[ 'Check' ], [ "InActive", "Banned", "Probing" ] ) )    
      continue
    
    resC = ResourceStatus.setStorageElementStatus( se, 'Check', 'Active', reason, userName )
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

subject = '%s storage elements allowed for use' % len( ses )
address = gConfig.getValue( '/Operations/EMail/Production', 'lhcb-grid@cern.ch' )
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

NotificationClient().sendMail( address, subject, body, '%s@cern.ch' % userName )
DIRAC.exit( 0 )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF