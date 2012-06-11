#!/usr/bin/env python
########################################################################
# $HeadURL$
########################################################################
__RCSID__ = "$Id$"
import DIRAC
from DIRAC.Core.Base                                   import Script

userName = None
userDN = None
userMail = None
userGroups = []

def setUserName( arg ):
  global userName
  if userName or not arg:
    Script.showHelp()
    DIRAC.exit( -1 )
  userName = arg

def setUserDN( arg ):
  global userDN
  if userDN or not arg:
    Script.showHelp()
    DIRAC.exit( -1 )
  userDN = arg

def setUserMail( arg ):
  global userMail
  if userMail or not arg:
    Script.showHelp()
    DIRAC.exit( -1 )
  if not arg.find( '@' ) > 0:
    Script.gLogger.error( 'Not a valid mail address', arg )
    DIRAC.exit( -1 )
  userMail = arg

def addUserGroup( arg ):
  global userGroups
  if not arg:
    Script.showHelp()
    DIRAC.exit( -1 )
  if not arg in userGroups:
    userGroups.append( arg )

Script.setUsageMessage( '\n'.join( ['Add or Modify a User info in DIRAC',
                                    'Usage:',
                                    '%s [option|cfgfile] ... Property=<Value> ...' % Script.scriptName,
                                    'Arguments:',
                                    ' Property=<Value>: Properties to be added to the User like (Phone=XXXX)', ] ) )

Script.registerSwitch( 'N:', 'UserName:', 'Short Name of the User (Mandatory)', setUserName )
Script.registerSwitch( 'D:', 'UserDN:', 'DN of the User Certificate (Mandatory)', setUserDN )
Script.registerSwitch( 'M:', 'UserMail:', 'eMail of the user (Mandatory)', setUserMail )
Script.registerSwitch( 'G:', 'UserGroup:', 'Name of the Group for the User (Allow Multiple instances or None)', addUserGroup )

Script.parseCommandLine( ignoreErrors = True )

if userName == None or userDN == None or userMail == None:
  Script.showHelp()
  DIRAC.exit( -1 )

args = Script.getPositionalArgs()

from DIRAC.Interfaces.API.DiracAdmin                         import DiracAdmin
diracAdmin = DiracAdmin()
exitCode = 0
errorList = []

userProps = {'DN': userDN, 'Email': userMail}
if userGroups:
  userProps['Groups' ] = userGroups
for prop in args:
  pl = prop.split( "=" )
  if len( pl ) < 2:
    errorList.append( ( "in arguments", "Property %s has to include a '=' to separate name from value" % prop ) )
    exitCode = 255
  else:
    pName = pl[0]
    pValue = "=".join( pl[1:] )
    Script.gLogger.info( "Setting property %s to %s" % ( pName, pValue ) )
    userProps[ pName ] = pValue

if not diracAdmin.csModifyUser( userName, userProps, createIfNonExistant = True )['OK']:
  errorList.append( ( "add user", "Cannot register user %s" % userName ) )
  exitCode = 255
else:
  result = diracAdmin.csCommitChanges()
  if not result[ 'OK' ]:
    errorList.append( ( "commit", result[ 'Message' ] ) )
    exitCode = 255

for error in errorList:
  Script.gLogger.error( "%s: %s" % error )

DIRAC.exit( exitCode )
