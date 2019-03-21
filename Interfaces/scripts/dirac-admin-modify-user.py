#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-admin-modify-user
# Author :  Adrian Casajus
########################################################################
"""
  Modify a user in the CS.
"""
from __future__ import print_function
__RCSID__ = "$Id$"
import DIRAC
from DIRAC.Core.Base import Script

Script.registerSwitch( "p:", "property=", "Add property to the user <name>=<value>" )
Script.registerSwitch( "f", "force", "create the user if it doesn't exist" )

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... user DN group [group] ...' % Script.scriptName,
                                     'Arguments:',
                                     '  user:     User name',
                                     '  DN:       DN of the User',
                                     '  group:    Add the user to the group' ] ) )
Script.parseCommandLine( ignoreErrors = True )

args = Script.getPositionalArgs()

if len( args ) < 3:
  Script.showHelp()

from DIRAC.Interfaces.API.DiracAdmin                         import DiracAdmin
diracAdmin = DiracAdmin()
exitCode = 0
forceCreation = False
errorList = []

userProps = {}
for unprocSw in Script.getUnprocessedSwitches():
  if unprocSw[0] in ( "f", "force" ):
    forceCreation = True
  elif unprocSw[0] in ( "p", "property" ):
    prop = unprocSw[1]
    pl = prop.split( "=" )
    if len( pl ) < 2:
      errorList.append( ( "in arguments", "Property %s has to include a '=' to separate name from value" % prop ) )
      exitCode = 255
    else:
      pName = pl[0]
      pValue = "=".join( pl[1:] )
      print("Setting property %s to %s" % (pName, pValue))
      userProps[ pName ] = pValue

userName = args[0]
userProps[ 'DN' ] = args[1]
userProps[ 'Groups' ] = args[2:]

if not diracAdmin.csModifyUser( userName, userProps, createIfNonExistant = forceCreation ):
  errorList.append( ( "modify user", "Cannot modify user %s" % userName ) )
  exitCode = 255
else:
  result = diracAdmin.csCommitChanges()
  if not result[ 'OK' ]:
    errorList.append( ( "commit", result[ 'Message' ] ) )
    exitCode = 255

for error in errorList:
  print("ERROR %s: %s" % error)

DIRAC.exit( exitCode )
