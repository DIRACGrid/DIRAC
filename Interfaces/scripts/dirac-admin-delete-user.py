#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Interfaces/scripts/dirac-admin-delete-user.py,v 1.1 2008/10/16 09:21:28 paterson Exp $
# File :   dirac-admin-modify-user
# Author : Adrian Casajus
########################################################################
__RCSID__   = "$Id: dirac-admin-delete-user.py,v 1.1 2008/10/16 09:21:28 paterson Exp $"
__VERSION__ = "$Revision: 1.1 $"
from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script

Script.registerSwitch( "P:", "property=", "Add property to the user <name>=<value>" )

from DIRAC.Interfaces.API.DiracAdmin                         import DiracAdmin

args = Script.getPositionalArgs()

def usage():
  print 'Usage: %s <username>+' %(Script.scriptName)
  DIRAC.exit(2)


diracAdmin = DiracAdmin()
exitCode = 0
errorList = []

if len(args) < 1:
  usage()

choice = raw_input( "Are you sure you want to delete user/s %s? yes/no [no]: " % ", ".join( args ) )
choice = choice.lower()
if choice not in ( "yes", "y" ):
  print "Delete aborted"
  DIRAC.exit(0)

for user in args:
  if not diracAdmin.csDeleteUser( user ):
    errorList.append( ( "delete user", "Cannot delete user %s" % user ) )
    exitCode = 255

if not exitCode:
  result = diracAdmin.csCommitChanges()
  if not result[ 'OK' ]:
    errorList.append( ( "commit", result[ 'Message' ] ) )
    exitCode = 255

for error in errorList:
  print "ERROR %s: %s" % error

DIRAC.exit(exitCode)