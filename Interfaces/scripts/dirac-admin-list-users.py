#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Interfaces/scripts/dirac-admin-list-users.py,v 1.1 2008/10/16 09:21:28 paterson Exp $
# File :   dirac-admin-list-users
# Author : Adrian Casajus
########################################################################
__RCSID__   = "$Id: dirac-admin-list-users.py,v 1.1 2008/10/16 09:21:28 paterson Exp $"
__VERSION__ = "$Revision: 1.1 $"
from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script

Script.registerSwitch( "e", "extended", "Show extended info" )

from DIRAC.Interfaces.API.DiracAdmin                         import DiracAdmin

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

def usage():
  print 'Usage: %s [<DIRAC group>/all]+' %(Script.scriptName)
  DIRAC.exit(2)

if len(args) < 1:
  usage()

diracAdmin = DiracAdmin()
exitCode = 0
errorList = []
extendedInfo = False

for unprocSw in Script.getUnprocessedSwitches():
  if unprocSw[0] in ( 'e', 'extended' ):
    extendedInfo = True

def printUsersInGroup( group = False ):
  result = diracAdmin.csListUsers( group )
  if result[ 'OK' ]:
    if group:
      print "Users in group %s:" % group
    else:
      print "All users registered:"
    for username in result[ 'Value' ]:
      print " %s" % username

def describeUsersInGroup( group = False ):
  result = diracAdmin.csListUsers( group )
  if result[ 'OK' ]:
    if group:
      print "Users in group %s:" % group
    else:
      print "All users registered:"
    result = diracAdmin.csDescribeUsers( result[ 'Value' ] )
    print diracAdmin.pPrint.pformat( result[ 'Value' ] )

for group in args:
  if 'all' in args:
    group = False
  if not extendedInfo:
    printUsersInGroup( group )
  else:
    describeUsersInGroup( group )

for error in errorList:
  print "ERROR %s: %s" % error

DIRAC.exit(exitCode)