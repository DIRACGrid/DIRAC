#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :   dirac-admin-sync-users-from-file
# Author : Adrian Casajus
########################################################################
__RCSID__   = "$Id$"
__VERSION__ = "$Revision: 1.3 $"
import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.CFG import CFG

Script.registerSwitch( "t", "test", "Only test. Don't commit changes" )

from DIRAC.Interfaces.API.DiracAdmin                         import DiracAdmin

args = Script.getExtraCLICFGFiles()

def usage():
  print 'Usage: %s <fileWithUsers>.cfg' %(Script.scriptName)
  print ' File has to contain a user sections with DN groups and extra properties as options'
  DIRAC.exit(2)


diracAdmin = DiracAdmin()
exitCode = 0
testOnly = False
errorList = []

if len(args) < 1:
  usage()

userProps = {}
for unprocSw in Script.getUnprocessedSwitches():
  if unprocSw[0] in ( "t", "test" ):
    testOnly = True


try:
  usersCFG = CFG().loadFromFile( args[0] )
except Exception, e:
  errorList.append( "file open", "Can't parse file %s: %s" % ( args[0], str(e) ) )
  errorCode = 1
else:
  if not diracAdmin.csSyncUsersWithCFG( usersCFG ):
    errorList.append( ( "modify user", "Cannot sync with %s" % args[0] ) )
    exitCode = 255

if not exitCode and not testOnly:
  result = diracAdmin.csCommitChanges()
  if not result[ 'OK' ]:
    errorList.append( ( "commit", result[ 'Message' ] ) )
    exitCode = 255

for error in errorList:
  print "ERROR %s: %s" % error

DIRAC.exit(exitCode)
