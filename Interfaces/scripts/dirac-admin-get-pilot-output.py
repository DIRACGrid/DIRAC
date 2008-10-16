#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Interfaces/scripts/dirac-admin-get-pilot-output.py,v 1.1 2008/10/16 09:21:28 paterson Exp $
# File :   dirac-admin-get-pilot-output
# Author : Stuart Paterson
########################################################################
__RCSID__   = "$Id: dirac-admin-get-pilot-output.py,v 1.1 2008/10/16 09:21:28 paterson Exp $"
__VERSION__ = "$Revision: 1.1 $"
from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Interfaces.API.DiracAdmin                         import DiracAdmin

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

def usage():
  print 'Usage: %s <Grid pilot reference> [<Grid pilot reference>]' %(Script.scriptName)
  DIRAC.exit(2)

if len(args) < 1:
  usage()
  
diracAdmin = DiracAdmin()
exitCode = 0
errorList = []

for gridID in args:

  result = diracAdmin.getPilotOutput(gridID)
  if not result['OK']:
    errorList.append( ( gridID, result['Message']) )
    exitCode = 2

for error in errorList:
  print "ERROR %s: %s" % error

DIRAC.exit(exitCode)