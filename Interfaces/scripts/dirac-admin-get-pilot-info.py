#!/usr/bin/env python
########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/DIRAC/trunk/DIRAC/Interfaces/scripts/dirac-admin-get-pilot-output.py $
# File :   dirac-admin-get-pilot-output
# Author : Ricardo Graciani
########################################################################
#
# Retrieve available info about the given pilotid
# 
__RCSID__   = "$Id: dirac-admin-get-pilot-output.py 18161 2009-11-11 12:07:09Z acasajus $"
__VERSION__ = "$Revision: 1.1 $"

import DIRAC
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

  result = diracAdmin.getPilotInfo(gridID)
  if not result['OK']:
    errorList.append( ( gridID, result['Message']) )
    exitCode = 2
  else:
    print diracAdmin.pPrint.pformat(result['Value'])


for error in errorList:
  print "ERROR %s: %s" % error

DIRAC.exit(exitCode)