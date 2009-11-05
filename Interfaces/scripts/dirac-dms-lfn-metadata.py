#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :   dirac-admin-lfn-metadata
# Author : Stuart Paterson
########################################################################
__RCSID__   = "$Id$"
__VERSION__ = "$Revision: 1.1 $"
from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Interfaces.API.Dirac                       import Dirac

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

def usage():
  print 'Usage: %s <LFN> [<LFN>]' %(Script.scriptName)
  DIRAC.exit(2)

if len(args) < 1:
  usage()

dirac = Dirac()
exitCode = 0
errorList = []

result = dirac.getMetadata(args,printOutput=True)
if not result['OK']:
  errorList.append( (lfn, result['Message']) )
  exitCode = 2

for error in errorList:
  print "ERROR %s: %s" % error

DIRAC.exit(exitCode)