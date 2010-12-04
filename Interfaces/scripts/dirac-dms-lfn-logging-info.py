#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-dms-lfn-logging-ingo
# Author :  Stuart Paterson
########################################################################
__RCSID__ = "$Id$"
import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Interfaces.API.Dirac                              import Dirac

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

def usage():
  print 'Usage: %s <LFN> |[<LFN>]' % ( Script.scriptName )
  DIRAC.exit( 2 )

if len( args ) < 1:
  usage()

dirac = Dirac()
exitCode = 0
errorList = []

for job in args:

  try:
    job = str( job )
  except Exception, x:
    errorList.append( ( 'Expected integer for LFN', job ) )
    exitCode = 2
    continue

  result = dirac.dataLoggingInfo( job, printOutput = True )
  if not result['OK']:
    errorList.append( ( job, result['Message'] ) )
    exitCode = 2

for error in errorList:
  print "ERROR %s: %s" % error

DIRAC.exit( exitCode )
