#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-production-job-submit
# Author :  Stuart Paterson
########################################################################
__RCSID__ = "$Id$"
import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Interfaces.API.Dirac                              import Dirac

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

def usage():
  print 'Usage: %s <Path to JDL file> |[<Path to JDL file>]' % ( Script.scriptName )
  DIRAC.exit( 2 )

if len( args ) < 1:
  usage()

dirac = Dirac()
exitCode = 0
errorList = []

for jdl in args:

  result = dirac.submit( jdl )
  if result['OK']:
    print 'JobID = %s' % ( result['Value'] )
  else:
    errorList.append( ( jdl, result['Message'] ) )
    exitCode = 2

for error in errorList:
  print "ERROR %s: %s" % error

DIRAC.exit( exitCode )
