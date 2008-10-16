#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Interfaces/scripts/dirac-wms-job-submit.py,v 1.1 2008/10/16 09:28:34 paterson Exp $
# File :   dirac-production-job-submit
# Author : Stuart Paterson
########################################################################
__RCSID__   = "$Id: dirac-wms-job-submit.py,v 1.1 2008/10/16 09:28:34 paterson Exp $"
__VERSION__ = "$Revision: 1.1 $"
from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Interfaces.API.Dirac                              import Dirac

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

def usage():
  print 'Usage: %s <Path to JDL file> |[<Path to JDL file>]' %(Script.scriptName)
  DIRAC.exit(2)

if len(args) < 1:
  usage()
  
dirac=Dirac()
exitCode = 0
errorList = []

for jdl in args:

  result = dirac.submit(jdl)
  if result['OK']:
    print 'JobID = %s' %(result['Value'])
  else:
    errorList.append( (jdl, result['Message']) )
    exitCode = 2

for error in errorList:
  print "ERROR %s: %s" % error

DIRAC.exit(exitCode)