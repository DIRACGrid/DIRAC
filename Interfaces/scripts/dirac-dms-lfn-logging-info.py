#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Interfaces/scripts/dirac-dms-lfn-logging-info.py,v 1.1 2008/10/16 09:28:33 paterson Exp $
# File :   dirac-dms-lfn-logging-ingo
# Author : Stuart Paterson
########################################################################
__RCSID__   = "$Id: dirac-dms-lfn-logging-info.py,v 1.1 2008/10/16 09:28:33 paterson Exp $"
__VERSION__ = "$Revision: 1.1 $"
from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Interfaces.API.Dirac                              import Dirac

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

def usage():
  print 'Usage: %s <LFN> |[<LFN>]' %(Script.scriptName)
  DIRAC.exit(2)

if len(args) < 1:
  usage()

dirac=Dirac()
exitCode = 0
errorList = []

for job in args:

  try:
    job = str(job)
  except Exception,x:
    errorList.append( ('Expected integer for LFN', job) )
    exitCode = 2
    continue

  result = dirac.dataLoggingInfo(job,printOutput=True)
  if not result['OK']:
    errorList.append( (job, result['Message']) )
    exitCode = 2

for error in errorList:
  print "ERROR %s: %s" % error

DIRAC.exit(exitCode)