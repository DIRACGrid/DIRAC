#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Interfaces/scripts/dirac-wms-job-reschedule.py,v 1.1 2008/10/16 09:28:34 paterson Exp $
# File :   dirac-production-job-delete
# Author : Stuart Paterson
########################################################################
__RCSID__   = "$Id: dirac-wms-job-reschedule.py,v 1.1 2008/10/16 09:28:34 paterson Exp $"
__VERSION__ = "$Revision: 1.1 $"
from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Interfaces.API.Dirac                              import Dirac

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

def usage():
  print 'Usage: %s <JobID> |[<JobID>]' %(Script.scriptName)
  DIRAC.exit(2)

if len(args) < 1:
  usage()
  
dirac=Dirac()
exitCode = 0
errorList = []

for job in args:

  try:
    job = int(job)
  except Exception,x:
    errorList.append( ('Expected integer for jobID', job) )
    exitCode = 2
    continue

  result = dirac.reschedule(job)
  if result['OK']:
    print 'Rescheduled job %s' %(result['Value'][0])
  else:
    errorList.append( (job, result['Message']) )
    print result['Message']
    exitCode = 2

for error in errorList:
  print "ERROR %s: %s" % error

DIRAC.exit(exitCode)