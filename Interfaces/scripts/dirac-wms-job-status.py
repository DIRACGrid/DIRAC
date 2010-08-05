#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :   dirac-production-job-status
# Author : Stuart Paterson
########################################################################
__RCSID__   = "$Id$"
__VERSION__ = "$Revision: 1.1 $"
import DIRAC
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

try:
  jobs = [ int(job) for job in args ]
except Exception,x:
  print 'Expected integer for jobID'
  exitCode = 2
  DIRAC.exit(exitCode)

result = dirac.status(jobs)
if result['OK']:
  for job in result['Value']:
    print 'JobID='+str(job), 
    for status in result['Value'][job]:
      print status+'='+result['Value'][job][status]+';',
    print  
else:
  print "ERROR: %s" % error

DIRAC.exit(exitCode)