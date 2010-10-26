#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :   dirac-production-job-get-output
# Author : Stuart Paterson
########################################################################

__RCSID__   = "$Id$"

import DIRAC
from DIRAC.Core.Base import Script
import os

Script.registerSwitch( "d:", "dir=", "Store the output in this directory" )

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

from DIRAC.Interfaces.API.Dirac  import Dirac

def usage():
  print 'Usage: %s <JobID> |[<JobID>]' %(Script.scriptName)
  DIRAC.exit(2)

if len(args) < 1:
  usage()
  
dirac=Dirac()
exitCode = 0
errorList = []

outputDir = None
for sw,v in Script.getUnprocessedSwitches():
  if sw in ( 'd', 'dir' ):
    outputDir = v

for job in args:

  try:
    job = int(job)
  except Exception,x:
    errorList.append( ('Expected integer for jobID', job) )
    exitCode = 2
    continue

  result = dirac.getOutputSandbox(job, outputDir)
  if result['OK']:
    if os.path.exists('%s' %job):
      print 'Job output sandbox retrieved in %s/' %(job)
  else:
    errorList.append( (job, result['Message']) )
    exitCode = 2

for error in errorList:
  print "ERROR %s: %s" % error

DIRAC.exit(exitCode)