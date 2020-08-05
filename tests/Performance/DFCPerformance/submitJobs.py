#!/usr/bin/env python

""" This script is used to submit the jobs on the grid.
    It uses an executable (first argument), creates 
    a directory in which it will store all the job ids (<jobName> args),
    and submit a configurable amount of jobs.

"""

from __future__ import print_function
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC.Interfaces.API.Job import Job

import sys
import os

if len(sys.argv) < 4:
  print("Usage %s <scriptName> <jobName> <nbJobs>" % sys.argv[0])
  sys.exit(1)

scriptName = sys.argv[1]
jobName = sys.argv[2]
nbJobs = int(sys.argv[3])

if not os.path.exists(jobName):
  os.makedirs(jobName)
  os.makedirs("%s/Done"%jobName)
  os.makedirs("%s/Failed"%jobName)
else:
  print("Folder %s exists" % jobName)
  sys.exit(1)      

f = open("%s/jobIdList.txt"%jobName, 'w')

for i in xrange(nbJobs):
  j = Job()
  j.setCPUTime(10000)
  j.setExecutable(scriptName)
  j.addToOutputSandbox.append('myLog.txt')
  j.addToOutputSandbox.append('clock.txt')
  j.addToOutputSandbox.append('time.txt')
  dirac = Dirac()
  jobID = dirac.submitJob(j)
  realId = jobID.get('JobID')
  f.write("%s\n"%realId)

f.close()
