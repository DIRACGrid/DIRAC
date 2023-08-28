#!/usr/bin/env python

""" This script is used to submit the jobs on the grid.
    It uses an executable (first argument), creates
    a directory in which it will store all the job ids (<jobName> args),
    and submit a configurable amount of jobs.
"""
import DIRAC

DIRAC.initialize()  # Initialize configuration

from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC.Interfaces.API.Job import Job

import sys
import os

if len(sys.argv) < 4:
    print(f"Usage {sys.argv[0]} <scriptName> <jobName> <nbJobs>")
    sys.exit(1)

scriptName = sys.argv[1]
jobName = sys.argv[2]
nbJobs = int(sys.argv[3])

if not os.path.exists(jobName):
    os.makedirs(jobName)
    os.makedirs(f"{jobName}/Done")
    os.makedirs(f"{jobName}/Failed")
else:
    print(f"Folder {jobName} exists")
    sys.exit(1)

f = open(f"{jobName}/jobIdList.txt", "w")

for i in range(nbJobs):
    j = Job()
    j.setCPUTime(10000)
    j.setExecutable(scriptName)
    j.addToOutputSandbox.append("myLog.txt")
    j.addToOutputSandbox.append("clock.txt")
    j.addToOutputSandbox.append("time.txt")
    dirac = Dirac()
    jobID = dirac.submitJob(j)
    realId = jobID.get("JobID")
    f.write(f"{realId}\n")

f.close()
