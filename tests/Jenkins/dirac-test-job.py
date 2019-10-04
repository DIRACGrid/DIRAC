#!/usr/bin/env python
""" Submission of test jobs for use by Jenkins
"""

# pylint: disable=wrong-import-position,unused-wildcard-import,wildcard-import

import os.path

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC import gLogger

from DIRAC.tests.Utilities.utils import find_all

from DIRAC.Interfaces.API.Job import Job
from DIRAC.Interfaces.API.Dirac import Dirac
#from tests.Workflow.Integration.Test_UserJobs import createJob

gLogger.setLevel('DEBUG')

cwd = os.path.realpath('.')

dirac = Dirac()

# Simple Hello Word job to DIRAC.Jenkins.ch
gLogger.info("\n Submitting hello world job targeting DIRAC.Jenkins.ch")
helloJ = Job()
helloJ.setName("helloWorld-TEST-TO-Jenkins")
helloJ.setInputSandbox([find_all('exe-script.py', '..', '/DIRAC/tests/Workflow/')[0]])
helloJ.setExecutable("exe-script.py", "", "helloWorld.log")
helloJ.setCPUTime(1780)
helloJ.setDestination('DIRAC.Jenkins.ch')
helloJ.setLogLevel('DEBUG')
result = dirac.submitJob(helloJ)
gLogger.info("Hello world job: ", result)
if not result['OK']:
  gLogger.error("Problem submitting job", result['Message'])
  exit(1)

# Simple Hello Word job to DIRAC.Jenkins.ch, that needs to be matched by a MP WN
gLogger.info("\n Submitting hello world job targeting DIRAC.Jenkins.ch and a MP WN")
helloJMP = Job()
helloJMP.setName("helloWorld-TEST-TO-Jenkins-MP")
helloJMP.setInputSandbox([find_all('exe-script.py', '..', '/DIRAC/tests/Workflow/')[0]])
helloJMP.setExecutable("exe-script.py", "", "helloWorld.log")
helloJMP.setCPUTime(1780)
helloJMP.setDestination('DIRAC.Jenkins.ch')
helloJMP.setLogLevel('DEBUG')
helloJMP.setNumberOfProcessors(2)
result = dirac.submitJob(helloJMP)  # this should make the difference!
gLogger.info("Hello world job MP: ", result)
if not result['OK']:
  gLogger.error("Problem submitting job", result['Message'])
  exit(1)
