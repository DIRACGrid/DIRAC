""" Collection of user jobs for testing purposes
"""

# pylint: disable=invalid-name

import os

from DIRAC import rootPath
from DIRAC.Interfaces.API.Job import Job
from DIRAC.Interfaces.API.Dirac import Dirac

from DIRAC.tests.Utilities.utils import find_all

# parameters

# Common functions


def getJob(jobClass=None):
  if not jobClass:
    jobClass = Job
  oJob = jobClass()
  return oJob


def getDIRAC(diracClass=None):
  if not diracClass:
    diracClass = Dirac
  oDirac = diracClass()
  return oDirac


def baseToAllJobs(jName, jobClass=None):
  print "**********************************************************************************************************"
  print "\n Submitting job ", jName

  J = getJob(jobClass)
  J.setName(jName)
  J.setCPUTime(17800)
  return J


def endOfAllJobs(J):
  result = getDIRAC().submit(J)
  print "Job submission result:", result
  if result['OK']:
    print "Submitted with job ID:", result['Value']

  return result


# List of jobs

def helloWorld():
  """ simple hello world job
  """

  J = baseToAllJobs('helloWorld')
  try:
    J.setInputSandbox([find_all('exe-script.py', rootPath, 'DIRAC/tests/Workflow')[0]])
  except IndexError:  # we are in Jenkins
    J.setInputSandbox([find_all('exe-script.py', os.environ['WORKSPACE'], 'DIRAC/tests/Workflow')[0]])
  J.setExecutable("exe-script.py", "", "helloWorld.log")
  return endOfAllJobs(J)


def mpJob():
  """ simple hello world job, with MultiProcessor tag
  """

  J = baseToAllJobs('mpJob')
  try:
    J.setInputSandbox([find_all('mpTest.py', rootPath, 'DIRAC/tests/Utilities')[0]] +
                      [find_all('testMpJob.sh', rootPath, 'DIRAC/tests/Utilities')[0]])
  except IndexError:  # we are in Jenkins
    J.setInputSandbox([find_all('mpTest.py', os.environ['WORKSPACE'], 'DIRAC/tests/Utilities')[0]] +
                      [find_all('testMpJob.sh', os.environ['WORKSPACE'], 'DIRAC/tests/Utilities')[0]])

  J.setExecutable('testMpJob.sh mpTest.py')
  J.setTag('MultiProcessor')
  return endOfAllJobs(J)


def parametricJob():
  """ Creates a parametric job with 3 subjobs which are simple hello world jobs
  """

  J = baseToAllJobs('helloWorld')
  try:
    J.setInputSandbox([find_all('exe-script.py', rootPath, 'DIRAC/tests/Workflow')[0]])
  except IndexError:  # we are in Jenkins
    J.setInputSandbox([find_all('exe-script.py', os.environ['WORKSPACE'], 'DIRAC/tests/Workflow')[0]])
  J.setParameterSequence("args", ['one', 'two', 'three'])
  J.setParameterSequence("iargs", [1, 2, 3])
  J.setExecutable("exe-script.py", arguments=": testing %(args)s %(iargs)s", logFile='helloWorld_%n.log')
  return endOfAllJobs(J)
