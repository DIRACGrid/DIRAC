""" Collection of user jobs for testing purposes
"""

# pylint: disable=invalid-name

from __future__ import print_function
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
  print("**********************************************************************************************************")
  print("\n Submitting job ", jName)

  J = getJob(jobClass)
  J.setName(jName)
  J.setCPUTime(17800)
  return J


def endOfAllJobs(J):
  result = getDIRAC().submitJob(J)
  print("Job submission result:", result)
  if result['OK']:
    print("Submitted with job ID:", result['Value'])

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


def helloWorldCERN():
  """ simple hello world job to CERN
  """

  J = baseToAllJobs('helloWorld')
  try:
    J.setInputSandbox([find_all('exe-script.py', rootPath, 'DIRAC/tests/Workflow')[0]])
  except IndexError:  # we are in Jenkins
    J.setInputSandbox([find_all('exe-script.py', os.environ['WORKSPACE'], 'DIRAC/tests/Workflow')[0]])
  J.setExecutable("exe-script.py", "", "helloWorld.log")
  J.setDestination('LCG.CERN.cern')
  return endOfAllJobs(J)


def helloWorldNCBJ():
  """ simple hello world job to NCBJ
  """

  J = baseToAllJobs('helloWorld')
  try:
    J.setInputSandbox([find_all('exe-script.py', rootPath, 'DIRAC/tests/Workflow')[0]])
  except IndexError:  # we are in Jenkins
    J.setInputSandbox([find_all('exe-script.py', os.environ['WORKSPACE'], 'DIRAC/tests/Workflow')[0]])
  J.setExecutable("exe-script.py", "", "helloWorld.log")
  J.setDestination('LCG.NCBJ.pl')
  return endOfAllJobs(J)


def helloWorldGRIDKA():
  """ simple hello world job to GRIDKA
  """

  J = baseToAllJobs('helloWorld')
  try:
    J.setInputSandbox([find_all('exe-script.py', rootPath, 'DIRAC/tests/Workflow')[0]])
  except IndexError:  # we are in Jenkins
    J.setInputSandbox([find_all('exe-script.py', os.environ['WORKSPACE'], 'DIRAC/tests/Workflow')[0]])
  J.setExecutable("exe-script.py", "", "helloWorld.log")
  J.setDestination('LCG.GRIDKA.de')
  return endOfAllJobs(J)


def helloWorldGRIF():
  """ simple hello world job to GRIF
  """

  J = baseToAllJobs('helloWorld')
  try:
    J.setInputSandbox([find_all('exe-script.py', rootPath, 'DIRAC/tests/Workflow')[0]])
  except IndexError:  # we are in Jenkins
    J.setInputSandbox([find_all('exe-script.py', os.environ['WORKSPACE'], 'DIRAC/tests/Workflow')[0]])
  J.setExecutable("exe-script.py", "", "helloWorld.log")
  J.setDestination('LCG.GRIF.fr')
  return endOfAllJobs(J)


def helloWorldSSHBatch():
  """ simple hello world job to DIRAC.Jenkins_SSHBatch.ch
  """

  J = baseToAllJobs('helloWorld')
  try:
    J.setInputSandbox([find_all('exe-script.py', rootPath, 'DIRAC/tests/Workflow')[0]])
  except IndexError:  # we are in Jenkins
    J.setInputSandbox([find_all('exe-script.py', os.environ['WORKSPACE'], 'DIRAC/tests/Workflow')[0]])
  J.setExecutable("exe-script.py", "", "helloWorld.log")
  J.setDestination('DIRAC.Jenkins_SSHBatch.ch')
  return endOfAllJobs(J)


def mpJob():
  """ simple hello world job, with 4Processors and MultiProcessor tags
  """

  J = baseToAllJobs('mpJob')
  try:
    J.setInputSandbox([find_all('mpTest.py', rootPath, 'DIRAC/tests/Utilities')[0]])
  except IndexError:  # we are in Jenkins
    J.setInputSandbox([find_all('mpTest.py', os.environ['WORKSPACE'], 'DIRAC/tests/Utilities')[0]])

  J.setExecutable('mpTest.py')
  J.setTag(['4Processors', 'MultiProcessor'])
  return endOfAllJobs(J)


def wholeNodeJob():
  """ simple hello world job, with WholeNode and MultiProcessor tags
  """

  J = baseToAllJobs('mpJob')
  try:
    J.setInputSandbox([find_all('mpTest.py', rootPath, 'DIRAC/tests/Utilities')[0]])
  except IndexError:  # we are in Jenkins
    J.setInputSandbox([find_all('mpTest.py', os.environ['WORKSPACE'], 'DIRAC/tests/Utilities')[0]])

  J.setExecutable('mpTest.py')
  J.setTag(['WholeNode', 'MultiProcessor'])
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
