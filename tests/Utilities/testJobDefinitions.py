""" Collection of user jobs for testing purposes
"""

import os
from DIRAC.tests.Utilities.utils import find_all
from DIRAC.Interfaces.API.Job import Job
from DIRAC.Interfaces.API.Dirac import Dirac

# parameters


# Common functions

def getJob( jobClass = None ):
  if not jobClass:
    jobClass = Job
  oJob = jobClass()
  return oJob

def getDIRAC( diracClass = None ):
  print "in getDIRAC"
  if not diracClass:
    diracClass = Dirac
  oDirac = diracClass()
  print oDirac
  return oDirac


def baseToAllJobs( jName, jobClass = None ):

  print "**********************************************************************************************************"
  print "\n Submitting job ", jName

  J = getJob( jobClass )
  print J
  J.setName( jName )
  J.setCPUTime( 17800 )
  return J


def endOfAllJobs( J ):
  result = getDIRAC().submit( J )
  print "Job submission result:", result
  if result['OK']:
    jobID = int( result['Value'] )
    print "Submitted with job ID:", jobID

  return result

  print "**********************************************************************************************************"




# List of jobs

def helloWorld():

  J = baseToAllJobs( 'helloWorld' )
  print "J in helloWorld", J
  J.setInputSandbox( [find_all( 'exe-script.py', os.environ['DIRAC'], 'tests/Workflow' )[0]] )
  print "after setInputSandbox"
  J.setExecutable( "exe-script.py", "", "helloWorld.log" )
  return endOfAllJobs( J )

def mpJob():
  J = baseToAllJobs( 'mpJob' )
  J.setInputSandbox( [find_all( 'mpTest.py', os.environ['DIRAC'], 'tests/Utilities' )[0]] )
  J.setExecutable( 'testMpJob.sh' )
  J.setTag( 'MultiProcessor' )
  return endOfAllJobs( J )
