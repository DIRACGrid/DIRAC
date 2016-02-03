""" Collection of user jobs for testing purposes
"""

from TestDIRAC.Utilities.utils import find_all
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
  if not diracClass:
    diracClass = Dirac
  oDirac = diracClass()
  return oDirac


def baseToAllJobs( jName, jobClass = None ):

  print "**********************************************************************************************************"
  print "\n Submitting job ", jName

  J = getJob( jobClass )
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
  J.setInputSandbox( [find_all( 'exe-script.py', '.', 'GridTestSubmission' )[0]] )
  J.setExecutable( "exe-script.py", "", "helloWorld.log" )
  return endOfAllJobs( J )

def mpJob():
  J = baseToAllJobs( 'mpJob' )
  J.setInputSandbox( 'mpTest.py' )
  J.setExecutable( 'testMpJob.sh' )
  J.setTag( 'MultiProcessor' )
  return endOfAllJobs( J )
