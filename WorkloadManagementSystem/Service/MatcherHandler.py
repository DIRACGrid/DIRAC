########################################################################
# $Id$
########################################################################
"""
Matcher class. It matches Agent Site capabilities to job requirements.
It also provides an XMLRPC interface to the Matcher

"""

__RCSID__ = "$Id$"

import re, os, sys, time
import string
import signal, fcntl, socket
import getopt
from   types import *
import threading

from DIRAC.Core.DISET.RequestHandler                   import RequestHandler
from DIRAC.Core.Utilities.ClassAd.ClassAdLight         import ClassAd
from DIRAC                                             import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.WorkloadManagementSystem.DB.JobDB           import JobDB
from DIRAC.WorkloadManagementSystem.DB.JobLoggingDB    import JobLoggingDB
from DIRAC.WorkloadManagementSystem.DB.TaskQueueDB     import TaskQueueDB
from DIRAC                                             import gMonitor
from DIRAC.Core.Utilities.ThreadScheduler              import gThreadScheduler

gMutex = threading.Semaphore()
gTaskQueues = {}
jobDB = False
jobLoggingDB = False
taskQueueDB = False

def initializeMatcherHandler( serviceInfo ):
  """  Matcher Service initialization
  """

  global jobDB
  global jobLoggingDB
  global taskQueueDB

  jobDB        = JobDB()
  jobLoggingDB = JobLoggingDB()
  taskQueueDB  = TaskQueueDB()
  
  gMonitor.registerActivity( 'matchTime', "Job matching time", 'Matching', "secs" ,gMonitor.OP_MEAN, 300 )
  gMonitor.registerActivity( 'matchTaskQueues', "Task queues checked per job", 'Matching', "task queues" ,gMonitor.OP_MEAN, 300 )
  gMonitor.registerActivity( 'matchesDone', "Job Matches", 'Matching', "matches" ,gMonitor.OP_MEAN, 300 )
  gMonitor.registerActivity( 'numTQs', "Number of Task Queues", 'Matching', "tqsk queues" ,gMonitor.OP_MEAN, 300 )
  
  taskQueueDB.recalculateTQSharesForAll()
  gThreadScheduler.addPeriodicTask( 120, taskQueueDB.recalculateTQSharesForAll )
  gThreadScheduler.addPeriodicTask( 120, sendNumTaskQueues )
  
  sendNumTaskQueues()

  return S_OK()

def sendNumTaskQueues():
  result = taskQueueDB.getNumTaskQueues()
  if result[ 'OK' ]:
    gMonitor.addMark( 'numTQs', result[ 'Value' ] )
  else:
    gLogger.error( "Cannot get the number of task queues", result[ 'Message' ] )

class MatcherHandler(RequestHandler):

  def selectJob(self, resourceJDL):
    """ Main job selection function to find the highest priority job
        matching the resource capacity
    """

    startTime = time.time()
    classAdAgent = ClassAd(resourceJDL)
    if not classAdAgent.isOK():
      return S_ERROR('Illegal Resource JDL')
    gLogger.verbose(classAdAgent.asJDL())

    resourceDict = {}
    for name in taskQueueDB.getSingleValueTQDefFields():
      if classAdAgent.lookupAttribute(name):
        if name == 'CPUTime':
          resourceDict[name] = classAdAgent.getAttributeInt(name)
        else:
          resourceDict[name] = classAdAgent.getAttributeString(name)

    for name in taskQueueDB.getMultiValueMatchFields():
      if classAdAgent.lookupAttribute(name):
        resourceDict[name] = classAdAgent.getAttributeString(name)

    # Check if a JobID is requested
    if classAdAgent.lookupAttribute('JobID'):
      resourceDict['JobID'] = classAdAgent.getAttributeInt('JobID')

    # Get common site mask and check the agent site
    result = jobDB.getSiteMask(siteState='Active')
    if result['OK']:
      maskList = result['Value']
    else:
      return S_ERROR('Internal error: can not get site mask')

    if not 'Site' in resourceDict:
      return S_ERROR('Missing Site Name in Resource JDL')

    if resourceDict['Site'] not in maskList:
      if 'GridCE' in resourceDict:
        del resourceDict['Site']
      else:
        return S_ERROR('Site not in mask and GridCE not specified')

    resourceDict['Setup'] = self.serviceInfoDict['clientSetup']

    print resourceDict

    result = taskQueueDB.matchAndGetJob( resourceDict )

    print result

    if not result['OK']:
      return result
    result = result['Value']
    if not result['matchFound']:
      return S_ERROR( 'No match found' )

    jobID = result['jobId']

    result = jobDB.setJobStatus(jobID,status='Matched',minor='Assigned')
    result = jobLoggingDB.addLoggingRecord(jobID,
                                           status='Matched',
                                           minor='Assigned',
                                           source='Matcher')

    result = jobDB.getJobJDL(jobID)
    if not result['OK']:
      return S_ERROR('Failed to get the job JDL')

    resultDict = {}
    resultDict['JDL'] = result['Value']

    matchTime = time.time() - startTime
    gLogger.info("Match time: [%s]" % str(matchTime))
    gMonitor.addMark( "matchTime", matchTime )

    # Get some extra stuff into the response returned
    resOpt = jobDB.getJobOptParameters(jobID)
    if resOpt['OK']:
      for key,value in resOpt['Value'].items():
        resultDict[key] = value
    resAtt = jobDB.getJobAttributes(jobID,['OwnerDN','OwnerGroup'])
    if not resAtt['OK']:
      return S_ERROR('Could not retrieve job attributes')
    if not resAtt['Value']:
      return S_ERROR('No attributes returned for job')

    resultDict['DN'] = resAtt['Value']['OwnerDN']
    resultDict['Group'] = resAtt['Value']['OwnerGroup']
    return S_OK(resultDict)

##############################################################################
  types_requestJob = [StringType]
  def export_requestJob(self, resourceJDL ):
    """ Serve a job to the request of an agent which is the highest priority
        one matching the agent's site capacity
    """

    #print "requestJob: ",resourceJDL

    result = self.selectJob(resourceJDL)
    gMonitor.addMark( "matchesDone" )
    return result

##############################################################################
  types_getActiveTaskQueues = []
  def export_getActiveTaskQueues( self ):
    """ Return all task queues
    """
    return taskQueueDB.retrieveTaskQueues()

##############################################################################
  types_getMatchingTaskQueues = [ DictType ]
  def export_getMatchingTaskQueues( self, resourceDict ):
    """ Return all task queues
    """
    return taskQueueDB.retrieveTaskQueuesThatMatch( resourceDict )
