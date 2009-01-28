########################################################################
# $Id: MatcherHandler.py,v 1.33 2009/01/28 12:03:59 acasajus Exp $
########################################################################
"""
Matcher class. It matches Agent Site capabilities to job requirements.
It also provides an XMLRPC interface to the Matcher

"""

__RCSID__ = "$Id: MatcherHandler.py,v 1.33 2009/01/28 12:03:59 acasajus Exp $"

import re, os, sys, time
import string
import signal, fcntl, socket
import getopt
from   types import *
import threading

from DIRAC.Core.DISET.RequestHandler                   import RequestHandler
from DIRAC.Core.Utilities.ClassAd.ClassAdCondor        import ClassAd, matchClassAd
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
  taskQueueDB.recalculateTQSharesForAll()
  gThreadScheduler.addPeriodicTask( 120, taskQueueDB.recalculateTQSharesForAll )

  gMonitor.registerActivity( 'matchTime', "Job matching time", 'Matching', "secs" ,gMonitor.OP_MEAN, 300 )
  gMonitor.registerActivity( 'matchTaskQueues', "Task queues checked per job", 'Matching', "task queues" ,gMonitor.OP_MEAN, 300 )
  return S_OK()

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
        return S_ERROR('Site in mask and GridCE not specified')

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
    return result

##############################################################################
  types_checkForJobs = [StringType]
  def export_checkForJobs(self, resourceJDL):
    """ Check if jobs eligible for the given resource capacity are available
        and with which priority
    """

    agentClassAd = ClassAd(resourceJDL)
    result = jobDB.getTaskQueues()
    if not result['OK']:
      return S_ERROR('Internal error: can not get the Task Queues')

    taskQueues = result['Value']
    matching_queues = []
    matching_priority = 0
    for tqID, tqReqs, priority in taskQueues:
      queueClassAd = ClassAd(tqReqs)
      result = matchClassAd(classAdAgent,queueClassAd)
      if result['OK']:
        symmetricMatch, leftToRightMatch, rightToLeftMatch = result['Value']
        if leftToRightMatch:
          matching_queues.append(tqID)
          if priority > matching_priority:
            matching_priority = priority

    if matching_queues:
      result = jobDB.getTaskQueueReport(matching_queues)
      if result['OK']:
        return result
      else:
        gLogger.warn('Failed to extract the Task Queue report')
        return S_ERROR('Failed to extract the Task Queue report')
    else:
      return S_OK([])
