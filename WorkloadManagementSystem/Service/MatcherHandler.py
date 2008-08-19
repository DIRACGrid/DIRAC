########################################################################
# $Id: MatcherHandler.py,v 1.20 2008/08/19 06:01:50 rgracian Exp $
########################################################################
"""
Matcher class. It matches Agent Site capabilities to job requirements.
It also provides an XMLRPC interface to the Matcher

"""

__RCSID__ = "$Id: MatcherHandler.py,v 1.20 2008/08/19 06:01:50 rgracian Exp $"

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

  jobDB        = JobDB()
  jobLoggingDB = JobLoggingDB()
  taskQueueDB  = TaskQueueDB()

  gMonitor.registerActivity( 'matchTime', "Job matching time", 'Matching', "secs" ,gMonitor.OP_MEAN, 300 )
  gMonitor.registerActivity( 'matchTaskQueues', "Task queues checked per job", 'Matching', "task queues" ,gMonitor.OP_MEAN, 300 )
  return S_OK()

class MatcherHandler(RequestHandler):

##############################################################################
  def selectJob(self, resourceJDL):
    """ Main job selection function to find the highest priority job
        matching the resource capacity
    """

    startTime = time.time()
    classAdAgent = ClassAd(resourceJDL)
    if not classAdAgent.isOK():
      return S_ERROR('Illegal Resource JDL')
    gLogger.verbose(classAdAgent.asJDL())
    agentSite = classAdAgent.getAttributeString('Site')
    if not classAdAgent.lookupAttribute("Requirements"):
      classAdAgent.insertAttributeBool("Requirements", True)
      agentRequirements = classAdAgent.get_expression("Requirements")
    else:
      agentRequirements = classAdAgent.get_expression("Requirements")

    agent_jobID = 0
    if agentRequirements:
      fields = agentRequirements.split()
      for ind in range(len(fields)):
        if fields[ind].strip() == "other.JobID":
          agent_jobID = int(fields[ind+2].replace('"','').replace(';',''))

    if agent_jobID:
      # The Agent requires a particular job
      jobID = self.matchJob(classAdAgent,agent_jobID)
    else:
      # Get common site mask and check the agent site
      result = jobDB.getSiteMask(siteState='Active')
      if result['OK']:
        maskList = result['Value']
      else:
        return S_ERROR('Internal error: can not get site mask')

      siteIsBanned = 0
      if agentSite not in maskList:
        gLogger.info("Site [%s] is not allowed to take jobs" % agentSite)
        siteIsBanned = 1

      jobID = 0
      # The Agent can take any job, look through the task queue
      result = jobDB.getTaskQueues()
      if not result['OK']:
        return S_ERROR('Internal error: can not get the Task Queues')

      taskQueues = result['Value']
      taskQueuesLooked = 0
      for tqID, tqReqs, priority in taskQueues:
        if not tqID in gTaskQueues:
          # Keep everything on a local dict
          tqDict = {'OK':True}
          classAdQueue = ClassAd(tqReqs)
          if not classAdQueue.isOK():
            gLogger.error("Illegal requirements for Task Queue:",  "%s\n%s" % (tqID, tqReqs ))
            tqDict['OK'] = False
            gTaskQueues[tqID] = tqDict
            continue
          tqSite = ''
          iP1 = tqReqs.find( 'other.Site' )
          # Determine if TaskQueue is directed to a Single site
          if iP1 > -1:
            if not tqReqs.find( 'other.Site', iP1 + 1 ) > -1:
              # One single Site
              tqSite = re.sub( r'([\S\s]*)(other.Site\s*==\s*["\']*)([\w.-]*)(["\']*)([\S\s]*)', r'\3', tqReqs )
          tqDict['Requirements']  = tqReqs
          tqDict['ClassAd']       = classAdQueue
          tqDict['Site']          = tqSite
          gTaskQueues[tqID] = dict(tqDict)

        gLogger.verbose(tqReqs)
        taskQueuesLooked += 1

        if not gTaskQueues[tqID]['OK']:
          continue

        # Find the matching job now
        classAdQueue = gTaskQueues[tqID]['ClassAd']
        tqReqs       = gTaskQueues[tqID]['Requirements']
        tqSite       = gTaskQueues[tqID]['Site']

        if siteIsBanned:
          if agentSite != tqSite:
            continue

        result = matchClassAd(classAdAgent,classAdQueue)
        symmetricMatch, leftToRightMatch, rightToLeftMatch = result['Value']
        if not result['OK']:
          if leftToRightMatch is None:
            gLogger.verbose("Error while matching the Queue to Agent requirements")
            continue
        if leftToRightMatch:
          jobID = self.findMatchInQueue(classAdAgent, tqID)
          if jobID > 0:
            break
        else:
          gLogger.verbose('Task Queue and Request do not match')

      gMonitor.addMark( "matchTaskQueues", taskQueuesLooked )

    if jobID == 0:
      gLogger.verbose("No match found for site: %s" % agentSite)
      return S_ERROR("No match found for site: %s" % agentSite)

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
    if resAtt['OK']:
      if resAtt['Value']:
        resultDict['DN'] = resAtt['Value']['OwnerDN']
        resultDict['Group'] = resAtt['Value']['OwnerGroup']

    return S_OK(resultDict)

##############################################################################
  def matchJob(self,classAdAgent,agent_jobID):
    """ Verify that the jobID suggested by the agent actually
        matches the agent's capacity
    """

    jobID = 0
    gMutex.acquire()
    jobID = jobDB.lookUpJobInQueue(agent_jobID)
    if jobID:
      result = jobDB.getJobJDL(jobID,status='Waiting')
      if result['OK']:
        jobJDL = result['Value']
        classAdJob = ClassAd(jobJDL)
        result = matchClassAd(classAdJob,classAdAgent)
        if result['OK']:
          symmetricMatch, leftToRightMatch, rightToLeftMatch = result['Value']
          if symmetricMatch:
            gLogger.verbose('Found a double match, agent requested JobID: %d' % agent_jobID)
          else:
            jobID = 0
            gLogger.verbose('No match found for agent requested JobID: %d' % agent_jobID)
        else:
          jobID = 0
          gLogger.warn("Error while matching the Agent-Job requirements")
          gLogger.debug("Agent JDL:\n"+classAdAgent.asJDL())
          gLogger.debug("Job JDL:\n"+classAdJob.asJDL())
      else:
        jobID = 0
        gLogger.warn("Error while getting the jobJDL")
    else:
      gLogger.warn("Agent requested job %d not found in the Task Queue" % agent_jobID)

    if jobID > 0:
      taskQueueDB.deleteJob(jobID)
      result = jobDB.deleteJobFromQueue(jobID)

    gMutex.release()
    return jobID

##############################################################################
  def findMatchInQueue (self, classAdAgent, queueID):
    """  Find the highest priority job in the Task Queue with queueID
         matching the classAdAgent requirements
    """

    startTime = time.time()
    jobID = 0
    gMutex.acquire()
    result = jobDB.getJobsInQueue(queueID)
    if result['OK']:
      jobList = result['Value']
    else:
      gLogger.warn("Failed to get jobs from Task Queue %d" % queueID)
      gMutex.release()
      return 0

    # Clean up the Task Queue if it is empty
    # This is done just in case as this should never happen
    if not jobList:
      result = jobDB.deleteQueue(queueID)
      if not result['OK']:
        gLogger.warn('Failed to delete Task Queue %d' % queueID)
      gMutex.release()
      return 0

    if jobList:
      njobs = len(jobList)
      if njobs > 10:
        gLogger.verbose("JobID's for Task Queue %d:\n %s... total %d jobs" % (queueID,str(jobList[:10]),njobs))
      else:
        gLogger.verbose("JobID's for Task Queue %d:\n %s" % (queueID,jobList))

    for job in jobList:
      result = jobDB.getJobAttributes(job,['SystemPriority','Status'])
      if result['OK']:
        if result['Value']:
          status = result['Value']['Status']
          if status == "Waiting":
            resJDL = jobDB.getJobJDL(job)
            jobJDL = resJDL['Value']
            if not jobJDL:
              continue
            priority = result['Value']['SystemPriority']
            classAdJob = ClassAd(jobJDL)
            if not classAdJob.isOK():
              gLogger.warn('Illegal job JDL for job %d' % job)
              continue
            result = matchClassAd(classAdJob,classAdAgent)
            if result['OK']:
              symmetricMatch, leftToRightMatch, rightToLeftMatch = result['Value']
              if symmetricMatch:
                gLogger.verbose('Found a double match, JobID: %d' % job)
                jobID = job
                break
            else:
              gLogger.warn("Error while matching the Agent-Job requirements")
              gLogger.debug("Agent JDL:\n"+classAdAgent.asJDL())
              gLogger.debug("Job JDL:\n"+classAdJob.asJDL())
          else:
            gLogger.warn("Job %d in the Task Queue but the status is %s" % (job,status))
            if status != 'Staging':
              taskQueueDB.deleteJob(jobID)
              result = jobDB.deleteJobFromQueue(job)
              if not result['OK']:
                gLogger.warn("Failed to delete job %d from Task Queue" % job)
        else:
          gLogger.warn("Job %d ot found in the JobDB, will be deleted from the Task Queue" % job)
          taskQueueDB.deleteJob(jobID)
          result = jobDB.deleteJobFromQueue(job)
          if not result['OK']:
            gLogger.warn("Failed to delete job %d from Task Queue" % job)
      else:
        gLogger.warn("Error while getting the job attributes for %d" % job)

    if jobID > 0:
      taskQueueDB.deleteJob(jobID)
      result = jobDB.deleteJobFromQueue(job)
      if not result['OK']:
        gLogger.warn("Failed to delete job %d from Task Queue" % job)

    matchTime = time.time() - startTime
    gLogger.verbose("Job Match time: [%s]" % str(matchTime))
    gMutex.release()
    return jobID

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
