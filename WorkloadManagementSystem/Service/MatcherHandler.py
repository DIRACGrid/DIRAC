########################################################################
# $Id: MatcherHandler.py,v 1.1 2007/11/23 15:57:43 atsareg Exp $
########################################################################
"""
Matcher class. It matches Agent Site capabilities to job requirements.
It also provides an XMLRPC interface to the Matcher

"""

__RCSID__ = "$Id: MatcherHandler.py,v 1.1 2007/11/23 15:57:43 atsareg Exp $"

import re, os, sys, time
import string
import signal, fcntl, socket
import getopt
from   types import *
import threading

from DIRAC.Core.Utilities.ClassAd.ClassAdCondor import ClassAd, matchClassAd
from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
from DIRAC.WorkloadManagementSystem.DB.JobLoggingDB import JobLoggingDB

gMutex = threading.Semaphore()
jobDB = False
jobLoggingDB = False

def initializeMatcherHandler( serviceInfo ):
  """  Matcher Service initialization
  """

  global jobDB
  global jobLoggingDB

  jobDB = JobDB()
  jobLoggingDB = jobLoggingDB
  return S_OK()

class MatcherHandler:

  def __init__(self):
    """ Matcher service per call initialization
    """
    pass

##############################################################################
  def selectJob(self, resourceJDL):
    """ Main job selection function to find the highest priority job
        matching the resource capacity
    """

    startTime = time.time()
    result = -1
    classAdAgent = ClassAd("["+resourceJDL+"]")
    gLogger.verbose(classAdAgent.asJDL())
    agentSite = classAdAgent.getAttributeString('Site')
    agentRequirements = classAdAgent.get_expression("Requirements")
    if not agentRequirements:
      classAdAgent.insertAttributeBool("Requirements", 'True')

    fields = agentRequirements.split()
    agent_jobID = 0
    for ind in range(len(fields)):
      if fields[ind].strip() == "other.JobID":
        agent_jobID = int(fields[ind+2])

    if agent_jobID:
      # The Agent requires a particular job
      jobID = self.matchJob(ClassAdAgent,agent_jobID)
    else:
      # Get common site mask and check the agent site
      result = self.getMask()
      if result['OK']:
        maskList = result['Value']
      else:
        return S_ERROR('Internal error: can not get site mask')

      siteIsBanned = 0
      if agentSite not in maskList:
        gLogger.info("Site [%s] is not allowed to take jobs" % agentSite)
        result = -2
        siteIsBanned = 1

      jobID = 0
      # The Agent can take any job, look through the task queue
      result = jobDB.getTaskQueues()
      if not result['OK']:
        return S_ERROR('Internal error: can not get the Task Queues')

      taskQueues = result['Value']
      for tqID, tqReqs, priority in taskQueues:
        gLogger.verbose(tqReqs)

        if siteIsBanned:
          # Check if the task queue is devoted to one site
          siteInTheMask = ''
          ind1 = tqReqs.find('other.Site ==')
          if ind1 != -1:
            ind2 = tqReqs[ind1+13:].find('other.Site ==')
            if ind2 == -1:
              # There is only one site in the requirements, get it
              siteInTheMask = tqReqs[ind1+13:],split()[0]

          if siteInTheMask and siteInTheMask == agentSite:
            # We can continue
            pass
          else:
            return S_ERROR('Site is banned and no dedicated jobs available')

        # Find the matching job now
        classAdQueue = ClassAd(tqReqs)
        result = matchClassAd(classAdAgent,classAdQueue)
        if result['OK']:
          symmetricMatch, leftToRightMatch, rightToLeftMatch = result['Value']
          if rightToLeftMatch:
            jobID = self.findMatchInQueue(classAdAgent, tqID)
            if jobID > 0:
              break

    if jobID > 0:
      result = jobDB.setJobStatus(jobID,status='Matched',minor='Assigned')
      result = jobLoggingDB.addLoggingRecord(self,jobID,
                                             status='Matched',
                                             minor='Assigned',
                                             source='Matcher')
      jdl = jobDB.getJobJDL(jobID)
      result = S_OK(jdl)
    else:
      gLogger.verbose("No match found for site: [%s]" % siteAgent)

    matchTime = time.time() - startTime
    gLogger.verbose("Match time: [%s]" % str(matchTime))
    return result

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
      result = jobDB.deleteJobFromQueue(jobID)

    gMmutex.release()
    return jobID

##############################################################################
  def findMatchInQueue (self, classAdAgent, queueID):
    """  Find the highest priority job in the Task Queue with queueID
         matching the classAdAgent requirements
    """

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
      result = self.jobDB.deleteQueue(queueID)
      if not result['OK']:
        gLogger.warn('Failed to delete Task Queue %d' % queueID)
      gMutex.release()
      return 0

    if jobList:
      njobs = len(jobList)
      if njobs > 10:
        gLogger.verbose("JobID's for Task Queue %d:\n %s... total %d jobs" % (queueID,str(jobids[:10]),njobs))
      else:
        gLogger.verbose("JobID's for Task Queue %d:\n %s" % (queueId,jobids))

    for job in jobList:
      result = jobDB.getJobAttributes(job,['JDL','Priority','Status'])
      if result['OK']:
        if result['Value']:
          jobJDL = result['Value'][0]
          priority = result['Value'][1]
          status = result['Value'][2]
          if status == "Waiting":
            classAdJob = ClassAd(jobJDL)
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
        else:
          gLogger.warn("Job %d ot found in the JobDB, will be deleted from the Task Queue" % job)
          result = jobDB.deleteJobFromQueue(job)
          if not result['OK']:
            gLogger.warn("Failed to delete job %d from Task Queue" % job)
      else:
        gLogger.warn("Error while getting the job attributes for %d" % job)

      if jobID > 0:
        result = jobDB.deleteJobFromQueue(job)
        if not result['OK']:
          gLogger.warn("Failed to delete job %d from Task Queue" % job)

      gMutex.release()
      return jobID

##############################################################################
  types_requestJob = [StringType]
  def export_requestJob(self, resourceJDL ):
    """ Serve a job to the request of an agent which is the highest priority
        one matching the agent's site capacity
    """

    #print "requestJob: ",resourceJDL

    result = self.selectJob(ResourceJDL)
    if result == -1:
      return S_ERROR('No work avalaible')
    elif result == -2:
      return S_ERROR('Site not allowed to take jobs and no assigned jobs are available')

    return S_OK(result)

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
