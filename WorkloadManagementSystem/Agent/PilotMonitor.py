########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/Agent/PilotMonitor.py,v 1.14 2008/08/04 17:27:33 atsareg Exp $
# File :   PilotMonitor.py
# Author : Stuart Paterson
########################################################################

"""  The Pilot Monitor Agent controls the tracking of pilots via the AgentMonitor and Grid
     specific sub-classes. This is a simple wrapper that performs the instantiation and monitoring
     of the AgentMonitor instance for all Grids.
"""

__RCSID__ = "$Id: PilotMonitor.py,v 1.14 2008/08/04 17:27:33 atsareg Exp $"

from DIRAC.Core.Base.Agent    import Agent
from DIRAC                    import S_OK, S_ERROR, gConfig, gLogger
from DIRAC.WorkloadManagementSystem.DB.PilotAgentsDB import PilotAgentsDB
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
import DIRAC.Core.Utilities.Time as Time

import os, time

AGENT_NAME = 'WorkloadManagement/PilotMonitor'

class PilotMonitor(Agent):

  #############################################################################
  def __init__(self):
    """ Standard constructor for Agent
    """
    Agent.__init__(self,AGENT_NAME)

  #############################################################################
  def initialize(self):
    """Sets defaults
    """
    result = Agent.initialize(self)
    self.pollingTime = gConfig.getValue(self.section+'/PollingTime',120)
    self.selectJobLimit = gConfig.getValue(self.section+'/JobSelectLimit',100)
    self.maxWaitingTime = gConfig.getValue(self.section+'/MaxJobWaitingTime',5*60)
    self.maxPilotAgents = gConfig.getValue(self.section+'/MaxPilotsPerJob',4)
    self.clearPilotsDelay = gConfig.getValue(self.section+'/ClearPilotsDelay',30)
    self.clearAbortedDelay = gConfig.getValue(self.section+'/ClearAbortedPilotsDelay',7)
    
    self.pilotDB = PilotAgentsDB()
    self.jobDB = JobDB()
    return result

  #############################################################################
  def execute(self):
    """The PilotMonitor execution method.
    """

    for minor in ['Pilot Agent Response', 'Director Submitting']:
    
      selection = {'Status':'Waiting','MinorStatus':minor}
      #delay  = time.localtime( time.time() - self.maxWaitingTime )
      #delay = time.strftime( "%Y-%m-%d %H:%M:%S", delay )
      delay = Time.toString(Time.dateTime() - self.maxWaitingTime*Time.second)
      result = self.jobDB.selectJobs(selection, older=delay, limit=self.selectJobLimit, orderAttribute='LastUpdateTime')
      if not result['OK']:
        return result
        
      jobList = result['Value']  
      for jobID in jobList:
      
        self.log.info( "Processing job", jobID )

        result = self.jobDB.lookUpJobInQueue(jobID)
        if not result:
          self.log.warn('Job Not in TaskQueue', jobID )
          continue

      
        result = self.pilotDB.getPilotsForJob(int(jobID))
        if not result['OK']:
          self.log.warn('Failed to get pilots for job %d' % int(jobID))
          # Assume no pilots were sent yet
          result = self.jobDB.setJobAttribute(jobID,"MinorStatus",
                                              "Pilot Agent Submission",
                                              update=True) 
          continue
          
        pilotList = result['Value']  
        result = self.pilotDB.getPilotInfo(pilotList)
        if not result['OK']:
          self.log.warn('Failed to get pilots info for job %d' % int(jobID))
          continue
          
        resultDict = result['Value']
        
        self.log.debug( "Pilot info", resultDict )
        
        aborted_pilots = []
        submitted_pilots = []
        for pRef,pilotDict in resultDict.items():
          if pilotDict['Status'] == "Aborted":
            aborted_pilots.append(pRef)
          if pilotDict['Status'] == "Submitted" or \
             pilotDict['Status'] == "Scheduled":
            submitted_pilots.append(pRef)
            
        if len(submitted_pilots) < self.maxPilotAgents:
          result = self.jobDB.setJobAttribute(jobID,"MinorStatus",
                                              "Pilot Agent Submission",
                                              update=True)
        else:
          result = self.jobDB.setJobAttribute(jobID,"MinorStatus",
                                               minor,
                                               update=True)
      
    result = self.pilotDB.clearPilots(self.clearPilotsDelay,self.clearAbortedDelay)
    if not result['OK']:
      self.log.warn('Failed to clear old pilots in the PilotAgentsDB')  
    
    return S_OK('Monitoring cycle complete.')
    
  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
