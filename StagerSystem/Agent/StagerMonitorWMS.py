########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/StagerSystem/Agent/Attic/StagerMonitorWMS.py,v 1.6 2008/04/30 09:51:41 paterson Exp $
# File :   StagerMonitorWMS.py
# Author : Stuart Paterson
########################################################################

"""  The StagerMonitorWMS Agent reports staging progress to the WMS.
     Updates are conveyed for the pending WorkloadManagement tasks. This
     uses the following StagerDB functions.

     getAllJobs(source)
     getJobsForSystemAndState(state,source,limit)
     setJobsDone(jobidsList)
     getLFNsForJob(singleJob)
     getStageTimeForSystem(lfnsList,source)

     Required file status changes are:
     ToUpdate -> Staged + report
     Failed -> purged with status change
     Successful -> purged with status change
"""

__RCSID__ = "$Id: StagerMonitorWMS.py,v 1.6 2008/04/30 09:51:41 paterson Exp $"

from DIRAC.Core.Base.Agent                                 import Agent
from DIRAC.Core.DISET.RPCClient                            import RPCClient
from DIRAC.StagerSystem.Client.StagerClient                import StagerClient
from DIRAC.Core.Utilities.GridCredentials                  import setupProxy,restoreProxy,setDIRACGroup, getProxyTimeLeft
from DIRAC                                                 import S_OK, S_ERROR, gConfig, gLogger

import os, sys, re, string, time

AGENT_NAME = 'Stager/StagerMonitorWMS'

class StagerMonitorWMS(Agent):

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
    self.pollingTime = gConfig.getValue(self.section+'/PollingTime',10)
    self.proxyLength = gConfig.getValue(self.section+'/DefaultProxyLength',24) # hours
    self.minProxyValidity = gConfig.getValue(self.section+'/MinimumProxyValidity',30*60) # seconds
    self.proxyLocation = gConfig.getValue(self.section+'/ProxyLocation','/opt/dirac/work/StagerMonitorWMS/shiftProdProxy')
    self.system = gConfig.getValue(self.section+'/SystemID','WorkloadManagement')
    self.stagingStatus = gConfig.getValue(self.section+'/StagingStatus','Staging')
    self.updateStatus = gConfig.getValue(self.section+'/UpdateStatus','ToUpdate')
    self.jobSelectLimit = gConfig.getValue(self.section+'/JobSelectLimit',500)
    self.monStatusDict = {self.updateStatus:'Staged','New':'Pending','Submitted':'Pending','Staged':'Staged','Successful':'Staged','Failed':'Failed'}
    self.wmsAdmin = RPCClient('WorkloadManagement/WMSAdministrator')
    self.jobReport = None #Initialized after proxy
    self.stagerClient = None #Initialized after proxy
    self.monitoring = None #Initialized after proxy
    return result

  #############################################################################
  def execute(self):
    """The StagerMonitorWMS execution method.
    """
    prodDN = gConfig.getValue('Operations/Production/ShiftManager','')
    if not prodDN:
      self.log.warn('Production shift manager DN not defined (/Operations/Production/ShiftManager)')
      return S_OK('Production shift manager DN is not defined')

    self.log.verbose('Checking proxy for %s' %(prodDN))
    result = self.__getProdProxy(prodDN)
    if not result['OK']:
      self.log.warn('Could not set up proxy for shift manager %s %s' %(prodDN))
      return S_OK('Production shift manager proxy could not be set up')

    self.monitoring = RPCClient('WorkloadManagement/JobMonitoring')
    self.jobReport  = RPCClient('WorkloadManagement/JobStateUpdate')
    self.stagerClient = StagerClient()

    self.log.verbose('Checking submitted jobs for status changes')
    result = self.__checkSubmittedJobs()
    if not result['OK']:
      self.log.warn('Problem checking submitted jobs:\n%s' %(result))

    self.log.verbose('Checking for staged files and report to WMS')
    result = self.__getStagedFiles()
    if not result['OK']:
      self.log.warn('Problem checking for staged files:\n%s' %(result))

    self.log.verbose('Checking for completed jobs and purging StagerDB')
    result = self.__purgeCompletedJobs()
    if not result['OK']:
      self.log.warn('Problem checking for completed jobs:\n%s' %(result))

    self.log.verbose('WMS Staging Monitoring cycle complete')
    return S_OK('Execution cycle complete')

  #############################################################################
  def __checkSubmittedJobs(self):
    """This method ensures that jobs in the staging state have not been deleted
       and purges any jobs that have been.
    """
    result = self.stagerClient.getAllJobs(self.system)
    if not result['OK']:
      return result

    if not result['JobIDs']:
      self.log.verbose('No %s jobs to check' %self.system)
      return S_OK('No jobs to check')

    self.log.info('%s %s job(s) submitted to Stager system' %(len(result['JobIDs']),self.system))
    statusDict = self.__getJobsStatus(result['JobIDs'])
    if not statusDict['OK']:
      return statusDict

    deletedJobs = []
    jobsDict = statusDict['Value']
    for jobID,valDict in jobsDict.items():
      for key,val in valDict.items():
        if key=='Status' and val!=self.stagingStatus:
          self.log.verbose('Job %s no longer in %s status' %(jobID,self.stagingStatus))
          deletedJobs.append(jobID)

    if not deletedJobs:
      return S_OK('All jobs checked')

    deletedJobs = [ str(x) for x in deletedJobs]
    result = self.stagerClient.setJobsDone(deletedJobs)
    if not result['OK']:
      self.log.warn('Failed to purge jobs from StagerDB with error:\n%s' %(result))
      return result

    return S_OK('Purged deleted jobs')

  #############################################################################
  def __getStagedFiles(self):
    """This method checks for files in the ToUpdate status and conveys updates
       to the job state service via job parameters.  The file status is then
       changed to Staged.
    """
    result = self.stagerClient.getJobsForSystemAndState(self.updateStatus,self.system,self.jobSelectLimit)
    if not result['OK']:
      self.log.warn('Failed to get jobs for %s status with error:\n%s' %(self.updateStatus,result))
      return result

    if not result['JobIDs']:
      self.log.verbose('No jobs available to update')
      return result

    totalJobs = len(result['JobIDs'])
    updatedJobs=[]
    self.log.info('%s %s job(s) found to update' %(totalJobs,self.system))
    for jobID in result['JobIDs']:
      update = self.__updateJobProgress(jobID,self.stagingStatus)
      if not update['OK']:
        self.log.warn('Failed to update %s monitoring for job %s with error:\n%s' %(self.system,jobID,update))
      else:
        updatedJobs.append(jobID)

    self.log.info('%s jobs successfully updated, %s failed to be updated' %(len(updatedJobs),(totalJobs-len(updatedJobs))))
    return S_OK('Updated jobs with staged files')

  #############################################################################
  def __updateJobProgress(self,jobID,primaryStatus,secondaryStatus=None):
    """Updates the WMS monitoring for a given jobID for which some files are newly staged.
    """
    #First get the job input data, site and status
    result = self.stagerClient.getJobFilesStatus(jobID)
    if not result['OK']:
      return result

    lfnsList = result['Files'].keys()
    totalFiles = len(lfnsList)
    lfnPfnStatusDict = result['Files']
    site = result['Site']
    retries = result['Retries']
    seDict = result['SE']

    #Get timing information for ToUpdate / Staged files
    result = self.stagerClient.getStageTimeForSystem(lfnsList,self.system)
    if not result['OK']:
      return result

    lfnTimingDict = result['TimingDict'][jobID] #{LFN:time}
    stagedCount = 0
    updateLFNs = []
    monitoringReport = [('SURL','Retries','Status','TimingInfo','SE')] #these become headers in the report
    for lfn,reps in lfnPfnStatusDict.items():
      for surl,status in reps.items():
        lfnTime = lfnTimingDict[lfn].split('.')[0]
        if re.search('-',lfnTime):
          lfnTime = '00:00:00'
        monitoringReport.append((surl,retries[lfn],self.monStatusDict[status],lfnTime,seDict[lfn])) #we don't need microsecond accuracy ;)
        if status==self.updateStatus or status=='Staged':
          stagedCount+=1
        if status==self.updateStatus:
          updateLFNs.append(lfn)

    #Send detailed report to the monitoring service
    minorStatus = '%s / %s' %(stagedCount,totalFiles)
    if secondaryStatus:
      minorStatus = secondaryStatus
    header = 'Report from DIRAC StagerSystem for %s on %s [UTC]:' %(site,time.asctime(time.gmtime()))
    result = self.__sendMonitoringReport(jobID,header,monitoringReport,primaryStatus,minorStatus)
    if not result['OK']:
      self.log.warn('Problem sending monitoring report for job %s:\n%s' %(jobID,result))
      return result

    #Finally update the ToUpdate file status to Staged in the StagerDB
    if updateLFNs:
      result = self.stagerClient.setFilesState(updateLFNs,site,'Staged')
      if not result['OK']:
        return result

    return S_OK('Job updated')

  #############################################################################
  def __sendMonitoringReport(self,jobID,header,monitoringReport,primaryStatus,secondaryStatus):
    """Constructs and sends a formatted report suitable for entering in the
       WMS monitoring as a job parameter.
    """
    #First format the header
    border = ''
    for i in xrange(len(header)):
      border+='='
    header = '\n%s\n%s\n%s\n' % (border,header,border)

    #Construct formatted report body from [()] monitoringReport dict
    body = ''
    surlAdj = 0
    retryAdj = 0
    statusAdj = 0
    timingAdj = 0
    seAdj = 0
    for surl,retry,status,timing,se in monitoringReport: #always same fields in the tuple
      if len(surl)+2>surlAdj:
        surlAdj = len(surl)+2
      if len(str(retry))+2>retryAdj:
        retryAdj = len(str(retry))+2
      if len(status)+2>statusAdj:
        statusAdj = len(status)+2
      if len(timing)+2>timingAdj:
        timingAdj = len(timing)+2
      if len(se)+2>seAdj:
        seAdj = len(se)+2

    for surl,retry,status,timing,se in monitoringReport:
      body += surl.ljust(surlAdj)+str(retry).ljust(retryAdj)+status.ljust(statusAdj)+timing.ljust(timingAdj)+se.ljust(seAdj)+'\n'

    #Update job status and send staging report
    stagerReport = '%s\n%s' %(header,body)
    result = self.__setJobParam(jobID,'StagerReport',stagerReport)
    if not result['OK']:
      return result

    result = self.__setJobStatus(jobID,primaryStatus,secondaryStatus)
    return result

  #############################################################################
  def __purgeCompletedJobs(self):
    """This method checks for jobs in the Successful/Failed status and conveys updates
       to the job state service and job monitoring.
    """
    toPurge = []
    statusList = [('Successful','Waiting','Pilot Agent Submission'),('Failed','Failed','Exceeded Max Staging Retry')]
    for state,status,minorStatus in statusList:
      result = self.stagerClient.getJobsForSystemAndState(state,self.system,self.jobSelectLimit)
      if not result['OK']:
        self.log.warn('Failed to get jobs for %s status with error:\n%s' %(result))
        return result
      if result['JobIDs']:
        self.log.verbose('%s %s jobs available to update' %(len(result['JobIDs']),state))
        for jobID in result['JobIDs']:
          result = self.__updateJobProgress(jobID,status,minorStatus)
          if not result['OK']:
            return result
          toPurge.append(jobID)
      else:
        self.log.verbose('No %s jobs available to update' %state)

    toPurge = [ str(x) for x in toPurge]
    if toPurge:
      result = self.stagerClient.setJobsDone(toPurge)
      if not result['OK']:
        self.log.warn('setJobsDone failed for jobs %s with result:\n%s' %(string.join(toPurge,', '),result))

    return S_OK('Jobs purged')

  #############################################################################
  def __getJobsStatus(self,jobList):
    """Wraps around getJobsStatus of monitoring client.
    """
    result = self.monitoring.getJobsStatus(jobList)
    if not result['OK']:
      self.log.warn('JobMonitoring client responded with error:\n%s' %result)

    return result

  #############################################################################
  def __setJobParam(self,jobID,name,value):
    """Wraps around setJobParameter of state update client
    """
    jobParam = self.jobReport.setJobParameter(int(jobID),str(name),str(value))
    self.log.verbose('setJobParameter(%s,%s,%s)' %(jobID,name,value))
    if not jobParam['OK']:
      self.log.warn(jobParam['Message'])

    return jobParam

  #############################################################################
  def __setJobStatus(self,jobID,status,minorStatus):
    """Wraps around setJobStatus of state update client
    """
    jobStatus = self.jobReport.setJobStatus(int(jobID),status,minorStatus,'StagerSystem')
    self.log.verbose('setJobStatus(%s,%s,%s,%s)' %(jobID,status,minorStatus,'StagerSystem'))
    if not jobStatus['OK']:
      self.log.warn(jobStatus['Message'])

    return jobStatus

  #############################################################################
  def __getProdProxy(self,prodDN):
    """This method sets up the proxy for immediate use if not available, and checks the existing
       proxy if this is available.
    """
    prodGroup = gConfig.getValue(self.section+'/ProductionGroup','lhcb_prod')
    self.log.info("Determining the length of proxy for DN %s" %prodDN)
    obtainProxy = False
    if not os.path.exists(self.proxyLocation):
      self.log.info("No proxy found")
      obtainProxy = True
    else:
      currentProxy = open(self.proxyLocation,'r')
      oldProxyStr = currentProxy.read()
      res = getProxyTimeLeft(oldProxyStr)
      if not res["OK"]:
        self.log.error("Could not determine the time left for proxy", res['Message'])
        res = S_OK(0) # force update of proxy

      proxyValidity = int(res['Value'])
      self.log.debug('Current proxy found to be valid for %s seconds' %proxyValidity)
      self.log.info('%s proxy found to be valid for %s seconds' %(prodDN,proxyValidity))
      if proxyValidity <= self.minProxyValidity:
        obtainProxy = True

    if obtainProxy:
      self.log.info('Attempting to renew %s proxy' %prodDN)
      res = self.wmsAdmin.getProxy(prodDN,prodGroup,self.proxyLength)
      if not res['OK']:
        self.log.error('Could not retrieve proxy from WMS Administrator', res['Message'])
        return S_OK()
      proxyStr = res['Value']
      if not os.path.exists(os.path.dirname(self.proxyLocation)):
        os.makedirs(os.path.dirname(self.proxyLocation))
      res = setupProxy(proxyStr,self.proxyLocation)
      if not res['OK']:
        self.log.error('Could not create environment for proxy.', res['Message'])
        return S_OK()

      setDIRACGroup(prodGroup)
      self.log.info('Successfully renewed %s proxy' %prodDN)

    #os.system('voms-proxy-info -all')
    return S_OK('Active proxy available')

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
