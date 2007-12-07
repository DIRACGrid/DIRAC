########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/Agent/JobAgent.py,v 1.5 2007/12/07 12:32:31 paterson Exp $
# File :   JobAgent.py
# Author : Stuart Paterson
########################################################################

"""  The Job Agent class instantiates a CE instance that acts as a client to a
     compute resource and also to the WMS.  The Job Agent constructs a classAd
     based on the local resource description in the CS and the current resource
     status that is used for matching.
"""

__RCSID__ = "$Id: JobAgent.py,v 1.5 2007/12/07 12:32:31 paterson Exp $"

from DIRAC.Core.Utilities.ClassAd.ClassAdLight           import ClassAd
from DIRAC.Core.Base.Agent                               import Agent
from DIRAC.Core.Utilities.Subprocess                     import shellCall
from DIRAC.Core.DISET.RPCClient                          import RPCClient
from DIRAC.Resources.Computing.ComputingElementFactory   import ComputingElementFactory
from DIRAC.Resources.Computing.ComputingElement          import ComputingElement
from DIRAC                                               import S_OK, S_ERROR, gConfig

import os, sys, re, string, time

AGENT_NAME = 'WorkloadManagement/JobAgent'

class JobAgent(Agent):

  #############################################################################
  def __init__(self):
    """ Standard constructor for Agent
    """
    Agent.__init__(self,AGENT_NAME)
    #self.log.setLevel('verb')
    self.jobManager  = RPCClient('WorkloadManagement/JobManager')
    self.matcher = RPCClient('WorkloadManagement/Matcher')
    self.jobReport  = RPCClient('WorkloadManagement/JobStateUpdate')

  #############################################################################
  def initialize(self,loops=0):
    """Sets default parameters and creates CE instance
    """
   # self.log.setLevel('debug') #temporary for debugging
    self.maxcount = loops
    result = Agent.initialize(self)
    ceUniqueID = gConfig.getOption(self.section+'/CEUniqueID','InProcess')
    if not ceUniqueID['OK']:
      self.log.warn(ceUniqueID['Message'])
      return ceUniqueID
    ceFactory = ComputingElementFactory(ceUniqueID['Value'])
    self.ceName = ceUniqueID['Value']
    ceInstance = ceFactory.getCE()
    if not ceInstance['OK']:
      self.log.warn(ceInstance['Message'])
      return ceInstance

    self.computingElement = ceInstance['Value']
    self.siteRoot = gConfig.getValue('LocalSite/Root','/Users/stuart/dirac/workspace/DIRAC3')
    self.jobWrapperTemplate = self.siteRoot+gConfig.getValue(self.section+'/JobWrapperTemplate','/DIRAC/WorkloadManagementSystem/JobWrapper/JobWrapperTemplate')
    self.jobSubmissionDelay = gConfig.getValue(self.section+'/SubmissionDelay',10)
    return result

  #############################################################################
  def execute(self):
    """The JobAgent execution method.
    """
    self.log.debug('Job Agent execution loop')
    available = self.computingElement.available()
    if not available['OK']:
      self.log.info('Resource is not available')
      self.log.info(available['Message'])
      return available

    self.log.info(available['Value'])

    ceJDL = self.computingElement.getJDL()
    resourceJDL = ceJDL['Value']
    self.log.debug(resourceJDL)
    start = time.time()
    jobRequest = self.__requestJob(resourceJDL)
    matchTime = time.time() - start
    self.log.info('MatcherTime = %.2f (s)' %(matchTime))

    if not jobRequest['OK']:
      if re.search('No work available',jobRequest['Message']):
        self.log.info('Job request OK: %s' %(jobRequest['Message']))
        return S_OK(jobRequest['Message'])
      else:
        self.log.info('Failed to get jobs: %s'  %(jobRequest['Message']))
        return S_ERROR(jobRequest['Message'])

    jobJDL = jobRequest['Value']
    if not jobJDL:
      msg = 'Matcher service returned S_OK with null JDL'
      self.log.error(msg)
      return S_ERROR(msg)

    parameters = self.__getJDLParameters(jobJDL)
    if not parameters['OK']:
      return parameters

    params = parameters['Value']
    jobID = params['JobID']
    jobType = params['JobType']
    systemConfig = params['SystemConfig']
    self.log.verbose('Job request successful: \n %s' %(jobRequest['Value']))
    self.log.info('Received JobID=%s, JobType=%s, SystemConfig=%s' %(jobID,jobType,systemConfig))

    try:
      self.__setJobParam(jobID,'MatcherServiceTime',str(matchTime))
      self.__report(jobID,'Matched','Job Received by Agent')

      saveJDL = self.__saveJobJDLRequest(jobID,jobJDL)
      if not saveJDL['OK']:
        result = self.jobManager.rescheduleJob(int(jobID))
        if not result['OK']:
          self.log.error(result['Message'])
        else:
          self.log.info('Rescheduled job %s' %(jobID))
        return saveJDL

      software = self.__checkInstallSoftware(jobID,params)
      if not software['OK']:
        self.log.error('Failed to install software for job %s' %(jobID))
        self.log.error(software['Message'])
        result = self.jobManager.rescheduleJob(jobID)
        if not result['OK']:
          self.log.error(result['Message'])
          return result
        else:
          self.log.info('Rescheduled job after software installation failure %s' %(jobID))

      self.__report(jobID,'Matched','Job Prepared to Submit')
      resourceParameters = self.__getJDLParameters(resourceJDL)
      if not resourceParameters['OK']:
        return resourceParameters
      resourceParams = resourceParameters['Value']
      self.log.debug('Before %sCE submitJob()' %(self.ceName))
      submission = self.__submitJob(jobID,params,resourceParams,jobJDL)
      self.log.debug('After %sCE submitJob()' %(self.ceName))
    except Exception, x:
      self.log.exception(x)
      result = self.jobManager.rescheduleJob(jobID)
      if not result['OK']:
        self.log.error(result['Message'])
      else:
        self.log.info('Rescheduled job %s' %(jobID))

      return S_ERROR('Job processing failed with exception')

    return S_OK('Job Agent cycle complete')

  #############################################################################
  def __checkInstallSoftware(self,jobID,parameters):
    """Checks software requirement of job and whether this is already present
       before installing software locally.
    """
    self.__report(jobID,'Matched','Installing Software')
    # to implement
    return S_OK()

  #############################################################################
  def __submitJob(self,jobID,jobParams,resourceParams,jobJDL):
    """Submit job to the Computing Element instance after creating a custom
       Job Wrapper with the available job parameters.
    """
    result = self.__createJobWrapper(jobID,jobParams,resourceParams)
    wrapperFile = result['Value']
    self.__report(jobID,'Matched','Queued')

    wrapperName = os.path.basename(wrapperFile)
    self.log.info('Submitting %s to %sCE' %(wrapperName,self.ceName))

    batchID = 'dc%s' %(jobID)
    submission = self.computingElement.submitJob(wrapperFile,jobJDL,batchID)

    if submission['OK']:
      batchID = submission['Value']
      self.log.info('Job %s submitted as %s' %(jobID,batchID))
      self.log.debug('Set JobParameter: Local batch ID %s' %(batchID))
      self.__setJobParam(jobID,'LocalBatchID',str(batchID))
      time.sleep(self.jobSubmissionDelay)
    else:
      self.log.warn('Job '+str(jobID)+' submission failed')
      self.__setJobParam(jobID,'ErrorMessage','%s CE Submission Error' %(self.ceName))
      self.__report(jobID,'Failed','%s CE Submission Error' %(self.ceName))

    return S_OK('Job submitted')

  #############################################################################
  def __createJobWrapper(self,jobID,jobParams,resourceParams):
    """This method creates a job wrapper filled with the CE and Job parameters
       to executed the job.
    """
    arguments = {'Job':jobParams,'CE':resourceParams}
    self.log.debug('Job arguments are: \n %s' %(arguments))

    if not os.path.exists(self.siteRoot+'/job/Wrapper'):
      os.makedirs(self.siteRoot+'/job/Wrapper')

    jobWrapperFile = self.siteRoot+'/job/Wrapper/Wrapper_%s' %(jobID)
    if os.path.exists(jobWrapperFile):
      self.log.debug('Removing existing Job Wrapper for %s' %(jobID))
      os.remove(jobWrapperFile)
    wrapperTemplate = open(self.jobWrapperTemplate,'r').read()
    wrapper = open (jobWrapperFile,"w")
    dateStr = time.strftime("%Y-%m-%d",time.localtime(time.time()))
    timeStr = time.strftime("%H:%M",time.localtime(time.time()))
    date_time = '%s %s' %(dateStr,timeStr)
    signature = __RCSID__
    dPython = sys.executable
    print >> wrapper, wrapperTemplate % (dPython,signature,jobID,date_time)
    wrapper.write('sys.path.insert(0,"%s")\n' %(self.siteRoot))
    jobArgs = "execute("+str(arguments)+")\n"
    wrapper.write(jobArgs)
    wrapper.close ()
    os.chmod(jobWrapperFile,0755)
    return S_OK(jobWrapperFile)

  #############################################################################
  def __saveJobJDLRequest(self,jobID,jobJDL):
    """Save job JDL local to JobAgent.
    """
    classAdJob = ClassAd('['+jobJDL+']')
    classAdJob.insertAttributeString('LocalCE',self.ceName)
    jdlFileName = jobID+'.jdl'
    jdlFile = open(jdlFileName,'w')
    jdl = classAdJob.asJDL()
    jdlFile.write(jdl)
    jdlFile.close()
    return S_OK(jdlFileName)

  #############################################################################
  def __requestJob(self,resourceJDL):
    """Request a single job from the matcher service.
    """
    try:
      result = self.matcher.requestJob(resourceJDL)
      return result
    except Exception, x:
      self.log.exception(x)
      return S_ERROR('Job request to matcher service failed with exception')

  #############################################################################
  def __getJDLParameters(self,jdl):
    """Returns a dictionary of JDL parameters.
    """
    try:
      parameters = {}
#      print jdl
      if not re.search('\[',jdl):
        jdl = '['+jdl+']'
      classAdJob = ClassAd(jdl)
      paramsDict = classAdJob.contents
      for param,value in paramsDict.items():
        if re.search('{',value):
          self.log.debug('Found list type parameter %s' %(param))
          rawValues = value.replace('{','').replace('}','').replace('"','').replace('LFN:','').split()
          valueList = []
          for val in rawValues:
            if re.search(',$',val):
              valueList.append(val[:-1])
            else:
              valueList.append(val)
          parameters[param] = valueList
        else:
          self.log.debug('Found standard parameter %s' %(param))
          parameters[param]= value.replace('"','')
      return S_OK(parameters)
    except Exception, x:
      self.log.exception(x)
      return S_ERROR('Exception while extracting JDL parameters for job')

  #############################################################################
  def __report(self,jobID,status,minorStatus):
    """Wraps around setJobStatus of state update client
    """
    jobStatus = self.jobReport.setJobStatus(int(jobID),status,minorStatus,'JobAgent')
    self.log.debug('setJobStatus(%s,%s,%s,%s)' %(jobID,status,minorStatus,'JobAgent'))
    if not jobStatus['OK']:
        self.log.warn(jobStatus['Message'])

    return jobStatus

  #############################################################################
  def __setJobParam(self,jobID,name,value):
    """Wraps around setJobParameter of state update client
    """
    jobParam = self.jobReport.setJobParameter(int(jobID),str(name),str(value))
    self.log.debug('setJobParameter(%s,%s,%s)' %(jobID,name,value))
    if not jobParam['OK']:
        self.log.warn(jobParam['Message'])

    return jobParam

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#