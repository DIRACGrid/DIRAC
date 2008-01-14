########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/Agent/JobAgent.py,v 1.15 2008/01/14 14:39:19 paterson Exp $
# File :   JobAgent.py
# Author : Stuart Paterson
########################################################################

"""  The Job Agent class instantiates a CE instance that acts as a client to a
     compute resource and also to the WMS.  The Job Agent constructs a classAd
     based on the local resource description in the CS and the current resource
     status that is used for matching.
"""

__RCSID__ = "$Id: JobAgent.py,v 1.15 2008/01/14 14:39:19 paterson Exp $"

from DIRAC.Core.Utilities.ModuleFactory                  import ModuleFactory
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
    self.siteRoot = gConfig.getValue('LocalSite/Root',os.getcwd())
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
    if not params.has_key('JobID'):
      msg = 'Job has not JobID defined in JDL parameters'
      self.log.warn(msg)
      return S_ERROR(msg)
    else:
      jobID = params['JobID']

    if not params.has_key('JobType'):
      self.log.warn('Job has no JobType defined in JDL parameters')
      jobType = 'Unknown'
    else:
      jobType = params['JobType']

    if not params.has_key('SystemConfig'):
      self.log.warn('Job has no system configuration defined in JDL parameters')
      systemConfig = 'ANY'
    else:
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

      self.__report(jobID,'Matched','Job Prepared to Submit')
      resourceParameters = self.__getJDLParameters(resourceJDL)
      if not resourceParameters['OK']:
        return resourceParameters
      resourceParams = resourceParameters['Value']

      software = self.__checkInstallSoftware(jobID,params,resourceParams)
      if not software['OK']:
        self.log.error('Failed to install software for job %s' %(jobID))
        self.log.error(software['Message'])
        result = self.jobManager.rescheduleJob(jobID)
        if not result['OK']:
          self.log.error(result['Message'])
          return result
        else:
          self.log.info('Rescheduled job after software installation failure %s' %(jobID))

      self.log.debug('Before %sCE submitJob()' %(self.ceName))
      submission = self.__submitJob(jobID,params,resourceParams,jobJDL)
      if not submission['OK']:
        self.log.warn('Job submission failed during creation of the Job Wrapper')
        return submission

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
  def __checkInstallSoftware(self,jobID,jobParams,resourceParams):
    """Checks software requirement of job and whether this is already present
       before installing software locally.
    """

    if not jobParams.has_key('SoftwareDistModule'):
      msg = 'Job has no software installation requirement'
      self.log.verbose(msg)
      return S_OK(msg)

    self.__report(jobID,'Matched','Installing Software')
    softwareDist = jobParams['SoftwareDistModule']
    self.log.verbose('Found VO Software Distribution module: %s' %(softwareDist))
    argumentsDict = {'Job':jobParams,'CE':resourceParams}
    moduleFactory = ModuleFactory()
    moduleInstance = moduleFactory.getModule(softwareDist,argumentsDict)
    if not moduleInstance['OK']:
      return moduleInstance

    module = moduleInstance['Value']
    result = module.execute()
    return result

  #############################################################################
  def __submitJob(self,jobID,jobParams,resourceParams,jobJDL):
    """Submit job to the Computing Element instance after creating a custom
       Job Wrapper with the available job parameters.
    """
    result = self.__createJobWrapper(jobID,jobParams,resourceParams)

    if not result['OK']:
      return result

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

    systemConfig = ''
    if jobParams.has_key('SystemConfig'):
      systemConfig = jobParams['SystemConfig']
      self.log.verbose('Job system configuration requirement is %s' %(systemConfig))
      if resourceParams.has_key('Root'):
        jobPython = '%s/%s/bin/python' %(resourceParams['Root'],systemConfig)
        if os.path.exists(jobPython):
          self.log.verbose('Found local python for job:\n%s' %(jobPython))
          dPython = jobPython
        else:
          if systemConfig == 'ANY':
            self.log.verbose('Using standard available python %s for job' %(dPython))
          else:
            self.log.warn('Job requested python \n%s\n but this is not available locally' %(jobPython))
      else:
        self.log.warn('Job requested python \n%s\n but no LocalSite/Root defined' %(jobPython))
    else:
      self.log.warn('Job has no system configuration requirement')

    if not systemConfig or systemConfig=='ANY':
      systemConfig = gConfig.getValue('/LocalSite/Architecture','')
      if not systemConfig:
        return S_ERROR('Could not establish system configuration from Job requirements or LocalSite/Architecture section')

    realPythonPath = os.path.realpath(dPython)
#    if dPython != realPythonPath:
    self.log.debug('Real python path after resolving links is:')
    self.log.debug(realPythonPath)
    dPython = realPythonPath

    siteRootPython = 'sys.path.insert(0,"%s")' %(self.siteRoot)
    self.log.debug('DIRACPython is:\n%s' %dPython)
    self.log.debug('SiteRootPythonDir is:\n%s' %siteRootPython)
    print >> wrapper, wrapperTemplate % (siteRootPython,signature,jobID,date_time)
    libDir = '%s/%s/lib' %(self.siteRoot,systemConfig)
    scriptsDir = '%s/scripts' %(self.siteRoot)
    #contribDir = '%s/contrib' %(self.siteRoot)
    archLibDir = '%s/%s/lib/python' %(self.siteRoot,systemConfig)
    wrapper.write('sys.path.insert(0,"%s")\n' %(libDir))
    wrapper.write('sys.path.insert(0,"%s")\n' %(scriptsDir))
    #wrapper.write('sys.path.insert(0,"%s")\n' %(contribDir))
    wrapper.write('sys.path.insert(0,"%s")\n' %(archLibDir))
    #wrapper.write("os.environ['PYTHONPATH'] = '%s:%s:%s:%s:'+os.environ['PYTHONPATH']\n" %(contribDir,scriptsDir,libDir,self.siteRoot))
    wrapper.write("os.environ['PYTHONPATH'] = '%s:%s:%s:%s:'+os.environ['PYTHONPATH']\n" %(archLibDir,scriptsDir,libDir,self.siteRoot))
    wrapper.write("os.environ['LD_LIBRARY_PATH'] = '%s:%s:'+os.environ['LD_LIBRARY_PATH']\n" %(archLibDir,libDir))
    jobArgs = "execute("+str(arguments)+")\n"
    wrapper.write(jobArgs)
    wrapper.close ()
    os.chmod(jobWrapperFile,0755)
    jobExeFile = '%s/job/Wrapper/Job%s' %(self.siteRoot,jobID)
    jobFileContents = '#!/bin/sh\n%s %s -o LogLevel=debug' %(dPython,jobWrapperFile)
    jobFile = open(jobExeFile,'w')
    jobFile.write(jobFileContents)
    jobFile.close()
    os.chmod(jobExeFile,0755)
    #return S_OK(jobWrapperFile)
    return S_OK(jobExeFile)

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
