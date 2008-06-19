########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/Agent/JobAgent.py,v 1.36 2008/06/19 16:41:50 paterson Exp $
# File :   JobAgent.py
# Author : Stuart Paterson
########################################################################

"""  The Job Agent class instantiates a CE instance that acts as a client to a
     compute resource and also to the WMS.  The Job Agent constructs a classAd
     based on the local resource description in the CS and the current resource
     status that is used for matching.
"""

__RCSID__ = "$Id: JobAgent.py,v 1.36 2008/06/19 16:41:50 paterson Exp $"

from DIRAC.Core.Utilities.ModuleFactory                  import ModuleFactory
from DIRAC.Core.Utilities.ClassAd.ClassAdLight           import ClassAd
from DIRAC.Core.Utilities.TimeLeft.TimeLeft              import TimeLeft
from DIRAC.Core.Base.Agent                               import Agent
from DIRAC.Core.DISET.RPCClient                          import RPCClient
from DIRAC.Resources.Computing.ComputingElementFactory   import ComputingElementFactory
from DIRAC.Resources.Computing.ComputingElement          import ComputingElement
from DIRAC.Core.Utilities.GridCredentials                import setupProxy,restoreProxy,setDIRACGroup,getDIRACGroup
from DIRAC                                               import S_OK, S_ERROR, gConfig, platform

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
    self.matcher = RPCClient('WorkloadManagement/Matcher', timeout = 600 )
    self.jobReport  = RPCClient('WorkloadManagement/JobStateUpdate')
    self.wmsAdmin = RPCClient('WorkloadManagement/WMSAdministrator')

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
    self.siteName = gConfig.getValue('LocalSite/Site','Unknown')
    self.pilotReference = gConfig.getValue('LocalSite/PilotReference','Unknown')
    self.cpuFactor = gConfig.getValue('LocalSite/CPUScalingFactor','Unknown')
    self.jobWrapperTemplate = self.siteRoot+gConfig.getValue(self.section+'/JobWrapperTemplate','/DIRAC/WorkloadManagementSystem/JobWrapper/JobWrapperTemplate')
    self.jobSubmissionDelay = gConfig.getValue(self.section+'/SubmissionDelay',10)
    self.defaultProxyLength = gConfig.getValue(self.section+'/DefaultProxyLength',12)
    self.maxProxyLength =  gConfig.getValue(self.section+'/MaxProxyLength',72)
    #Added default in case pilot role was stripped somehow during proxy delegation
    self.defaultProxyGroup = gConfig.getValue(self.section+'/DefaultProxyGroup','lhcb_pilot')
    self.defaultLogLevel = gConfig.getValue(self.section+'/DefaultLogLevel','debug')
    self.fillingMode = gConfig.getValue(self.section+'/FillingModeFlag',1)
    self.jobCount=0
    return result

  #############################################################################
  def execute(self):
    """The JobAgent execution method.
    """
    #TODO: Initially adding timeleft utility for information, not yet instrumented
    #      to perform scheduling based on the output.
    if self.jobCount:
      #Only call timeLeft utility after a job has been picked up
      self.log.info('Attempting to check CPU time left for filling mode')
      if self.fillingMode:
        result = self.__getCPUTimeLeft()
        self.log.info('Result from TimeLeft utility:')
        if not result['OK']:
          self.log.warn(result['Message'])
          return self.__finish(result['Message'])
        timeLeft = result['Value']
        self.log.info('%s normalized CPU units remaining in slot' %(timeLeft))
        return self.__finish('Filling Mode is Disabled')

    self.log.verbose('Job Agent execution loop')
    available = self.computingElement.available()
    if not available['OK']:
      self.log.info('Resource is not available')
      self.log.info(available['Message'])
      return self.__finish('CE Not Available')

    self.log.info(available['Value'])

    ceJDL = self.computingElement.getJDL()
    resourceJDL = ceJDL['Value']
    self.log.verbose(resourceJDL)
    start = time.time()
    jobRequest = self.__requestJob(resourceJDL)
    matchTime = time.time() - start
    self.log.info('MatcherTime = %.2f (s)' %(matchTime))

    if not jobRequest['OK']:
      if re.search('No work available',jobRequest['Message']):
        self.log.info('Job request OK: %s' %(jobRequest['Message']))
        return S_OK(jobRequest['Message'])
      elif jobRequest['Message'].find( "seconds timeout" ):
        self.log.error( jobRequest['Message'] )
        return S_OK(jobRequest['Message'])
      else:
        self.log.info('Failed to get jobs: %s'  %(jobRequest['Message']))
        return S_OK(jobRequest['Message'])

    matcherInfo = jobRequest['Value']
    matcherParams = ['JDL','DN','Group']
    for p in matcherParams:
      if not matcherInfo.has_key(p):
        self.__report(jobID,'Failed','Matcher did not return %s' %(p))
        return self.__finish('Matcher Failed')
      elif not matcherInfo[p]:
        self.__report(jobID,'Failed','Matcher returned null %s' %(p))
        return self.__finish('Matcher Failed')
      else:
        self.log.verbose('Matcher returned %s = %s ' %(p,matcherInfo[p]))

    jobJDL = matcherInfo['JDL']
    jobGroup = matcherInfo['Group']
    ownerDN = matcherInfo['DN']

    optimizerParams = {}
    for key in matcherInfo.keys():
      if not key in matcherParams:
        value = matcherInfo[key]
        optimizerParams[key] = value

    parameters = self.__getJDLParameters(jobJDL)
    if not parameters['OK']:
      self.__report(jobID,'Failed','Could Not Extract JDL Parameters')
      self.log.warn(parameters['Message'])
      return self.__finish('JDL Problem')

    params = parameters['Value']
    if not params.has_key('JobID'):
      msg = 'Job has not JobID defined in JDL parameters'
      self.log.warn(msg)
      return S_OK(msg)
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

    if not params.has_key('MaxCPUTime'):
      self.log.warn('Job has no CPU requirement defined in JDL parameters')
      jobCPUReqt = 0
    else:
      jobCPUReqt = params['MaxCPUTime']

    self.log.verbose('Job request successful: \n %s' %(jobRequest['Value']))
    self.log.info('Received JobID=%s, JobType=%s, SystemConfig=%s' %(jobID,jobType,systemConfig))
    self.log.info('OwnerDN: %s JobGroup: %s' %(ownerDN,jobGroup) )
    self.jobCount+=1
    try:
      self.__setJobParam(jobID,'MatcherServiceTime',str(matchTime))
      self.__report(jobID,'Matched','Job Received by Agent')
      self.__setJobSite(jobID,self.siteName)
      self.__reportPilotInfo(jobID)
      proxyResult = self.__setupProxy(jobID,ownerDN,jobGroup,self.siteRoot,jobCPUReqt)
      if not proxyResult['OK']:
        self.log.warn('Problem while setting up proxy for job %s' %(jobID))
        self.__report(jobID,'Failed','Invalid Proxy')
        return self.__finish('Invalid Proxy')

      proxyTuple = proxyResult['Value']
      proxyLogging = self.__changeProxy(proxyTuple[1],proxyTuple[0])
      if not proxyLogging['OK']:
        self.log.warn('Problem while changing the proxy for job %s' %jobID)
        return proxyLogging

      saveJDL = self.__saveJobJDLRequest(jobID,jobJDL)
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
          self.log.warn(result['Message'])
          return self.__finish('Problem Rescheduling Job')
        else:
          self.log.info('Rescheduled job after software installation failure %s' %(jobID))
          return self.__finish('Job Rescheduled')

      self.log.verbose('Before %sCE submitJob()' %(self.ceName))
      submission = self.__submitJob(jobID,params,resourceParams,optimizerParams,jobJDL)
      if not submission['OK']:
        self.log.warn('Job submission failed during creation of the Job Wrapper')
        self.__report(jobID,'Failed','Job Wrapper Creation')
        return self.__finish('Problem Creating Job Wrapper')

      self.log.verbose('After %sCE submitJob()' %(self.ceName))
      self.log.info('Restoring original proxy %s' %(proxyTuple[1]))
      restoreProxy(proxyTuple[0],proxyTuple[1])
      proxyLogging = self.__changeProxy(proxyTuple[1],proxyTuple[0])
      if not proxyLogging['OK']:
        self.log.warn('Problem while changing back the proxy from job %s' %jobID)
        self.__finish('Job processing failed with exception')
    except Exception, x:
      self.log.exception(x)
      result = self.jobManager.rescheduleJob(jobID)
      if not result['OK']:
        self.log.warn(result['Message'])
      else:
        self.log.info('Rescheduled job %s' %(jobID))

      return self.__finish('Job processing failed with exception')

    return S_OK('Job Agent cycle complete')

  #############################################################################
  def __getCPUTimeLeft(self):
    """Wrapper around TimeLeft utility. Returns CPU time left in DIRAC normalized
       units. This value is subsequently used for scheduling further jobs in the
       same slot.
    """
    utime, stime, cutime, cstime, elapsed = os.times()
    cpuTime = utime + stime + cutime
    self.log.info('Current raw CPU time consumed is %s' %cpuTime)
    tl = TimeLeft()
    result = tl.getTimeLeft(cpuTime)
    return result

  #############################################################################
  def __changeProxy(self,oldProxy,newProxy):
    """Can call glexec utility here to set uid or simply log the changeover
       of a proxy.
    """
    self.log.verbose('Log proxy change (to be instrumented)')
    return S_OK()

  #############################################################################
  def __setupProxy(self,job,ownerDN,jobGroup,workingDir,jobCPUTime):
    """Retrieves user proxy with correct role for job and sets up environment to
       run job locally.
    """
    hours = int(round(int(jobCPUTime)/60))
    if hours > self.maxProxyLength:
      hours = self.maxProxyLength
    if hours < self.defaultProxyLength:
      hours = self.defaultProxyLength

    currentGroup =  getDIRACGroup('None')
    if currentGroup == 'None':
      self.log.warn('Current DIRAC group is not found, setting to %s explicitly' %(self.defaultProxyGroup))
      setDIRACGroup(self.defaultProxyGroup)

    currentGroup =  getDIRACGroup('None')
    self.log.info('Current DIRAC Group is: %s' %(currentGroup))
    self.log.info('Attempting to obtain proxy with length %s hours for DN\n %s' %(hours,ownerDN))
    result = self.wmsAdmin.getProxy(ownerDN,jobGroup,hours)
    if not result['OK']:
      self.log.warn('Could not retrieve proxy from WMS Administrator')
      self.log.verbose(result)
      self.__setJobParam(job,'ProxyError',str(result))
      self.log.info('Checking current proxy info:')
      os.system('voms-proxy-info -all')
      sys.stdout.flush()
      self.__report(job,'Failed','Proxy Retrieval')
      return S_ERROR('Error retrieving proxy')

    if result.has_key('Message'):
      self.log.warn('WMSAdministrator Message: %s' %(result['Message']))
      self.__setJobParam(job,'ProxyWarning',result['Message'])

    proxyStr = result['Value']
    if not os.path.exists('%s/proxy' %(workingDir)):
      os.mkdir('%s/proxy' %(workingDir))

    proxyFile = '%s/proxy/proxy%s' %(workingDir,job)
    setupResult = setupProxy(proxyStr,proxyFile)
    #TODO: remove this temporary debugging output when proxy issue solved
    self.log.info('VOMS proxy info temporarily printed for debugging purposes')
    os.system('voms-proxy-info -all')
    sys.stdout.flush()
    if not setupResult['OK']:
      self.log.warn('Could not create environment for proxy')
      self.log.verbose(setupResult)
      self.__report(job,'Failed','Proxy Environment')
      return S_ERROR('Error setting up proxy')

    self.log.info('Setting DIRAC group to %s' %jobGroup)
    setDIRACGroup(jobGroup)

    self.log.verbose(setupResult)
    return setupResult

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
  def __submitJob(self,jobID,jobParams,resourceParams,optimizerParams,jobJDL):
    """Submit job to the Computing Element instance after creating a custom
       Job Wrapper with the available job parameters.
    """
    result = self.__createJobWrapper(jobID,jobParams,resourceParams,optimizerParams)

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
      self.log.verbose('Set JobParameter: Local batch ID %s' %(batchID))
      self.__setJobParam(jobID,'LocalBatchID',str(batchID))
      time.sleep(self.jobSubmissionDelay)
    else:
      self.log.warn('Job '+str(jobID)+' submission failed')
      self.__setJobParam(jobID,'ErrorMessage','%s CE Submission Error' %(self.ceName))
      self.__report(jobID,'Failed','%s CE Submission Error' %(self.ceName))

    return S_OK('Job submitted')

  #############################################################################
  def __createJobWrapper(self,jobID,jobParams,resourceParams,optimizerParams):
    """This method creates a job wrapper filled with the CE and Job parameters
       to executed the job.
    """
    arguments = {'Job':jobParams,'CE':resourceParams,'Optimizer':optimizerParams}
    self.log.verbose('Job arguments are: \n %s' %(arguments))

    if not os.path.exists(self.siteRoot+'/job/Wrapper'):
      os.makedirs(self.siteRoot+'/job/Wrapper')

    jobWrapperFile = self.siteRoot+'/job/Wrapper/Wrapper_%s' %(jobID)
    if os.path.exists(jobWrapperFile):
      self.log.verbose('Removing existing Job Wrapper for %s' %(jobID))
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

    logLevel=self.defaultLogLevel
    if jobParams.has_key('LogLevel'):
      logLevel = jobParams['LogLevel']
      self.log.info('Found Job LogLevel JDL parameter with value: %s' %(logLevel))
    else:
      self.log.info('Applying default LogLevel JDL parameter with value: %s' %(logLevel))

    realPythonPath = os.path.realpath(dPython)
#    if dPython != realPythonPath:
    self.log.debug('Real python path after resolving links is:')
    self.log.debug(realPythonPath)
    dPython = realPythonPath

    siteRootPython = 'sys.path.insert(0,"%s")' %(self.siteRoot)
    self.log.debug('DIRACPython is:\n%s' %dPython)
    self.log.debug('SiteRootPythonDir is:\n%s' %siteRootPython)
    print >> wrapper, wrapperTemplate % (siteRootPython,signature,jobID,date_time)
    libDir = '%s/%s/lib' %(self.siteRoot,platform)
    scriptsDir = '%s/scripts' %(self.siteRoot)
    #contribDir = '%s/contrib' %(self.siteRoot)
    #archLibDir = '%s/%s/lib/python' %(self.siteRoot,systemConfig)
    #archLib64Dir = '%s/%s/lib64/python' %(self.siteRoot,systemConfig)
    #lib64Dir = '%s/%s/lib64' %(self.siteRoot,systemConfig)
    #usrlibDir = '%s/%s/usr/lib' %(self.siteRoot,systemConfig)
    #wrapper.write('sys.path.insert(0,"%s")\n' %(libDir))
    #wrapper.write('sys.path.insert(0,"%s")\n' %(libDir))
    #wrapper.write('sys.path.insert(0,"%s")\n' %(scriptsDir))
    #wrapper.write('sys.path.insert(0,"%s")\n' %(archLibDir))
    #wrapper.write('sys.path.insert(0,"%s")\n' %(archLib64Dir))
    #wrapper.write("os.environ['PYTHONPATH'] = '%s:%s:%s:%s:'+os.environ['PYTHONPATH']\n" %(contribDir,scriptsDir,libDir,self.siteRoot))
    #wrapper.write("os.environ['PYTHONPATH'] = '%s:%s:%s:%s:%s:'+os.environ['PYTHONPATH']\n" %(archLibDir,archLib64Dir,scriptsDir,libDir,self.siteRoot))
    #wrapper.write("os.environ['LD_LIBRARY_PATH'] = '%s:%s:%s'+os.environ['LD_LIBRARY_PATH']\n" %(libDir,lib64Dir,usrlibDir))
    wrapper.write("os.environ['LD_LIBRARY_PATH'] = '%s'\n" %(libDir))
    jobArgs = "execute("+str(arguments)+")\n"
    wrapper.write(jobArgs)
    wrapper.close ()
    os.chmod(jobWrapperFile,0755)
    jobExeFile = '%s/job/Wrapper/Job%s' %(self.siteRoot,jobID)
    #jobFileContents = '#!/bin/sh\nexport LD_LIBRARY_PATH=%s:%s:%s:$LD_LIBRARY_PATH\n%s %s -o LogLevel=debug' %(libDir,lib64Dir,usrlibDir,dPython,jobWrapperFile)
    jobFileContents = '#!/bin/sh\nexport LD_LIBRARY_PATH=%s\n%s %s -o LogLevel=%s' %(libDir,dPython,jobWrapperFile,logLevel)
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
    classAdJob = ClassAd(jobJDL)
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
          rawValues = value.replace('{','').replace('}','').replace('"','').split()
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
    self.log.verbose('setJobStatus(%s,%s,%s,%s)' %(jobID,status,minorStatus,'JobAgent'))
    if not jobStatus['OK']:
      self.log.warn(jobStatus['Message'])

    return jobStatus

  #############################################################################
  def __reportPilotInfo(self,jobID):
    """Sends back useful information for the pilotAgentsDB via the WMSAdministrator
       service.
    """
    result = self.wmsAdmin.setJobForPilot(int(jobID),str(self.pilotReference))
    if not result['OK']:
      self.log.warn(result['Message'])

    result = self.wmsAdmin.setPilotBenchmark(str(self.pilotReference),float(self.cpuFactor))
    if not result['OK']:
      self.log.warn(result['Message'])

    return S_OK()

  #############################################################################
  def __setJobSite(self,jobID,site):
    """Wraps around setJobSite of state update client
    """
    jobSite = self.jobReport.setJobSite(int(jobID),site)
    self.log.verbose('setJobSite(%s,%s)' %(jobID,site))
    if not jobSite['OK']:
      self.log.warn(jobSite['Message'])

    return jobSite

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
  def __finish(self,message):
    """Force the JobAgent to complete gracefully.
    """
    self.log.info('JobAgent will stop with message "%s", execution complete.' %message)
    fd = open(self.controlDir+'/stop_agent','w')
    fd.write('JobAgent Stopped at %s [UTC]' % (time.asctime(time.gmtime())))
    fd.close()
    return S_OK(message)

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
