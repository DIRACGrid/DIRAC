########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/Agent/JobAgent.py,v 1.67 2009/04/15 07:52:32 rgracian Exp $
# File :   JobAgent.py
# Author : Stuart Paterson
########################################################################

"""  The Job Agent class instantiates a CE instance that acts as a client to a
     compute resource and also to the WMS.  The Job Agent constructs a classAd
     based on the local resource description in the CS and the current resource
     status that is used for matching.
"""

__RCSID__ = "$Id: JobAgent.py,v 1.67 2009/04/15 07:52:32 rgracian Exp $"

from DIRAC.Core.Utilities.ModuleFactory                  import ModuleFactory
from DIRAC.Core.Utilities.ClassAd.ClassAdLight           import ClassAd
from DIRAC.Core.Utilities.TimeLeft.TimeLeft              import TimeLeft
from DIRAC.Core.Base.Agent                               import Agent
from DIRAC.Core.DISET.RPCClient                          import RPCClient
from DIRAC.Resources.Computing.ComputingElementFactory   import ComputingElementFactory
from DIRAC.Resources.Computing.ComputingElement          import ComputingElement
from DIRAC                                               import S_OK, S_ERROR, gConfig, platform
from DIRAC.FrameworkSystem.Client.ProxyManagerClient     import gProxyManager
from DIRAC.Core.Security.Misc                            import getProxyInfo
from DIRAC.Core.Security                                 import Locations
from DIRAC.Core.Security                                 import Properties
from DIRAC.WorkloadManagementSystem.Client.JobReport     import JobReport

import os, sys, re, string, time

AGENT_NAME = 'WorkloadManagement/JobAgent'

class JobAgent(Agent):

  #############################################################################
  def __init__(self):
    """ Standard constructor for Agent
    """
    Agent.__init__(self,AGENT_NAME)
    #self.log.setLevel('verb')

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
    localCE = gConfig.getOption('/LocalSite/LocalCE','')
    if localCE['OK']:
      if localCE['Value']:
        self.log.info('Defining CE from local configuration = %s' %localCE['Value'])
        ceUniqueID=localCE

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
    self.jobWrapperTemplate = self.siteRoot+gConfig.getValue(self.section+'/JobWrapperTemplate','/DIRAC/WorkloadManagementSystem/JobWrapper/JobWrapperTemplate.py')
    self.jobSubmissionDelay = gConfig.getValue(self.section+'/SubmissionDelay',10)
    self.defaultProxyLength = gConfig.getValue( '/Security/DefaultProxyLifeTime', 86400*5 )
    self.defaultLogLevel = gConfig.getValue(self.section+'/DefaultLogLevel','debug')
    self.fillingMode = gConfig.getValue(self.section+'/FillingModeFlag',1)
    self.jobCount=0
    return result

  #############################################################################
  def execute(self):
    """The JobAgent execution method.
    """
    jobManager  = RPCClient('WorkloadManagement/JobManager')
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
      ret = getProxyInfo( disableVOMS = True )
      if not ret['OK']:
        self.log.error( 'Invalid Proxy', ret['Message'] )
        return self.__rescheduleFailedJob( jobID ,'Invalid Proxy' )

      proxyChain = ret['Value']['chain']
      if not 'groupProperties' in ret['Value']:
        print ret['Value']
        print proxyChain.dumpAllToString()
        self.log.error( 'Invalid Proxy', 'Group has no properties defined')
        return self.__rescheduleFailedJob( jobID , 'Proxy has no group properties defined')

      if Properties.GENERIC_PILOT in ret['Value']['groupProperties']:
        proxyResult = self.__setupProxy(jobID,ownerDN,jobGroup,self.siteRoot)
        if not proxyResult['OK']:
          self.log.error( 'Invalid Proxy', proxyResult['Message'] )
          return self.__rescheduleFailedJob( jobID , 'Fail to setup proxy' )
        else:
          proxyChain = proxyResult['Value']

      saveJDL = self.__saveJobJDLRequest(jobID,jobJDL)
      #self.__report(jobID,'Matched','Job Prepared to Submit')

      resourceParameters = self.__getJDLParameters(resourceJDL)
      if not resourceParameters['OK']:
        return resourceParameters
      resourceParams = resourceParameters['Value']

      software = self.__checkInstallSoftware(jobID,params,resourceParams)
      if not software['OK']:
        self.log.error('Failed to install software for job %s' %(jobID))
        errorMsg = software['Message']
        if not errorMsg:
          errorMsg = 'Failed software installation'
        return self.__rescheduleFailedJob( jobID, errorMsg )
        
      self.log.verbose('Before %sCE submitJob()' %(self.ceName))
      submission = self.__submitJob(jobID,params,resourceParams,optimizerParams,jobJDL,proxyChain)
      if not submission['OK']:
        self.log.warn('Job submission failed during creation of the Job Wrapper')
        self.__report(jobID,'Failed','Job Wrapper Creation')
        return self.__finish('Error During CE Submission')

      self.log.verbose('After %sCE submitJob()' %(self.ceName))
    except Exception, x:
      self.log.exception()
      return self.__rescheduleFailedJob( jobID , 'Job processing failed with exception' )

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
  def __setupProxy(self,job,ownerDN,ownerGroup,workingDir):
    """Retrieves user proxy with correct role for job and sets up environment to
       run job locally.
    """
    self.log.info( "Requesting proxy for %s@%s" % ( ownerDN, ownerGroup ) )
    token = gConfig.getValue( "/Security/ProxyToken", "" )
    if token:
      retVal = gProxyManager.getPayloadProxyFromDIRACGroup( ownerDN,
                                                            ownerGroup,
                                                            token,
                                                            self.defaultProxyLength
                                                          )
    else:
      self.log.info( "No token defined. Trying to download proxy without token" )
      retVal = gProxyManager.downloadVOMSProxy( ownerDN,
                                                ownerGroup,
                                                limited = True,
                                                requiredTimeLeft = self.defaultProxyLength )
    if not retVal[ 'OK' ]:
      self.log.error('Could not retrieve proxy')
      self.log.verbose(retVal)
      self.__setJobParam( job, 'ProxyError', retVal[ 'Message' ] )
      os.system('dirac-proxy-info')
      sys.stdout.flush()
      self.__report(job,'Failed','Proxy Retrieval')
      return S_ERROR( 'Error retrieving proxy' )

    chain = retVal[ 'Value' ]

    return S_OK( chain )

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
  def __submitJob(self,jobID,jobParams,resourceParams,optimizerParams,jobJDL,proxyChain):
    """Submit job to the Computing Element instance after creating a custom
       Job Wrapper with the available job parameters.
    """
    result = self.__createJobWrapper(jobID,jobParams,resourceParams,optimizerParams)

    if not result['OK']:
      return result

    wrapperFile = result['Value']
    self.__report(jobID,'Matched','Submitted To CE')

    wrapperName = os.path.basename(wrapperFile)
    self.log.info('Submitting %s to %sCE' %(wrapperName,self.ceName))

    #Pass proxy to the CE
    proxy =  proxyChain.dumpAllToString()
    if not proxy['OK']:
      self.log.error(proxy)
      return S_ERROR('Payload Proxy Not Found')

    payloadProxy=proxy['Value']
    batchID = 'dc%s' %(jobID)
    submission = self.computingElement.submitJob(wrapperFile,jobJDL,payloadProxy,batchID)

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
    arguments = {'Job':jobParams,
                 'CE':resourceParams,
                 'Optimizer':optimizerParams}
    self.log.verbose('Job arguments are: \n %s' %(arguments))

    workingDir = gConfig.getValue('/LocalSite/WorkingDirectory',self.siteRoot)
    if not os.path.exists('%s/job/Wrapper' %(workingDir)):
      try:
        os.makedirs('%s/job/Wrapper' %(workingDir))
      except Exception,x:
        self.log.error('Could not create directory %s/job/Wrapper for job wrapper script' %(workingDir),str(x))
        return S_ERROR('Could not create directory %s/job/Wrapper for job wrapper script' %(workingDir))

    jobWrapperFile = '%s/job/Wrapper/Wrapper_%s' %(workingDir,jobID)
    if os.path.exists(jobWrapperFile):
      self.log.verbose('Removing existing Job Wrapper for %s' %(jobID))
      os.remove(jobWrapperFile)
    fd = open(self.jobWrapperTemplate,'r')
    wrapperTemplate = fd.read()
    fd.close()

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
        self.log.warn( 'No LocalSite/Root defined' )
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
    self.log.debug('Real python path after resolving links is:')
    self.log.debug(realPythonPath)
    dPython = realPythonPath

    siteRootPython = self.siteRoot
    self.log.debug('DIRACPython is:\n%s' %dPython)
    self.log.debug('SiteRootPythonDir is:\n%s' %siteRootPython)
    libDir = '%s/%s/lib' %(self.siteRoot,platform)
    scriptsDir = '%s/scripts' %(self.siteRoot)
    wrapperTemplate = wrapperTemplate.replace( "@SIGNATURE@", str(signature) )
    wrapperTemplate = wrapperTemplate.replace( "@JOBID@", str(jobID) )
    wrapperTemplate = wrapperTemplate.replace( "@DATESTRING@", str(date_time) )
    wrapperTemplate = wrapperTemplate.replace( "@JOBARGS@", str(arguments) )
    wrapperTemplate = wrapperTemplate.replace("@SITEPYTHON@",str(siteRootPython) )
    wrapper = open (jobWrapperFile,"w")
    wrapper.write( wrapperTemplate )
    wrapper.close ()
    jobExeFile = '%s/job/Wrapper/Job%s' %(workingDir,jobID)
    #jobFileContents = '#!/bin/sh\nexport LD_LIBRARY_PATH=%s:%s:%s:$LD_LIBRARY_PATH\n%s %s -o LogLevel=debug' %(libDir,lib64Dir,usrlibDir,dPython,jobWrapperFile)
    #jobFileContents = '#!/bin/sh\nexport LD_LIBRARY_PATH=%s\n%s %s -o LogLevel=%s' %(libDir,dPython,jobWrapperFile,logLevel)
    jobFileContents = '#!/bin/sh\n%s %s -o LogLevel=%s' %(dPython,jobWrapperFile,logLevel)
    jobFile = open(jobExeFile,'w')
    jobFile.write(jobFileContents)
    jobFile.close()
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
      matcher = RPCClient('WorkloadManagement/Matcher', timeout = 600 )
      result = matcher.requestJob(resourceJDL)
      return result
    except Exception, x:
      self.log.exception(lException=x)
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
      self.log.exception(lException=x)
      return S_ERROR('Exception while extracting JDL parameters for job')

  #############################################################################
  def __report(self,jobID,status,minorStatus):
    """Wraps around setJobStatus of state update client
    """
    jobReport = RPCClient('WorkloadManagement/JobStateUpdate')
    jobStatus = jobReport.setJobStatus(int(jobID),status,minorStatus,'JobAgent')
    self.log.verbose('setJobStatus(%s,%s,%s,%s)' %(jobID,status,minorStatus,'JobAgent'))
    if not jobStatus['OK']:
      self.log.warn(jobStatus['Message'])

    return jobStatus

  #############################################################################
  def __reportPilotInfo(self,jobID):
    """Sends back useful information for the pilotAgentsDB via the WMSAdministrator
       service.
    """

    gridCE = gConfig.getValue('LocalSite/GridCE','Unknown')

    wmsAdmin = RPCClient('WorkloadManagement/WMSAdministrator')
    if gridCE != 'Unknown':
      result = wmsAdmin.setJobForPilot(int(jobID),str(self.pilotReference),gridCE)
    else:
      result = wmsAdmin.setJobForPilot(int(jobID),str(self.pilotReference))

    if not result['OK']:
      self.log.warn(result['Message'])

    result = wmsAdmin.setPilotBenchmark(str(self.pilotReference),float(self.cpuFactor))
    if not result['OK']:
      self.log.warn(result['Message'])

    return S_OK()

  #############################################################################
  def __setJobSite(self,jobID,site):
    """Wraps around setJobSite of state update client
    """
    jobReport = RPCClient('WorkloadManagement/JobStateUpdate')
    jobSite = jobReport.setJobSite(int(jobID),site)
    self.log.verbose('setJobSite(%s,%s)' %(jobID,site))
    if not jobSite['OK']:
      self.log.warn(jobSite['Message'])

    return jobSite

  #############################################################################
  def __setJobParam(self,jobID,name,value):
    """Wraps around setJobParameter of state update client
    """
    jobReport = RPCClient('WorkloadManagement/JobStateUpdate')
    jobParam = jobReport.setJobParameter(int(jobID),str(name),str(value))
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

  #############################################################################
  def __rescheduleFailedJob( self, jobID, message):

    self.log.warn('Failure during %s' %(message))

    jobManager  = RPCClient('WorkloadManagement/JobManager')
    jobReport = JobReport(int(jobID),'JobAgent')

    #Setting a job parameter does not help since the job will be rescheduled,
    #instead set the status with the cause and then another status showing the
    #reschedule operation.

    jobReport.setJobStatus(  'Failed', message, sendFlag = False )
    jobReport.setApplicationStatus( 'Failed %s ' % message, sendFlag = False )
    jobReport.setJobStatus( minor = 'ReschedulingJob', sendFlag = True )

    self.log.info('Job will be rescheduled')
    result = jobManager.rescheduleJob( jobID)
    if not result['OK']:
      self.log.error(result['Message'])
      return self.__finish('Problem Rescheduling Job')
    else:
      self.log.info('Job Rescheduled %s' %(jobID))
      return self.__finish('Job Rescheduled')

    return
  #############################################################################
  def finalize(self):
    """Force the JobAgent to complete gracefully.
    """

    gridCE = gConfig.getValue('LocalSite/GridCE','Unknown')

    wmsAdmin = RPCClient('WorkloadManagement/WMSAdministrator')
    result = wmsAdmin.setPilotStatus(str(self.pilotReference),'Done',gridCE,'Report from JobAgent')
    if not result['OK']:
      self.log.warn(result['Message'])

    Agent.finalize(self)

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
