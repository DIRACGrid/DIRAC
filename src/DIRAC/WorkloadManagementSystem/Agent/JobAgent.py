"""
  The Job Agent class instantiates a CE that acts as a client to a
  compute resource and also to the WMS.
  The Job Agent constructs a classAd based on the local resource description in the CS
  and the current resource status that is used for matching.
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import division

__RCSID__ = "$Id$"

import os
import sys
import re
import time
import six

from diraccfg import CFG

from DIRAC import S_OK, S_ERROR, gConfig, rootPath, siteName
from DIRAC.Core.Utilities.ModuleFactory import ModuleFactory
from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.Core.Security import Properties
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
from DIRAC.Resources.Computing.BatchSystems.TimeLeft.TimeLeft import TimeLeft
from DIRAC.Resources.Computing.ComputingElementFactory import ComputingElementFactory
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
from DIRAC.RequestManagementSystem.private.RequestValidator import RequestValidator
from DIRAC.WorkloadManagementSystem.Client.MatcherClient import MatcherClient
from DIRAC.WorkloadManagementSystem.Client.PilotManagerClient import PilotManagerClient
from DIRAC.WorkloadManagementSystem.Client.JobManagerClient import JobManagerClient
from DIRAC.WorkloadManagementSystem.Client.JobReport import JobReport
from DIRAC.WorkloadManagementSystem.Client import JobStatus
from DIRAC.WorkloadManagementSystem.Utilities.Utils import createJobWrapper


class JobAgent(AgentModule):
  """ This agent is what runs in a worker node. The pilot runs it, after having prepared its configuration.
  """

  def __init__(self, agentName, loadName, baseAgentName=False, properties=None):
    """ Just defines some default parameters
    """
    if not properties:
      properties = {}
    super(JobAgent, self).__init__(agentName, loadName, baseAgentName, properties)

    # Inner CE
    # CE type the JobAgent submits to. It can be "InProcess" or "Pool" or "Singularity".
    self.ceName = 'InProcess'
    # "Inner" CE submission type (e.g. for Pool CE). It can be "InProcess" or "Singularity".
    self.innerCESubmissionType = 'InProcess'
    self.computingElement = None  # The ComputingElement object, e.g. SingularityComputingElement()

    # Localsite options
    self.siteName = 'Unknown'
    self.pilotReference = 'Unknown'
    self.defaultProxyLength = 86400 * 5

    # Agent options
    # This is the factor to convert raw CPU to Normalized units (based on the CPU Model)
    self.cpuFactor = 0.0
    self.jobSubmissionDelay = 10
    self.fillingMode = True
    self.minimumTimeLeft = 5000
    self.stopOnApplicationFailure = True
    self.stopAfterFailedMatches = 10
    self.jobCount = 0
    self.matchFailedCount = 0
    self.extraOptions = ''

    # Timeleft
    self.initTimes = os.times()
    self.initTimeLeft = 0.0
    self.timeLeft = self.initTimeLeft
    self.timeLeftUtil = None
    self.pilotInfoReportedFlag = False

  #############################################################################
  def initialize(self):
    """Sets default parameters and creates CE instance
    """
    # Disable monitoring
    self.am_setOption('MonitoringEnabled', False)

    localCE = gConfig.getValue('/LocalSite/LocalCE', self.ceName)
    if localCE != self.ceName:
      self.log.info('Defining Inner CE from local configuration', '= %s' % localCE)

    # Create backend Computing Element
    ceFactory = ComputingElementFactory()
    self.ceName = localCE.split('/')[0]  # It might be "Pool/Singularity", or simply "Pool"
    self.innerCESubmissionType = localCE.split('/')[1] if len(localCE.split('/')) == 2 else self.innerCESubmissionType
    ceInstance = ceFactory.getCE(self.ceName)
    if not ceInstance['OK']:
      self.log.warn("Can't instantiate a CE", ceInstance['Message'])
      return ceInstance
    self.computingElement = ceInstance['Value']
    self.computingElement.ceParameters['InnerCESubmissionType'] = self.innerCESubmissionType

    result = self.computingElement.getDescription()
    if not result['OK']:
      self.log.warn("Can not get the CE description")
      return result
    if isinstance(result['Value'], list):
      ceDict = result['Value'][0]
    else:
      ceDict = result['Value']
    self.initTimeLeft = ceDict.get('CPUTime', self.initTimeLeft)
    self.initTimeLeft = gConfig.getValue('/Resources/Computing/CEDefaults/MaxCPUTime', self.initTimeLeft)
    self.timeLeft = self.initTimeLeft

    self.initTimes = os.times()
    # Localsite options
    self.siteName = siteName()
    self.pilotReference = gConfig.getValue('/LocalSite/PilotReference', self.pilotReference)
    self.defaultProxyLength = gConfig.getValue('/Registry/DefaultProxyLifeTime', self.defaultProxyLength)
    # Agent options
    # This is the factor to convert raw CPU to Normalized units (based on the CPU Model)
    self.cpuFactor = gConfig.getValue('/LocalSite/CPUNormalizationFactor', self.cpuFactor)
    self.jobSubmissionDelay = self.am_getOption('SubmissionDelay', self.jobSubmissionDelay)
    self.fillingMode = self.am_getOption('FillingModeFlag', self.fillingMode)
    self.minimumTimeLeft = self.am_getOption('MinimumTimeLeft', self.minimumTimeLeft)
    self.stopOnApplicationFailure = self.am_getOption('StopOnApplicationFailure', self.stopOnApplicationFailure)
    self.stopAfterFailedMatches = self.am_getOption('StopAfterFailedMatches', self.stopAfterFailedMatches)
    self.extraOptions = gConfig.getValue('/AgentJobRequirements/ExtraOptions', self.extraOptions)
    # Utilities
    self.timeLeftUtil = TimeLeft()
    return S_OK()

  #############################################################################
  def execute(self):
    """The JobAgent execution method.
    """

    # Temporary mechanism to pass a shutdown message to the agent
    if os.path.exists('/var/lib/dirac_drain'):
      return self.__finish('Node is being drained by an operator')

    # Check if we can match jobs at all
    self.log.verbose('Job Agent execution loop')
    result = self.computingElement.available()
    if not result['OK']:
      self.log.info('Resource is not available', result['Message'])
      return self.__finish('CE Not Available')

    ceInfoDict = result['CEInfoDict']
    runningJobs = ceInfoDict.get("RunningJobs")
    availableSlots = result['Value']

    if not availableSlots:
      if runningJobs:
        self.log.info('No available slots', ': %d running jobs' % runningJobs)
        return S_OK('Job Agent cycle complete with %d running jobs' % runningJobs)
      self.log.info('CE is not available (and there are no running jobs)')
      return self.__finish('CE Not Available')

    if self.jobCount:
      # Only call timeLeft utility after a job has been picked up
      self.log.info('Attempting to check CPU time left for filling mode')
      if self.fillingMode:
        self.timeLeft = self.computeCPUWorkLeft()
        self.log.info('normalized CPU units remaining in slot', self.timeLeft)
        if self.timeLeft <= self.minimumTimeLeft:
          return self.__finish('No more time left')
        # Need to update the Configuration so that the new value is published in the next matching request
        result = self.computingElement.setCPUTimeLeft(cpuTimeLeft=self.timeLeft)
        if not result['OK']:
          return self.__finish(result['Message'])

        # Update local configuration to be used by submitted job wrappers
        localCfg = CFG()
        if self.extraOptions:
          localConfigFile = os.path.join('.', self.extraOptions)
        else:
          localConfigFile = os.path.join(rootPath, "etc", "dirac.cfg")
        localCfg.loadFromFile(localConfigFile)
        if not localCfg.isSection('/LocalSite'):
          localCfg.createNewSection('/LocalSite')
        localCfg.setOption('/LocalSite/CPUTimeLeft', self.timeLeft)
        localCfg.writeToFile(localConfigFile)

      else:
        return self.__finish('Filling Mode is Disabled')

    # if we are here we assume that a job can be matched
    result = self.computingElement.getDescription()
    if not result['OK']:
      return result

    # We can have several prioritized job retrieval strategies
    if isinstance(result['Value'], dict):
      ceDictList = [result['Value']]
    elif isinstance(result['Value'], list):
      # This is the case for Pool ComputingElement, and parameter 'MultiProcessorStrategy'
      ceDictList = result['Value']

    for ceDict in ceDictList:

      # Add pilot information
      gridCE = gConfig.getValue('LocalSite/GridCE', 'Unknown')
      if gridCE != 'Unknown':
        ceDict['GridCE'] = gridCE
      if 'PilotReference' not in ceDict:
        ceDict['PilotReference'] = str(self.pilotReference)
      ceDict['PilotBenchmark'] = self.cpuFactor
      ceDict['PilotInfoReportedFlag'] = self.pilotInfoReportedFlag

      # Add possible job requirements
      result = gConfig.getOptionsDict('/AgentJobRequirements')
      if result['OK']:
        requirementsDict = result['Value']
        ceDict.update(requirementsDict)
        self.log.info('Requirements:', requirementsDict)

      self.log.verbose('CE dict', ceDict)

      # here finally calling the matcher
      start = time.time()
      jobRequest = MatcherClient().requestJob(ceDict)
      matchTime = time.time() - start
      self.log.info('MatcherTime', '= %.2f (s)' % (matchTime))
      if jobRequest['OK']:
        break

    self.stopAfterFailedMatches = self.am_getOption('StopAfterFailedMatches', self.stopAfterFailedMatches)

    if not jobRequest['OK']:

      # if we don't match a job, independently from the reason,
      # we wait a bit longer before trying again
      self.am_setOption("PollingTime", int(self.am_getOption("PollingTime") * 1.5))

      if re.search('No match found', jobRequest['Message']):
        self.log.notice('Job request OK, but no match found', ': %s' % (jobRequest['Message']))
        self.matchFailedCount += 1
        if self.matchFailedCount > self.stopAfterFailedMatches:
          return self.__finish('Nothing to do for more than %d cycles' % self.stopAfterFailedMatches)
        return S_OK(jobRequest['Message'])
      elif jobRequest['Message'].find("seconds timeout") != -1:
        self.log.error('Timeout while requesting job', jobRequest['Message'])
        self.matchFailedCount += 1
        if self.matchFailedCount > self.stopAfterFailedMatches:
          return self.__finish('Nothing to do for more than %d cycles' % self.stopAfterFailedMatches)
        return S_OK(jobRequest['Message'])
      elif jobRequest['Message'].find("Pilot version does not match") != -1:
        errorMsg = 'Pilot version does not match the production version'
        self.log.error(errorMsg, jobRequest['Message'].replace(errorMsg, ''))
        return S_ERROR(jobRequest['Message'])
      else:
        self.log.notice('Failed to get jobs', ': %s' % (jobRequest['Message']))
        self.matchFailedCount += 1
        if self.matchFailedCount > self.stopAfterFailedMatches:
          return self.__finish('Nothing to do for more than %d cycles' % self.stopAfterFailedMatches)
        return S_OK(jobRequest['Message'])

    # Reset the Counter
    self.matchFailedCount = 0

    # If we are here it is because we matched a job
    matcherInfo = jobRequest['Value']
    if not self.pilotInfoReportedFlag:
      # Check the flag after the first access to the Matcher
      self.pilotInfoReportedFlag = matcherInfo.get('PilotInfoReportedFlag', False)
    jobID = matcherInfo['JobID']
    jobReport = JobReport(jobID, 'JobAgent@%s' % self.siteName)
    matcherParams = ['JDL', 'DN', 'Group']
    for param in matcherParams:
      if param not in matcherInfo:
        jobReport.setJobStatus(status=JobStatus.FAILED,
                               minorStatus='Matcher did not return %s' % (param))
        return self.__finish('Matcher Failed')
      elif not matcherInfo[param]:
        jobReport.setJobStatus(status=JobStatus.FAILED,
                               minorStatus='Matcher returned null %s' % (param))
        return self.__finish('Matcher Failed')
      else:
        self.log.verbose('Matcher returned', '%s = %s ' % (param, matcherInfo[param]))

    jobJDL = matcherInfo['JDL']
    jobGroup = matcherInfo['Group']
    ownerDN = matcherInfo['DN']

    optimizerParams = {}
    for key in matcherInfo:
      if key not in matcherParams:
        optimizerParams[key] = matcherInfo[key]

    parameters = self._getJDLParameters(jobJDL)
    if not parameters['OK']:
      jobReport.setJobStatus(status=JobStatus.FAILED,
                             minorStatus='Could Not Extract JDL Parameters')
      self.log.warn('Could Not Extract JDL Parameters', parameters['Message'])
      return self.__finish('JDL Problem')

    params = parameters['Value']
    if 'JobID' not in params:
      msg = 'Job has not JobID defined in JDL parameters'
      jobReport.setJobStatus(status=JobStatus.FAILED, minorStatus=msg)
      self.log.warn(msg)
      return self.__finish('JDL Problem')
    else:
      jobID = params['JobID']

    if 'JobType' not in params:
      self.log.warn('Job has no JobType defined in JDL parameters')
      jobType = 'Unknown'
    else:
      jobType = params['JobType']

    if 'CPUTime' not in params:
      self.log.warn('Job has no CPU requirement defined in JDL parameters')

    # Job requirements for determining the number of processors
    # the minimum number of processors requested
    processors = int(params.get('NumberOfProcessors',
                                int(params.get('MinNumberOfProcessors', 1))))
    # the maximum number of processors allowed to the payload
    maxNumberOfProcessors = int(params.get('MaxNumberOfProcessors', 0))
    # need or not the whole node for the job
    wholeNode = 'WholeNode' in params
    mpTag = 'MultiProcessor' in params.get('Tags', [])

    if self.extraOptions and 'dirac-jobexec' in params.get('Executable', '').strip():
      params['Arguments'] = (params.get('Arguments', '') + ' ' + self.extraOptions).strip()
      params['ExtraOptions'] = self.extraOptions

    self.log.verbose('Job request successful: \n', jobRequest['Value'])
    self.log.info('Received',
                  'JobID=%s, JobType=%s, OwnerDN=%s, JobGroup=%s' % (jobID, jobType, ownerDN, jobGroup))
    self.jobCount += 1
    try:
      jobReport.setJobParameter(par_name='MatcherServiceTime',
                                par_value=str(matchTime),
                                sendFlag=False)

      if 'BOINC_JOB_ID' in os.environ:
        # Report BOINC environment
        for thisp in ('BoincUserID', 'BoincHostID', 'BoincHostPlatform', 'BoincHostName'):
          jobReport.setJobParameter(par_name=thisp,
                                    par_value=gConfig.getValue('/LocalSite/%s' % thisp, 'Unknown'),
                                    sendFlag=False)

      jobReport.setJobStatus(status=JobStatus.MATCHED,
                             minorStatus='Job Received by Agent',
                             sendFlag=False)
      result_setupProxy = self._setupProxy(ownerDN, jobGroup)
      if not result_setupProxy['OK']:
        return self._rescheduleFailedJob(
            jobID, result_setupProxy['Message'], self.stopOnApplicationFailure)
      proxyChain = result_setupProxy.get('Value')

      # Save the job jdl for external monitoring
      self.__saveJobJDLRequest(jobID, jobJDL)

      software = self._checkInstallSoftware(jobID, params, ceDict, jobReport)
      if not software['OK']:
        self.log.error('Failed to install software for job', '%s' % (jobID))
        errorMsg = software['Message']
        if not errorMsg:
          errorMsg = 'Failed software installation'
        return self._rescheduleFailedJob(
            jobID, errorMsg, self.stopOnApplicationFailure)

      gridCE = gConfig.getValue('/LocalSite/GridCE', '')
      if gridCE:
        jobReport.setJobParameter(par_name='GridCE',
                                  par_value=gridCE,
                                  sendFlag=False)

      queue = gConfig.getValue('/LocalSite/CEQueue', '')
      if queue:
        jobReport.setJobParameter(par_name='CEQueue',
                                  par_value=queue,
                                  sendFlag=False)

      self.log.debug('Before self._submitJob() (%sCE)' % (self.ceName))
      result_submitJob = self._submitJob(
          jobID=jobID,
          jobParams=params,
          resourceParams=ceDict,
          optimizerParams=optimizerParams,
          proxyChain=proxyChain,
          jobReport=jobReport,
          processors=processors,
          wholeNode=wholeNode,
          maxNumberOfProcessors=maxNumberOfProcessors,
          mpTag=mpTag)

      # Committing the JobReport before evaluating the result of job submission
      res = jobReport.commit()
      if not res['OK']:
        resFD = jobReport.generateForwardDISET()
        if not resFD['OK']:
          self.log.error("Error generating ForwardDISET operation", resFD['Message'])
        elif resFD['Value']:
          # Here we create the Request.
          op = resFD['Value']
          request = Request()
          requestName = 'jobAgent_%s' % jobID
          request.RequestName = requestName.replace('"', '')
          request.JobID = jobID
          request.SourceComponent = "JobAgent_%s" % jobID
          request.addOperation(op)
          # This might fail, but only a message would be printed.
          self._sendFailoverRequest(request)

      if not result_submitJob['OK']:
        return self.__finish(result_submitJob['Message'])
      elif 'PayloadFailed' in result_submitJob:
        # Do not keep running and do not overwrite the Payload error
        message = 'Payload execution failed with error code %s' % result_submitJob['PayloadFailed']
        if self.stopOnApplicationFailure:
          return self.__finish(message, self.stopOnApplicationFailure)
        else:
          self.log.info(message)

      self.log.debug('After %sCE submitJob()' % (self.ceName))
    except Exception as subExcept:  # pylint: disable=broad-except
      self.log.exception("Exception in submission", "", lException=subExcept, lExcInfo=True)
      return self._rescheduleFailedJob(jobID, 'Job processing failed with exception', self.stopOnApplicationFailure)

    return S_OK('Job Agent cycle complete')

  #############################################################################
  def __saveJobJDLRequest(self, jobID, jobJDL):
    """Save job JDL local to JobAgent.
    """
    classAdJob = ClassAd(jobJDL)
    classAdJob.insertAttributeString('LocalCE', self.ceName)
    jdlFileName = jobID + '.jdl'
    jdlFile = open(jdlFileName, 'w')
    jdl = classAdJob.asJDL()
    jdlFile.write(jdl)
    jdlFile.close()

  #############################################################################
  def computeCPUWorkLeft(self, processors=1):
    """
    Compute CPU Work Left in hepspec06 seconds

    :param int processors: number of processors available
    :return: cpu work left (cpu time left * cpu power of the cpus)
    """
    # Sum all times but the last one (elapsed_time) and remove times at init (is this correct?)
    cpuTime = sum(os.times()[:-1]) - sum(self.initTimes[:-1])
    result = self.timeLeftUtil.getTimeLeft(cpuTime, processors)
    if result['OK']:
      cpuWorkLeft = result['Value']
    else:
      self.log.warn(
          "There were errors calculating time left using the Timeleft utility",
          result['Message'])
      self.log.warn("The time left will be calculated using os.times() and the info in our possession")
      cpuWorkLeft = self._getCPUWorkLeft(cpuTime)
    return cpuWorkLeft

  def _getCPUWorkLeft(self, cpuConsumed):
    """
    Compute the CPU Work Left as estimated by DIRAC using the process time
    and the CPU Power defined locally in the Local Config.

    :param int cpuConsumed: cpu time already consumed since the beginning of the execution
    :return: cpu work left (cpu time left * cpu power of the cpus)
    """
    self.log.info('Current raw CPU time consumed is %s' % cpuConsumed)
    cpuWorkleft = self.timeLeft
    if self.cpuFactor:
      cpuWorkleft = self.initTimeLeft - cpuConsumed * self.cpuFactor
    return cpuWorkleft

  #############################################################################
  def _setupProxy(self, ownerDN, ownerGroup):
    """
    Retrieve a proxy for the execution of the job
    """
    if gConfig.getValue('/DIRAC/Security/UseServerCertificate', False):
      proxyResult = self._requestProxyFromProxyManager(ownerDN, ownerGroup)
      if not proxyResult['OK']:
        self.log.error('Failed to setup proxy', proxyResult['Message'])
        return S_ERROR('Failed to setup proxy: %s' % proxyResult['Message'])
      return S_OK(proxyResult['Value'])
    else:
      ret = getProxyInfo(disableVOMS=True)
      if not ret['OK']:
        self.log.error('Invalid Proxy', ret['Message'])
        return S_ERROR('Invalid Proxy')

      proxyChain = ret['Value']['chain']
      if 'groupProperties' not in ret['Value']:
        print(ret['Value'])
        print(proxyChain.dumpAllToString())
        self.log.error('Invalid Proxy', 'Group has no properties defined')
        return S_ERROR('Proxy has no group properties defined')

      groupProps = ret['Value']['groupProperties']
      if Properties.GENERIC_PILOT in groupProps or Properties.PILOT in groupProps:
        proxyResult = self._requestProxyFromProxyManager(ownerDN, ownerGroup)
        if not proxyResult['OK']:
          self.log.error('Invalid Proxy', proxyResult['Message'])
          return S_ERROR('Failed to setup proxy: %s' % proxyResult['Message'])
        proxyChain = proxyResult['Value']

    return S_OK(proxyChain)

  #############################################################################
  def _requestProxyFromProxyManager(self, ownerDN, ownerGroup):
    """Retrieves user proxy with correct role for job and sets up environment to
       run job locally.
    """

    self.log.info("Requesting proxy', 'for %s@%s" % (ownerDN, ownerGroup))
    token = gConfig.getValue("/Security/ProxyToken", "")
    if not token:
      self.log.verbose("No token defined. Trying to download proxy without token")
      token = False
    retVal = gProxyManager.getPayloadProxyFromDIRACGroup(ownerDN, ownerGroup,
                                                         self.defaultProxyLength, token)
    if not retVal['OK']:
      self.log.error('Could not retrieve payload proxy', retVal['Message'])
      os.system('dirac-proxy-info')
      sys.stdout.flush()
      return S_ERROR('Error retrieving proxy')

    chain = retVal['Value']
    return S_OK(chain)

  #############################################################################
  def _checkInstallSoftware(self, jobID, jobParams, resourceParams, jobReport):
    """Checks software requirement of job and whether this is already present
       before installing software locally.
    """
    if 'SoftwareDistModule' not in jobParams:
      msg = 'Job has no software installation requirement'
      self.log.verbose(msg)
      return S_OK(msg)

    jobReport.setJobStatus(status=JobStatus.MATCHED,
                           minorStatus='Installing Software',
                           sendFlag=False)
    softwareDist = jobParams['SoftwareDistModule']
    self.log.verbose('Found VO Software Distribution module', ': %s' % (softwareDist))
    argumentsDict = {'Job': jobParams, 'CE': resourceParams}
    moduleFactory = ModuleFactory()
    moduleInstance = moduleFactory.getModule(softwareDist, argumentsDict)
    if not moduleInstance['OK']:
      return moduleInstance

    module = moduleInstance['Value']
    return module.execute()

  #############################################################################
  def _submitJob(self, jobID, jobParams, resourceParams, optimizerParams,
                 proxyChain, jobReport,
                 processors=1, wholeNode=False, maxNumberOfProcessors=0, mpTag=False):
    """ Submit job to the Computing Element instance after creating a custom
        Job Wrapper with the available job parameters.
    """
    logLevel = self.am_getOption('DefaultLogLevel', 'INFO')
    defaultWrapperLocation = self.am_getOption('JobWrapperTemplate',
                                               'DIRAC/WorkloadManagementSystem/JobWrapper/JobWrapperTemplate.py')

    # Add the number of requested processors to the job environment
    if 'ExecutionEnvironment' in jobParams:
      if isinstance(jobParams['ExecutionEnvironment'], six.string_types):
        jobParams['ExecutionEnvironment'] = jobParams['ExecutionEnvironment'].split(';')
    jobParams.setdefault('ExecutionEnvironment', []).append('DIRAC_JOB_PROCESSORS=%d' % processors)

    jobDesc = {"jobID": jobID,
               "jobParams": jobParams,
               "resourceParams": resourceParams,
               "optimizerParams": optimizerParams,
               "extraOptions": self.extraOptions,
               "defaultWrapperLocation": defaultWrapperLocation}
    result = createJobWrapper(log=self.log, logLevel=logLevel, **jobDesc)
    if not result['OK']:
      return result

    wrapperFile = result['Value']
    jobReport.setJobStatus(status=JobStatus.MATCHED,
                           minorStatus='Submitting To CE')

    self.log.info('Submitting JobWrapper',
                  '%s to %sCE' % (os.path.basename(wrapperFile), self.ceName))

    # Pass proxy to the CE
    proxy = proxyChain.dumpAllToString()
    if not proxy['OK']:
      self.log.error('Invalid proxy', proxy)
      return S_ERROR('Payload Proxy Not Found')

    payloadProxy = proxy['Value']
    submission = self.computingElement.submitJob(wrapperFile, payloadProxy,
                                                 numberOfProcessors=processors,
                                                 maxNumberOfProcessors=maxNumberOfProcessors,
                                                 wholeNode=wholeNode,
                                                 mpTag=mpTag,
                                                 jobDesc=jobDesc,
                                                 log=self.log,
                                                 logLevel=logLevel)
    submissionResult = S_OK('Job submitted')

    if submission['OK']:
      batchID = submission['Value']
      self.log.info('Job submitted', '(DIRAC JobID: %s; Batch ID: %s' % (jobID, batchID))
      if 'PayloadFailed' in submission:
        submissionResult['PayloadFailed'] = submission['PayloadFailed']
      time.sleep(self.jobSubmissionDelay)
    else:
      self.log.error('Job submission failed', jobID)
      jobReport.setJobParameter(par_name='ErrorMessage',
                                par_value='%s CE Submission Error' % (self.ceName),
                                sendFlag=False)
      if 'ReschedulePayload' in submission:
        self._rescheduleFailedJob(jobID, submission['Message'], self.stopOnApplicationFailure)
        return S_OK()  # Without this, the job is marked as failed
      else:
        if 'Value' in submission:  # yes, it's "correct", S_ERROR with 'Value' key
          self.log.error('Error in DIRAC JobWrapper or inner CE execution:',
                         'exit code = %s' % (str(submission['Value'])))
      self.log.error("CE Error", "%s : %s" % (self.ceName, submission['Message']))
      submissionResult = submission

    return submissionResult

  #############################################################################
  def _getJDLParameters(self, jdl):
    """Returns a dictionary of JDL parameters.
    """
    try:
      parameters = {}
#      print jdl
      if not re.search(r'\[', jdl):
        jdl = '[' + jdl + ']'
      classAdJob = ClassAd(jdl)
      paramsDict = classAdJob.contents
      for param, value in paramsDict.items():
        if value.strip().startswith('{'):
          self.log.debug('Found list type parameter %s' % (param))
          rawValues = value.replace('{', '').replace('}', '').replace('"', '').split()
          valueList = []
          for val in rawValues:
            if re.search(',$', val):
              valueList.append(val[:-1])
            else:
              valueList.append(val)
          parameters[param] = valueList
        else:
          parameters[param] = value.replace('"', '').replace('{', '"{').replace('}', '}"')
          self.log.debug('Found standard parameter %s: %s' % (param, parameters[param]))
      return S_OK(parameters)
    except Exception as x:
      self.log.exception(lException=x)
      return S_ERROR('Exception while extracting JDL parameters for job')

  #############################################################################
  def __finish(self, message, stop=True):
    """Force the JobAgent to complete gracefully.
    """
    if stop:
      self.log.info('JobAgent will stop',
                    'with message "%s", execution complete.' % message)
      self.am_stopExecution()
      return S_ERROR(message)

    return S_OK(message)

  #############################################################################
  def _rescheduleFailedJob(self, jobID, message, stop=True):
    """
    Set Job Status to "Rescheduled" and issue a reschedule command to the Job Manager
    """

    self.log.warn('Failure ==> rescheduling',
                  '(during %s)' % (message))

    jobReport = JobReport(int(jobID), 'JobAgent@%s' % self.siteName)

    # Setting a job parameter does not help since the job will be rescheduled,
    # instead set the status with the cause and then another status showing the
    # reschedule operation.

    jobReport.setJobStatus(status=JobStatus.RESCHEDULED,
                           applicationStatus=message,
                           sendFlag=True)

    self.log.info('Job will be rescheduled')
    result = JobManagerClient().rescheduleJob(jobID)
    if not result['OK']:
      self.log.error('Failed to reschedule job', result['Message'])
      return self.__finish('Problem Rescheduling Job', stop)

    self.log.info('Job Rescheduled', jobID)
    return self.__finish('Job Rescheduled', stop)

  #############################################################################
  def _sendFailoverRequest(self, request):
    """ Send failover reques per Job.
        This request would basically be a DISET request for setting the job status.

        If this fails, it only prints a message.

        :param Request request: Request() object
        :return: None
    """
    if len(request):
      self.log.info("Trying to send the failover request")
      # The request is ready, send it now
      isValid = RequestValidator().validate(request)
      if not isValid["OK"]:
        self.log.error("Failover request is not valid", isValid["Message"])
        self.log.error("Printing out the content of the request")
        reqToJSON = request.toJSON()
        if reqToJSON['OK']:
          print(str(reqToJSON['Value']))
        else:
          self.log.error(
              "Something went wrong creating the JSON from request",
              reqToJSON['Message'])
      else:
        # Now trying to send the request
        requestClient = ReqClient()
        result = requestClient.putRequest(request)
        if not result['OK']:
          self.log.error('Failed to set failover request', result['Message'])

  def finalize(self):
    """ Job Agent finalization method
    """

    # wait for all jobs to be completed
    res = self.computingElement.shutdown()
    if not res['OK']:
      self.log.error("CE could not be properly shut down", res['Message'])
    elif res['Value']:
      self.log.info("Job submission(s) result", res['Value'])

    gridCE = gConfig.getValue('/LocalSite/GridCE', '')
    queue = gConfig.getValue('/LocalSite/CEQueue', '')
    result = PilotManagerClient().setPilotStatus(
        str(self.pilotReference), 'Done', gridCE,
        'Report from JobAgent', self.siteName, queue)
    if not result['OK']:
      self.log.warn('Issue setting the pilot status', result['Message'])

    return S_OK()
