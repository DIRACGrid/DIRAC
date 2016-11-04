"""
  The Job Agent class instantiates a CE that acts as a client to a
  compute resource and also to the WMS.
  The Job Agent constructs a classAd based on the local resource description in the CS
  and the current resource status that is used for matching.
"""

import os
import sys
import re
import time

from DIRAC                                                  import S_OK, S_ERROR, gConfig, rootPath
from DIRAC.Core.Utilities.ModuleFactory                     import ModuleFactory
from DIRAC.Core.Utilities.ClassAd.ClassAdLight              import ClassAd
from DIRAC.Core.Utilities.TimeLeft.TimeLeft                 import TimeLeft
from DIRAC.Core.Utilities.CFG                               import CFG
from DIRAC.Core.Utilities.Os                                import getNumberOfCores
from DIRAC.Core.Base.AgentModule                            import AgentModule
from DIRAC.Core.DISET.RPCClient                             import RPCClient
from DIRAC.Core.Security.ProxyInfo                          import getProxyInfo
from DIRAC.Core.Security                                    import Properties
from DIRAC.FrameworkSystem.Client.ProxyManagerClient        import gProxyManager
from DIRAC.Resources.Computing.ComputingElementFactory      import ComputingElementFactory
from DIRAC.WorkloadManagementSystem.Client.JobReport        import JobReport
from DIRAC.WorkloadManagementSystem.JobWrapper.JobWrapper   import rescheduleFailedJob
from DIRAC.WorkloadManagementSystem.Utilities.Utils         import createJobWrapper

__RCSID__ = "$Id$"


class JobAgent( AgentModule ):
  """ This agent is what runs in a worker node. The pilot runs it, after having prepared its configuration.
  """

  def __init__(self, agentName, loadName, baseAgentName = False, properties = None):
    """ Just defines some default parameters
    """
    if not properties:
      properties = {}
    super( JobAgent, self ).__init__( agentName, loadName, baseAgentName, properties )

    self.ceName = 'InProcess'
    self.computingElement = None
    self.timeLeft = 0.0

    self.initTimes = os.times()
    # Localsite options
    self.siteName = 'Unknown'
    self.pilotReference = 'Unknown'
    self.defaultProxyLength = 86400 * 5
    # Agent options
    # This is the factor to convert raw CPU to Normalized units (based on the CPU Model)
    self.cpuFactor = 0.0
    self.jobSubmissionDelay = 10
    self.fillingMode = False
    self.minimumTimeLeft = 1000
    self.stopOnApplicationFailure = True
    self.stopAfterFailedMatches = 10
    self.jobCount = 0
    self.matchFailedCount = 0
    self.extraOptions = ''
    # Timeleft
    self.timeLeftUtil = None
    self.timeLeftError = ''
    self.scaledCPUTime = 0.0
    self.pilotInfoReportedFlag = False


  #############################################################################
  def initialize( self, loops = 0 ):
    """Sets default parameters and creates CE instance
    """
    # Disable monitoring
    self.am_setOption( 'MonitoringEnabled', False )
    # self.log.setLevel('debug') #temporary for debugging
    self.am_setOption( 'MaxCycles', loops )

    ceType = self.am_getOption( 'CEType', 'InProcess' )
    localCE = gConfig.getValue( '/LocalSite/LocalCE', '' )
    if localCE:
      self.log.info( 'Defining CE from local configuration = %s' % localCE )
      ceType = localCE

    # Create backend Computing Element
    ceFactory = ComputingElementFactory()
    self.ceName = ceType
    ceInstance = ceFactory.getCE( ceType )
    if not ceInstance['OK']:
      self.log.warn( ceInstance['Message'] )
      return ceInstance
    self.computingElement = ceInstance['Value']

    result = self.computingElement.getDescription()
    if not result['OK']:
      self.log.warn( "Can not get the CE description" )
      return result
    ceDict = result['Value']
    self.timeLeft = ceDict.get( 'CPUTime', self.timeLeft )
    self.timeLeft = gConfig.getValue( '/Resources/Computing/CEDefaults/MaxCPUTime', self.timeLeft )

    self.initTimes = os.times()
    # Localsite options
    self.siteName = gConfig.getValue( '/LocalSite/Site', self.siteName )
    self.pilotReference = gConfig.getValue( '/LocalSite/PilotReference', self.pilotReference )
    self.defaultProxyLength = gConfig.getValue( '/Registry/DefaultProxyLifeTime', self.defaultProxyLength )
    # Agent options
    # This is the factor to convert raw CPU to Normalized units (based on the CPU Model)
    self.cpuFactor = gConfig.getValue( '/LocalSite/CPUNormalizationFactor', self.cpuFactor )
    self.jobSubmissionDelay = self.am_getOption( 'SubmissionDelay', self.jobSubmissionDelay )
    self.fillingMode = self.am_getOption( 'FillingModeFlag', self.fillingMode )
    self.minimumTimeLeft = self.am_getOption( 'MinimumTimeLeft', self.minimumTimeLeft )
    self.stopOnApplicationFailure = self.am_getOption( 'StopOnApplicationFailure', self.stopOnApplicationFailure )
    self.stopAfterFailedMatches = self.am_getOption( 'StopAfterFailedMatches', self.stopAfterFailedMatches )
    self.extraOptions = gConfig.getValue( '/AgentJobRequirements/ExtraOptions', self.extraOptions )
    # Timeleft
    self.timeLeftUtil = TimeLeft()
    return S_OK()

  #############################################################################
  def execute( self ):
    """The JobAgent execution method.
    """
    if self.jobCount:
      # Temporary mechanism to pass a shutdown message to the agent
      if os.path.exists('/var/lib/dirac_drain'):
        return self.__finish( 'Node is being drained by an operator' )
      # Only call timeLeft utility after a job has been picked up
      self.log.info( 'Attempting to check CPU time left for filling mode' )
      if self.fillingMode:
        if self.timeLeftError:
          self.log.warn( self.timeLeftError )
          return self.__finish( self.timeLeftError )
        self.log.info( '%s normalized CPU units remaining in slot' % ( self.timeLeft ) )
        if self.timeLeft <= self.minimumTimeLeft:
          return self.__finish( 'No more time left' )
        # Need to update the Configuration so that the new value is published in the next matching request
        result = self.computingElement.setCPUTimeLeft( cpuTimeLeft = self.timeLeft )
        if not result['OK']:
          return self.__finish( result['Message'] )

        # Update local configuration to be used by submitted job wrappers
        localCfg = CFG()
        if self.extraOptions:
          localConfigFile = os.path.join( '.', self.extraOptions )
        else:
          localConfigFile = os.path.join( rootPath, "etc", "dirac.cfg" )
        localCfg.loadFromFile( localConfigFile )
        if not localCfg.isSection( '/LocalSite' ):
          localCfg.createNewSection( '/LocalSite' )
        localCfg.setOption( '/LocalSite/CPUTimeLeft', self.timeLeft )
        localCfg.writeToFile( localConfigFile )

      else:
        return self.__finish( 'Filling Mode is Disabled' )

    self.log.verbose( 'Job Agent execution loop' )
    available = self.computingElement.available()
    if not available['OK'] or not available['Value']:
      self.log.info( 'Resource is not available' )
      self.log.info( available['Message'] )
      return self.__finish( 'CE Not Available' )

    self.log.info( available['Message'] )

    result = self.computingElement.getDescription()
    if not result['OK']:
      return result
    ceDict = result['Value']

    # Add pilot information
    gridCE = gConfig.getValue( 'LocalSite/GridCE', 'Unknown' )
    if gridCE != 'Unknown':
      ceDict['GridCE'] = gridCE
    if not 'PilotReference' in ceDict:
      ceDict['PilotReference'] = str( self.pilotReference )
    ceDict['PilotBenchmark'] = self.cpuFactor
    ceDict['PilotInfoReportedFlag'] = self.pilotInfoReportedFlag

    # Add possible job requirements
    result = gConfig.getOptionsDict( '/AgentJobRequirements' )
    if result['OK']:
      requirementsDict = result['Value']
      ceDict.update( requirementsDict )
      self.log.info( 'Requirements:', requirementsDict )

    processors, wholeNode = self.__getProcessors()
    ceDict['Processors'] = processors
    ceDict['WholeNode'] = wholeNode
    self.log.info( 'Configured number of processors: %d, WholeNode: %s' % ( processors, wholeNode ) )

    self.log.verbose( ceDict )
    start = time.time()
    jobRequest = self.__requestJob( ceDict )
    matchTime = time.time() - start
    self.log.info( 'MatcherTime = %.2f (s)' % ( matchTime ) )

    self.stopAfterFailedMatches = self.am_getOption( 'StopAfterFailedMatches', self.stopAfterFailedMatches )

    if not jobRequest['OK']:
      if re.search( 'No match found', jobRequest['Message'] ):
        self.log.notice( 'Job request OK: %s' % ( jobRequest['Message'] ) )
        self.matchFailedCount += 1
        if self.matchFailedCount > self.stopAfterFailedMatches:
          return self.__finish( 'Nothing to do for more than %d cycles' % self.stopAfterFailedMatches )
        return S_OK( jobRequest['Message'] )
      elif jobRequest['Message'].find( "seconds timeout" ) != -1:
        self.log.error( 'Timeout while requesting job', jobRequest['Message'] )
        self.matchFailedCount += 1
        if self.matchFailedCount > self.stopAfterFailedMatches:
          return self.__finish( 'Nothing to do for more than %d cycles' % self.stopAfterFailedMatches )
        return S_OK( jobRequest['Message'] )
      elif jobRequest['Message'].find( "Pilot version does not match" ) != -1 :
        errorMsg = 'Pilot version does not match the production version'
        self.log.error( errorMsg, jobRequest['Message'].replace( errorMsg, '' ) )
        return S_ERROR( jobRequest['Message'] )
      else:
        self.log.notice( 'Failed to get jobs: %s' % ( jobRequest['Message'] ) )
        self.matchFailedCount += 1
        if self.matchFailedCount > self.stopAfterFailedMatches:
          return self.__finish( 'Nothing to do for more than %d cycles' % self.stopAfterFailedMatches )
        return S_OK( jobRequest['Message'] )

    # Reset the Counter
    self.matchFailedCount = 0

    matcherInfo = jobRequest['Value']
    if not self.pilotInfoReportedFlag:
      # Check the flag after the first access to the Matcher
      self.pilotInfoReportedFlag = matcherInfo.get( 'PilotInfoReportedFlag', False )
    jobID = matcherInfo['JobID']
    matcherParams = ['JDL', 'DN', 'Group']
    for param in matcherParams:
      if param not in matcherInfo:
        self.__report( jobID, 'Failed', 'Matcher did not return %s' % ( param ) )
        return self.__finish( 'Matcher Failed' )
      elif not matcherInfo[param]:
        self.__report( jobID, 'Failed', 'Matcher returned null %s' % ( param ) )
        return self.__finish( 'Matcher Failed' )
      else:
        self.log.verbose( 'Matcher returned %s = %s ' % ( param, matcherInfo[param] ) )

    jobJDL = matcherInfo['JDL']
    jobGroup = matcherInfo['Group']
    ownerDN = matcherInfo['DN']

    optimizerParams = {}
    for key in matcherInfo:
      if key not in matcherParams:
        optimizerParams[key] = matcherInfo[key]

    parameters = self.__getJDLParameters( jobJDL )
    if not parameters['OK']:
      self.__report( jobID, 'Failed', 'Could Not Extract JDL Parameters' )
      self.log.warn( parameters['Message'] )
      return self.__finish( 'JDL Problem' )

    params = parameters['Value']
    if 'JobID' not in params:
      msg = 'Job has not JobID defined in JDL parameters'
      self.__report( jobID, 'Failed', msg )
      self.log.warn( msg )
      return self.__finish( 'JDL Problem' )
    else:
      jobID = params['JobID']

    if 'JobType' not in params:
      self.log.warn( 'Job has no JobType defined in JDL parameters' )
      jobType = 'Unknown'
    else:
      jobType = params['JobType']

    if 'CPUTime' not in params:
      self.log.warn( 'Job has no CPU requirement defined in JDL parameters' )

    if self.extraOptions:
      params['Arguments'] += ' ' + self.extraOptions
      params['ExtraOptions'] = self.extraOptions

    self.log.verbose( 'Job request successful: \n', jobRequest['Value'] )
    self.log.info( 'Received JobID=%s, JobType=%s' % ( jobID, jobType ) )
    self.log.info( 'OwnerDN: %s JobGroup: %s' % ( ownerDN, jobGroup ) )
    self.jobCount += 1
    try:
      jobReport = JobReport( jobID, 'JobAgent@%s' % self.siteName )
      jobReport.setJobParameter( 'MatcherServiceTime', str( matchTime ), sendFlag = False )

      if 'BOINC_JOB_ID' in os.environ:
        # Report BOINC environment
        for p in ( 'BoincUserID', 'BoincHostID', 'BoincHostPlatform', 'BoincHostName' ):
          jobReport.setJobParameter( p, gConfig.getValue( '/LocalSite/%s' % p, 'Unknown' ), sendFlag = False )

      jobReport.setJobStatus( 'Matched', 'Job Received by Agent' )
      result = self.__setupProxy( ownerDN, jobGroup )
      if not result[ 'OK' ]:
        return self.__rescheduleFailedJob( jobID, result[ 'Message' ], self.stopOnApplicationFailure )
      proxyChain = result.get( 'Value' )

      # Save the job jdl for external monitoring
      self.__saveJobJDLRequest( jobID, jobJDL )

      software = self.__checkInstallSoftware( jobID, params, ceDict )
      if not software['OK']:
        self.log.error( 'Failed to install software for job', '%s' % ( jobID ) )
        errorMsg = software['Message']
        if not errorMsg:
          errorMsg = 'Failed software installation'
        return self.__rescheduleFailedJob( jobID, errorMsg, self.stopOnApplicationFailure )

      self.log.debug( 'Before %sCE submitJob()' % ( self.ceName ) )
      submission = self.__submitJob( jobID, params, ceDict, optimizerParams, proxyChain )
      if not submission['OK']:
        self.__report( jobID, 'Failed', submission['Message'] )
        return self.__finish( submission['Message'] )
      elif 'PayloadFailed' in submission:
        # Do not keep running and do not overwrite the Payload error
        message = 'Payload execution failed with error code %s' % submission['PayloadFailed']
        if self.stopOnApplicationFailure:
          return self.__finish( message, self.stopOnApplicationFailure )
        else:
          self.log.info( message )

      self.log.debug( 'After %sCE submitJob()' % ( self.ceName ) )
    except Exception:
      self.log.exception()
      return self.__rescheduleFailedJob( jobID , 'Job processing failed with exception', self.stopOnApplicationFailure )

    # Sum all times but the last one (elapsed_time) and remove times at init (is this correct?)
    cpuTime = sum( os.times()[:-1] ) - sum( self.initTimes[:-1] )

    result = self.timeLeftUtil.getTimeLeft( cpuTime, processors )
    if result['OK']:
      self.timeLeft = result['Value']
    else:
      if result['Message'] != 'Current batch system is not supported':
        self.timeLeftError = result['Message']
      else:
        # if the batch system is not defined, use the process time and the CPU normalization defined locally
        self.timeLeft = self.__getCPUTimeLeft()

    scaledCPUTime = self.timeLeftUtil.getScaledCPU( processors )
    self.__setJobParam( jobID, 'ScaledCPUTime', str( scaledCPUTime - self.scaledCPUTime ) )
    self.scaledCPUTime = scaledCPUTime

    return S_OK( 'Job Agent cycle complete' )

  #############################################################################
  def __saveJobJDLRequest( self, jobID, jobJDL ):
    """Save job JDL local to JobAgent.
    """
    classAdJob = ClassAd( jobJDL )
    classAdJob.insertAttributeString( 'LocalCE', self.ceName )
    jdlFileName = jobID + '.jdl'
    jdlFile = open( jdlFileName, 'w' )
    jdl = classAdJob.asJDL()
    jdlFile.write( jdl )
    jdlFile.close()

  #############################################################################
  def __getCPUTimeLeft( self ):
    """Return the TimeLeft as estimated by DIRAC using the Normalization Factor in the Local Config.
    """
    cpuTime = sum( os.times()[:-1] )
    self.log.info( 'Current raw CPU time consumed is %s' % cpuTime )
    timeleft = self.timeLeft
    if self.cpuFactor:
      timeleft -= cpuTime * self.cpuFactor
    return timeleft

  #############################################################################
  def __setupProxy( self, ownerDN, ownerGroup ):
    """
    Retrieve a proxy for the execution of the job
    """
    if gConfig.getValue( '/DIRAC/Security/UseServerCertificate' , False ):
      proxyResult = self.__requestProxyFromProxyManager( ownerDN, ownerGroup )
      if not proxyResult['OK']:
        self.log.error( 'Failed to setup proxy', proxyResult['Message'] )
        return S_ERROR( 'Failed to setup proxy: %s' % proxyResult[ 'Message' ] )
      return S_OK( proxyResult['Value'] )
    else:
      ret = getProxyInfo( disableVOMS = True )
      if not ret['OK']:
        self.log.error( 'Invalid Proxy', ret['Message'] )
        return S_ERROR( 'Invalid Proxy' )

      proxyChain = ret['Value']['chain']
      if not 'groupProperties' in ret['Value']:
        print ret['Value']
        print proxyChain.dumpAllToString()
        self.log.error( 'Invalid Proxy', 'Group has no properties defined' )
        return S_ERROR( 'Proxy has no group properties defined' )

      groupProps = ret['Value']['groupProperties']
      if Properties.GENERIC_PILOT in groupProps or Properties.PILOT in groupProps:
        proxyResult = self.__requestProxyFromProxyManager( ownerDN, ownerGroup )
        if not proxyResult['OK']:
          self.log.error( 'Invalid Proxy', proxyResult['Message'] )
          return S_ERROR( 'Failed to setup proxy: %s' % proxyResult[ 'Message' ] )
        proxyChain = proxyResult['Value']

    return S_OK( proxyChain )

  #############################################################################
  def __requestProxyFromProxyManager( self, ownerDN, ownerGroup ):
    """Retrieves user proxy with correct role for job and sets up environment to
       run job locally.
    """

    self.log.info( "Requesting proxy for %s@%s" % ( ownerDN, ownerGroup ) )
    token = gConfig.getValue( "/Security/ProxyToken", "" )
    if not token:
      self.log.info( "No token defined. Trying to download proxy without token" )
      token = False
    retVal = gProxyManager.getPayloadProxyFromDIRACGroup( ownerDN, ownerGroup,
                                                          self.defaultProxyLength, token )
    if not retVal[ 'OK' ]:
      self.log.error( 'Could not retrieve payload proxy', retVal['Message'] )
      self.log.warn( retVal )
      os.system( 'dirac-proxy-info' )
      sys.stdout.flush()
      return S_ERROR( 'Error retrieving proxy' )

    chain = retVal[ 'Value' ]
    return S_OK( chain )

  #############################################################################
  def __checkInstallSoftware( self, jobID, jobParams, resourceParams ):
    """Checks software requirement of job and whether this is already present
       before installing software locally.
    """
    if 'SoftwareDistModule' not in jobParams:
      msg = 'Job has no software installation requirement'
      self.log.verbose( msg )
      return S_OK( msg )

    self.__report( jobID, 'Matched', 'Installing Software' )
    softwareDist = jobParams['SoftwareDistModule']
    self.log.verbose( 'Found VO Software Distribution module: %s' % ( softwareDist ) )
    argumentsDict = {'Job':jobParams, 'CE':resourceParams}
    moduleFactory = ModuleFactory()
    moduleInstance = moduleFactory.getModule( softwareDist, argumentsDict )
    if not moduleInstance['OK']:
      return moduleInstance

    module = moduleInstance['Value']
    return module.execute()

  #############################################################################
  def __submitJob( self, jobID, jobParams, resourceParams, optimizerParams, proxyChain ):
    """ Submit job to the Computing Element instance after creating a custom
        Job Wrapper with the available job parameters.
    """
    logLevel = self.am_getOption( 'DefaultLogLevel', 'INFO' )
    defaultWrapperLocation = self.am_getOption( 'JobWrapperTemplate',
                                                'DIRAC/WorkloadManagementSystem/JobWrapper/JobWrapperTemplate.py' )
    result = createJobWrapper( jobID, jobParams, resourceParams, optimizerParams,
                               extraOptions = self.extraOptions, defaultWrapperLocation = defaultWrapperLocation,
                               log = self.log, logLevel = logLevel )
    if not result['OK']:
      return result

    wrapperFile = result['Value']
    self.__report( jobID, 'Matched', 'Submitted To CE' )

    self.log.info( 'Submitting JobWrapper %s to %sCE' % ( os.path.basename( wrapperFile ), self.ceName ) )

    # Pass proxy to the CE
    proxy = proxyChain.dumpAllToString()
    if not proxy['OK']:
      self.log.error( 'Invalid proxy', proxy )
      return S_ERROR( 'Payload Proxy Not Found' )

    payloadProxy = proxy['Value']
    # FIXME: how can we set the batchID before we submit, this makes no sense
    batchID = 'dc%s' % ( jobID )
    submission = self.computingElement.submitJob( wrapperFile, payloadProxy )

    ret = S_OK( 'Job submitted' )

    if submission['OK']:
      batchID = submission['Value']
      self.log.info( 'Job %s submitted as %s' % ( jobID, batchID ) )
      self.log.verbose( 'Set JobParameter: Local batch ID %s' % ( batchID ) )
      self.__setJobParam( jobID, 'LocalBatchID', str( batchID ) )
      if 'PayloadFailed' in submission:
        ret['PayloadFailed'] = submission['PayloadFailed']
        return ret
      time.sleep( self.jobSubmissionDelay )
    else:
      self.log.error( 'Job submission failed', jobID )
      self.__setJobParam( jobID, 'ErrorMessage', '%s CE Submission Error' % ( self.ceName ) )
      if 'ReschedulePayload' in submission:
        rescheduleFailedJob( jobID, submission['Message'] )
        return S_OK()  # Without this job is marked as failed at line 265 above
      else:
        if 'Value' in submission:
          self.log.error( 'Error in DIRAC JobWrapper:', 'exit code = %s' % ( str( submission['Value'] ) ) )
      return S_ERROR( '%s CE Error: %s' % ( self.ceName, submission['Message'] ) )

    return ret

  #############################################################################
  def __requestJob( self, ceDict ):
    """Request a single job from the matcher service.
    """
    matcher = RPCClient( 'WorkloadManagement/Matcher', timeout = 600 )
    return matcher.requestJob( ceDict )

  #############################################################################
  def __getJDLParameters( self, jdl ):
    """Returns a dictionary of JDL parameters.
    """
    try:
      parameters = {}
#      print jdl
      if not re.search( r'\[', jdl ):
        jdl = '[' + jdl + ']'
      classAdJob = ClassAd( jdl )
      paramsDict = classAdJob.contents
      for param, value in paramsDict.items():
        if value.strip().startswith( '{' ):
          self.log.debug( 'Found list type parameter %s' % ( param ) )
          rawValues = value.replace( '{', '' ).replace( '}', '' ).replace( '"', '' ).split()
          valueList = []
          for val in rawValues:
            if re.search( ',$', val ):
              valueList.append( val[:-1] )
            else:
              valueList.append( val )
          parameters[param] = valueList
        else:
          parameters[param] = value.replace( '"', '' ).replace( '{', '"{' ).replace( '}', '}"' )
          self.log.debug( 'Found standard parameter %s: %s' % ( param, parameters[param] ) )
      return S_OK( parameters )
    except Exception as x:
      self.log.exception( lException = x )
      return S_ERROR( 'Exception while extracting JDL parameters for job' )

  #############################################################################
  def __report( self, jobID, status, minorStatus ):
    """Wraps around setJobStatus of state update client
    """
    jobReport = RPCClient( 'WorkloadManagement/JobStateUpdate' )
    jobStatus = jobReport.setJobStatus( int( jobID ), status, minorStatus, 'JobAgent@%s' % self.siteName )
    self.log.verbose( 'setJobStatus(%s,%s,%s,%s)' % ( jobID, status, minorStatus, 'JobAgent@%s' % self.siteName ) )
    if not jobStatus['OK']:
      self.log.warn( jobStatus['Message'] )

    return jobStatus

  #############################################################################
  # FIXME: this is not called anywhere...?
  def __setJobSite( self, jobID, site ):
    """Wraps around setJobSite of state update client
    """
    jobReport = RPCClient( 'WorkloadManagement/JobStateUpdate' )
    jobSite = jobReport.setJobSite( int( jobID ), site )
    self.log.verbose( 'setJobSite(%s,%s)' % ( jobID, site ) )
    if not jobSite['OK']:
      self.log.warn( jobSite['Message'] )

    return jobSite

  #############################################################################
  def __setJobParam( self, jobID, name, value ):
    """Wraps around setJobParameter of state update client
    """
    jobReport = RPCClient( 'WorkloadManagement/JobStateUpdate' )
    jobParam = jobReport.setJobParameter( int( jobID ), str( name ), str( value ) )
    self.log.verbose( 'setJobParameter(%s,%s,%s)' % ( jobID, name, value ) )
    if not jobParam['OK']:
      self.log.warn( jobParam['Message'] )

    return jobParam

  #############################################################################
  def __finish( self, message, stop = True ):
    """Force the JobAgent to complete gracefully.
    """
    if stop:
      self.log.info( 'JobAgent will stop with message "%s", execution complete.' % message )
      self.am_stopExecution()
      return S_ERROR( message )
    else:
      return S_OK( message )

  #############################################################################
  def __rescheduleFailedJob( self, jobID, message, stop = True ):
    """
    Set Job Status to "Rescheduled" and issue a reschedule command to the Job Manager
    """

    self.log.warn( 'Failure during %s' % ( message ) )

    jobManager = RPCClient( 'WorkloadManagement/JobManager' )
    jobReport = JobReport( int( jobID ), 'JobAgent@%s' % self.siteName )

    # Setting a job parameter does not help since the job will be rescheduled,
    # instead set the status with the cause and then another status showing the
    # reschedule operation.

    jobReport.setJobStatus( status = 'Rescheduled',
                            application = message,
                            sendFlag = True )

    self.log.info( 'Job will be rescheduled' )
    result = jobManager.rescheduleJob( jobID )
    if not result['OK']:
      self.log.error( 'Failed to reschedule job', result['Message'] )
      return self.__finish( 'Problem Rescheduling Job', stop )

    self.log.info( 'Job Rescheduled %s' % ( jobID ) )
    return self.__finish( 'Job Rescheduled', stop )

  #############################################################################
  def __getProcessors( self ):
    """
    Return number of processors from gConfig and a boolean corresponding to WholeNode option
    """
    tag = gConfig.getValue( '/Resources/Computing/CEDefaults/Tag', None )

    if tag is None: return 1, False

    self.log.verbose( "__getProcessors: /Resources/Computing/CEDefaults/Tag", repr( tag ) )

    # look for a pattern like "12345Processors" in tag list
    m = re.match( r'^(.*\D)?(?P<processors>\d+)Processors([ \t,].*)?$', tag )
    if m:
      return int( m.group( 'processors' ) ), False

    # In WholeNode case, detect number of cores from the host
    if re.match( r'^(.*,\s*)?WholeNode([ \t,].*)?$', tag ):
      return getNumberOfCores(), True

    return 1, False

  #############################################################################
  def finalize( self ):
    """ Job Agent finalization method
    """

    gridCE = gConfig.getValue( '/LocalSite/GridCE', '' )
    queue = gConfig.getValue( '/LocalSite/CEQueue', '' )
    wmsAdmin = RPCClient( 'WorkloadManagement/WMSAdministrator' )
    result = wmsAdmin.setPilotStatus( str( self.pilotReference ), 'Done', gridCE,
                                      'Report from JobAgent', self.siteName, queue )
    if not result['OK']:
      self.log.warn( result['Message'] )

    return S_OK()

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
