########################################################################
# $HeadURL$
# File :   JobAgent.py
# Author : Stuart Paterson
########################################################################
"""
  The Job Agent class instantiates a CE that acts as a client to a
  compute resource and also to the WMS.
  The Job Agent constructs a classAd based on the local resource description in the CS
  and the current resource status that is used for matching.
"""
__RCSID__ = "$Id$"

from DIRAC.Core.Utilities.ModuleFactory                     import ModuleFactory
from DIRAC.Core.Utilities.ClassAd.ClassAdLight              import ClassAd
from DIRAC.Core.Utilities.TimeLeft.TimeLeft                 import TimeLeft
from DIRAC.Core.Base.AgentModule                            import AgentModule
from DIRAC.Core.DISET.RPCClient                             import RPCClient
from DIRAC.Resources.Computing.ComputingElementFactory      import ComputingElementFactory
from DIRAC                                                  import S_OK, S_ERROR, gConfig, rootPath
from DIRAC.FrameworkSystem.Client.ProxyManagerClient        import gProxyManager
from DIRAC.Core.Security.ProxyInfo                          import getProxyInfo
from DIRAC.Core.Security                                    import Properties
from DIRAC.WorkloadManagementSystem.Client.JobReport        import JobReport
from DIRAC.WorkloadManagementSystem.JobWrapper.JobWrapper   import rescheduleFailedJob, AccountingJob
from DIRAC.Core.Utilities.CFG                               import CFG

import os, sys, re, time

class JobAgent( AgentModule ):
  """
      The specific agents must provide the following methods:
      - initialize() for initial settings
      - beginExecution()
      - execute() - the main method called in the agent cycle
      - endExecution()
      - finalize() - the graceful exit of the method, this one is usually used
                 for the agent restart
  """

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

    ceFactory = ComputingElementFactory()
    self.ceName = ceType
    ceInstance = ceFactory.getCE( ceType )
    if not ceInstance['OK']:
      self.log.warn( ceInstance['Message'] )
      return ceInstance

    self.initTimes = os.times()

    self.computingElement = ceInstance['Value']
    self.diracRoot = os.path.dirname( os.path.dirname( os.path.dirname( os.path.dirname( __file__ ) ) ) )
    #Localsite options
    self.siteRoot = gConfig.getValue( '/LocalSite/Root', os.getcwd() )
    self.siteName = gConfig.getValue( '/LocalSite/Site', 'Unknown' )
    self.pilotReference = gConfig.getValue( '/LocalSite/PilotReference', 'Unknown' )
    self.defaultProxyLength = gConfig.getValue( '/Registry/DefaultProxyLifeTime', 86400 * 5 )
    #Agent options
    # This is the factor to convert raw CPU to Normalized units (based on the CPU Model)
    self.cpuFactor = gConfig.getValue( '/LocalSite/CPUNormalizationFactor', 0.0 )
    defaultWrapperLocation = 'DIRAC/WorkloadManagementSystem/JobWrapper/JobWrapperTemplate.py'
    self.jobWrapperTemplate = os.path.join( self.diracRoot,
                                            self.am_getOption( 'JobWrapperTemplate',
                                                                defaultWrapperLocation ) )
    self.jobSubmissionDelay = self.am_getOption( 'SubmissionDelay', 10 )
    self.defaultLogLevel = self.am_getOption( 'DefaultLogLevel', 'info' )
    self.fillingMode = self.am_getOption( 'FillingModeFlag', False )
    self.stopOnApplicationFailure = self.am_getOption( 'StopOnApplicationFailure', True )
    self.stopAfterFailedMatches = self.am_getOption( 'StopAfterFailedMatches', 10 )
    self.jobCount = 0
    self.matchFailedCount = 0
    #Timeleft
    self.timeLeftUtil = TimeLeft()
    self.timeLeft = gConfig.getValue( '/Resources/Computing/CEDefaults/MaxCPUTime', 0.0 )
    self.gridCEQueue = gConfig.getValue( '/Resources/Computing/CEDefaults/GridCEQueue', '' )
    self.timeLeftError = ''
    self.scaledCPUTime = 0.0
    self.pilotInfoReportedFlag = False
    return S_OK()

  #############################################################################
  def execute( self ):
    """The JobAgent execution method.
    """
    if self.jobCount:
      #Only call timeLeft utility after a job has been picked up
      self.log.info( 'Attempting to check CPU time left for filling mode' )
      if self.fillingMode:
        if self.timeLeftError:
          self.log.warn( self.timeLeftError )
          return self.__finish( self.timeLeftError )
        self.log.info( '%s normalized CPU units remaining in slot' % ( self.timeLeft ) )
        # Need to update the Configuration so that the new value is published in the next matching request
        result = self.computingElement.setCPUTimeLeft( cpuTimeLeft = self.timeLeft )
        if not result['OK']:
          return self.__finish( result['Message'] )
        
        # Update local configuration to be used by submitted job wrappers
        localCfg = CFG()
        localConfigFile = os.path.join( rootPath, "etc", "dirac.cfg" )
        localCfg.loadFromFile( localConfigFile )
        if not localCfg.isSection('/LocalSite'):
          localCfg.createNewSection('/LocalSite')
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
        self.log.error( jobRequest['Message'] )
        self.matchFailedCount += 1
        if self.matchFailedCount > self.stopAfterFailedMatches:
          return self.__finish( 'Nothing to do for more than %d cycles' % self.stopAfterFailedMatches )
        return S_OK( jobRequest['Message'] )
      elif jobRequest['Message'].find( "Pilot version does not match" ) != -1 :
        self.log.error( jobRequest['Message'] )
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
    jobID = matcherInfo['JobID']
    self.pilotInfoReportedFlag = matcherInfo.get( 'PilotInfoReportedFlag', False )
    matcherParams = ['JDL', 'DN', 'Group']
    for param in matcherParams:
      if not matcherInfo.has_key( param ):
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
    for key in matcherInfo.keys():
      if not key in matcherParams:
        value = matcherInfo[key]
        optimizerParams[key] = value

    parameters = self.__getJDLParameters( jobJDL )
    if not parameters['OK']:
      self.__report( jobID, 'Failed', 'Could Not Extract JDL Parameters' )
      self.log.warn( parameters['Message'] )
      return self.__finish( 'JDL Problem' )

    params = parameters['Value']
    if not params.has_key( 'JobID' ):
      msg = 'Job has not JobID defined in JDL parameters'
      self.__report( jobID, 'Failed', msg )
      self.log.warn( msg )
      return self.__finish( 'JDL Problem' )
    else:
      jobID = params['JobID']

    if not params.has_key( 'JobType' ):
      self.log.warn( 'Job has no JobType defined in JDL parameters' )
      jobType = 'Unknown'
    else:
      jobType = params['JobType']

    if not params.has_key( 'CPUTime' ):
      self.log.warn( 'Job has no CPU requirement defined in JDL parameters' )

    self.log.verbose( 'Job request successful: \n %s' % ( jobRequest['Value'] ) )
    self.log.info( 'Received JobID=%s, JobType=%s' % ( jobID, jobType ) )
    self.log.info( 'OwnerDN: %s JobGroup: %s' % ( ownerDN, jobGroup ) )
    self.jobCount += 1
    try:
      jobReport = JobReport( jobID, 'JobAgent@%s' % self.siteName )
      jobReport.setJobParameter( 'MatcherServiceTime', str( matchTime ), sendFlag = False )
      if self.gridCEQueue:
        jobReport.setJobParameter( 'GridCEQueue', self.gridCEQueue, sendFlag = False )

      if os.environ.has_key( 'BOINC_JOB_ID' ):
        # Report BOINC environment
        for p in ['BoincUserID', 'BoincHostID', 'BoincHostPlatform', 'BoincHostName']:
          jobReport.setJobParameter( p, gConfig.getValue( '/LocalSite/%s' % p, 'Unknown' ), sendFlag = False )

      jobReport.setJobStatus( 'Matched', 'Job Received by Agent' )
      # self.__setJobSite( jobID, self.siteName )
      if not self.pilotInfoReportedFlag:
        self.__reportPilotInfo( jobID )
      result = self.__setupProxy( ownerDN, jobGroup )
      if not result[ 'OK' ]:
        return self.__rescheduleFailedJob( jobID, result[ 'Message' ], params, self.stopOnApplicationFailure )
      if 'Value' in result and result[ 'Value' ]:
        proxyChain = result[ 'Value' ]

      software = self.__checkInstallSoftware( jobID, params, ceDict )
      if not software['OK']:
        self.log.error( 'Failed to install software for job %s' % ( jobID ) )
        errorMsg = software['Message']
        if not errorMsg:
          errorMsg = 'Failed software installation'
        return self.__rescheduleFailedJob( jobID, errorMsg, params, self.stopOnApplicationFailure )

      self.log.verbose( 'Before %sCE submitJob()' % ( self.ceName ) )
      submission = self.__submitJob( jobID, params, ceDict, optimizerParams, jobJDL, proxyChain )
      if not submission['OK']:
        self.__report( jobID, 'Failed', submission['Message'] )
        return self.__finish( submission['Message'] )
      elif 'PayloadFailed' in submission:
        # Do not keep running and do not overwrite the Payload error
        return self.__finish( 'Payload execution failed with error code %s' % submission['PayloadFailed'],
                              self.stopOnApplicationFailure )

      self.log.verbose( 'After %sCE submitJob()' % ( self.ceName ) )
    except Exception:
      self.log.exception()
      return self.__rescheduleFailedJob( jobID , 'Job processing failed with exception',
                                         params, self.stopOnApplicationFailure )

    currentTimes = list( os.times() )
    for i in range( len( currentTimes ) ):
      currentTimes[i] -= self.initTimes[i]

    utime, stime, cutime, cstime, _elapsed = currentTimes
    cpuTime = utime + stime + cutime + cstime

    result = self.timeLeftUtil.getTimeLeft( cpuTime )
    if result['OK']:
      self.timeLeft = result['Value']
    else:
      if result['Message'] != 'Current batch system is not supported':
        self.timeLeftError = result['Message']
      else:
        if self.cpuFactor:
          # if the batch system is not defined used the CPUNormalizationFactor
          # defined locally
          self.timeLeft = self.__getCPUTimeLeft()
    scaledCPUTime = self.timeLeftUtil.getScaledCPU()['Value']

    self.__setJobParam( jobID, 'ScaledCPUTime', str( scaledCPUTime - self.scaledCPUTime ) )
    self.scaledCPUTime = scaledCPUTime

    return S_OK( 'Job Agent cycle complete' )

  #############################################################################
  def __getCPUTimeLeft( self ):
    """Return the TimeLeft as estimated by DIRAC using the Normalization Factor in the Local Config.
    """
    utime, stime, cutime, _cstime, _elapsed = os.times()
    cpuTime = utime + stime + cutime
    self.log.info( 'Current raw CPU time consumed is %s' % cpuTime )
    timeleft = self.timeLeft - cpuTime * self.cpuFactor
    return timeleft

  #############################################################################
  def __changeProxy( self, oldProxy, newProxy ):
    """Can call glexec utility here to set uid or simply log the changeover
       of a proxy.
    """
    self.log.verbose( 'Log proxy change (to be instrumented)' )
    return S_OK()

  #############################################################################
  def __setupProxy( self, ownerDN, ownerGroup ):
    """
    Retrieve a proxy for the execution of the job
    """
    if gConfig.getValue( '/DIRAC/Security/UseServerCertificate' , False ):
      proxyResult = self.__requestProxyFromProxyManager( ownerDN, ownerGroup )
      if not proxyResult['OK']:
        self.log.error( 'Invalid Proxy', proxyResult['Message'] )
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
      self.log.error( 'Could not retrieve proxy' )
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
    if not jobParams.has_key( 'SoftwareDistModule' ):
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
  def __submitJob( self, jobID, jobParams, resourceParams, optimizerParams, jobJDL, proxyChain ):
    """Submit job to the Computing Element instance after creating a custom
       Job Wrapper with the available job parameters.
    """
    result = self.__createJobWrapper( jobID, jobParams, resourceParams, optimizerParams )

    if not result['OK']:
      return result

    wrapperData = result[ 'Value' ]
    wrapperFile = wrapperData[ 'execFile' ]
    self.__report( jobID, 'Matched', 'Submitted To CE' )

    wrapperName = os.path.basename( wrapperFile )
    self.log.info( 'Submitting %s to %sCE' % ( wrapperName, self.ceName ) )

    #Pass proxy to the CE
    proxy = proxyChain.dumpAllToString()
    if not proxy['OK']:
      self.log.error( proxy )
      return S_ERROR( 'Payload Proxy Not Found' )

    payloadProxy = proxy['Value']
    # FIXME: how can we set the batchID before we submit, this makes no sense
    batchID = 'dc%s' % ( jobID )
    submission = self.computingElement.submitJob( wrapperFile, payloadProxy, wrapperData )

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
        rescheduleFailedJob( jobID, submission['Message'], self.__report )
      else:
        if 'Value' in submission:
          self.log.error( 'Error in DIRAC JobWrapper:', 'exit code = %s' % ( str( submission['Value'] ) ) )
        # make sure the Job is declared Failed
        self.__report( jobID, 'Failed', submission['Message'] )
      return S_ERROR( '%s CE Submission Error: %s' % ( self.ceName, submission['Message'] ) )

    return ret

  #############################################################################
  def __createJobWrapper( self, jobID, jobParams, resourceParams, optimizerParams ):
    """This method creates a job wrapper filled with the CE and Job parameters
       to executed the job.
    """
    arguments = {'Job':jobParams,
                 'CE':resourceParams,
                 'Optimizer':optimizerParams}
    self.log.verbose( 'Job arguments are: \n %s' % ( arguments ) )

    workingDir = gConfig.getValue( '/LocalSite/WorkingDirectory', self.siteRoot )
    if not os.path.exists( '%s/job/Wrapper' % ( workingDir ) ):
      try:
        os.makedirs( '%s/job/Wrapper' % ( workingDir ) )
      except Exception:
        self.log.exception()
        return S_ERROR( 'Could not create directory for wrapper script' )

    jobWrapperFile = '%s/job/Wrapper/Wrapper_%s' % ( workingDir, jobID )
    if os.path.exists( jobWrapperFile ):
      self.log.verbose( 'Removing existing Job Wrapper for %s' % ( jobID ) )
      os.remove( jobWrapperFile )
    fd = open( self.jobWrapperTemplate, 'r' )
    wrapperTemplate = fd.read()
    fd.close()

    dateStr = time.strftime( "%Y-%m-%d", time.localtime( time.time() ) )
    timeStr = time.strftime( "%H:%M", time.localtime( time.time() ) )
    date_time = '%s %s' % ( dateStr, timeStr )
    signature = __RCSID__
    dPython = sys.executable

    logLevel = self.defaultLogLevel
    if jobParams.has_key( 'LogLevel' ):
      logLevel = jobParams['LogLevel']
      self.log.info( 'Found Job LogLevel JDL parameter with value: %s' % ( logLevel ) )
    else:
      self.log.info( 'Applying default LogLevel JDL parameter with value: %s' % ( logLevel ) )

    realPythonPath = os.path.realpath( dPython )
    self.log.debug( 'Real python path after resolving links is:' )
    self.log.debug( realPythonPath )
    dPython = realPythonPath

    siteRootPython = self.siteRoot
    self.log.debug( 'DIRACPython is:\n%s' % dPython )
    self.log.debug( 'SiteRootPythonDir is:\n%s' % siteRootPython )
    wrapperTemplate = wrapperTemplate.replace( "@SIGNATURE@", str( signature ) )
    wrapperTemplate = wrapperTemplate.replace( "@JOBID@", str( jobID ) )
    wrapperTemplate = wrapperTemplate.replace( "@DATESTRING@", str( date_time ) )
    wrapperTemplate = wrapperTemplate.replace( "@JOBARGS@", str( arguments ) )
    wrapperTemplate = wrapperTemplate.replace( "@SITEPYTHON@", str( siteRootPython ) )
    wrapper = open ( jobWrapperFile, "w" )
    wrapper.write( wrapperTemplate )
    wrapper.close ()
    jobExeFile = '%s/job/Wrapper/Job%s' % ( workingDir, jobID )
    jobFileContents = \
"""#!/bin/sh
%s %s -o LogLevel=%s -o /DIRAC/Security/UseServerCertificate=no
""" % ( dPython, jobWrapperFile, logLevel )
    jobFile = open( jobExeFile, 'w' )
    jobFile.write( jobFileContents )
    jobFile.close()

    data = { 'execFile' : jobExeFile,
             'wrapperTemplate' : self.jobWrapperTemplate,
             'signature': signature,
             'wrapperFile' : jobWrapperFile,
             'logLevel' : logLevel,
             'jid' : jobID,
             'datetime': date_time,
             'jobArgs' : arguments,
             'sitePython' : siteRootPython }
    return S_OK( data )

  #############################################################################
  def __requestJob( self, ceDict ):
    """Request a single job from the matcher service.
    """
    try:
      matcher = RPCClient( 'WorkloadManagement/Matcher', timeout = 600 )
      result = matcher.requestJob( ceDict )
      return result
    except Exception, x:
      self.log.exception( lException = x )
      return S_ERROR( 'Job request to matcher service failed with exception' )

  #############################################################################
  def __getJDLParameters( self, jdl ):
    """Returns a dictionary of JDL parameters.
    """
    try:
      parameters = {}
#      print jdl
      if not re.search( '\[', jdl ):
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
    except Exception, x:
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
  def __reportPilotInfo( self, jobID ):
    """Sends back useful information for the pilotAgentsDB via the WMSAdministrator
       service.
    """

    gridCE = gConfig.getValue( 'LocalSite/GridCE', 'Unknown' )

    wmsAdmin = RPCClient( 'WorkloadManagement/WMSAdministrator' )
    if gridCE != 'Unknown':
      result = wmsAdmin.setJobForPilot( int( jobID ), str( self.pilotReference ), gridCE )
    else:
      result = wmsAdmin.setJobForPilot( int( jobID ), str( self.pilotReference ) )

    if not result['OK']:
      self.log.warn( result['Message'] )

    result = wmsAdmin.setPilotBenchmark( str( self.pilotReference ), float( self.cpuFactor ) )
    if not result['OK']:
      self.log.warn( result['Message'] )

    return S_OK()

  #############################################################################
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
    self.log.info( 'JobAgent will stop with message "%s", execution complete.' % message )
    if stop:
      self.am_stopExecution()
      return S_ERROR( message )
    else:
      return S_OK( message )

  #############################################################################
  def __rescheduleFailedJob( self, jobID, message, jobParams, stop = True ):
    """
    Set Job Status to "Rescheduled" and issue a reschedule command to the Job Manager
    """
    jobReport = JobReport( int( jobID ), 'JobAgent@%s' % self.siteName )

    rescheduleResult = rescheduleFailedJob( jobID, message, jobReport )

    self.__sendAccountingRecord( rescheduleResult, message, jobParams )

    if rescheduleResult == 'Rescheduled':
      self.log.info( 'Job Rescheduled %s' % ( jobID ) )
      return self.__finish( 'Job Rescheduled', stop )

    return self.__finish( 'Problem Rescheduling Job', stop )

  #############################################################################
  def __sendAccountingRecord( self, status, minorStatus, jobParams ):
    """
    Send Accounting Record for Rescheduled Job
    """
    accountingReport = AccountingJob()
    #Set now as start time
    accountingReport.setStartTime()
    #Set now as end time
    accountingReport.setEndTime()
    #Fill the data
    acData = {
               'User' : jobParams.get( 'Owner', 'unknown' ),
               'UserGroup' : jobParams.get( 'OwnerGroup', 'unknown' ),
               'JobGroup' : jobParams.get( 'JobGroup', 'unknown' ),
               'JobType' : jobParams.get( 'JobType', 'unknown' ),
               'JobClass' : jobParams.get( 'JobSplitType', 'unknown' ),
               'ProcessingType' : jobParams.get( 'ProcessingType', 'unknown' ),
               'FinalMajorStatus' : status,
               'FinalMinorStatus' : minorStatus,
               'CPUTime' : 0.0,
               # Based on the factor to convert raw CPU to Normalized units (based on the CPU Model)
               'NormCPUTime' : 0.0,
               'ExecTime' : 0.0,
               'InputDataSize' : 0.0,
               'OutputDataSize' : 0.0,
               'InputDataFiles' : 0.0,
               'OutputDataFiles' : 0.0,
               'DiskSpace' : 0.0,
               'InputSandBoxSize' : 0.0,
               'OutputSandBoxSize' : 0.0,
               'ProcessedEvents' : 0.0
             }
    accountingReport.setValuesFromDict( acData )
    accountingReport.commit()


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

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
