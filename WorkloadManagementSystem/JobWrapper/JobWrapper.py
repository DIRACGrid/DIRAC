########################################################################
# $Id$
# File :   JobWrapper.py
# Author : Stuart Paterson
########################################################################

""" The Job Wrapper Class is instantiated with arguments tailored for running
    a particular job. The JobWrapper starts a thread for execution of the job
    and a Watchdog Agent that can monitor progress.
"""

__RCSID__ = "$Id$"

from DIRAC.DataManagementSystem.Client.ReplicaManager               import ReplicaManager
from DIRAC.DataManagementSystem.Client.FailoverTransfer             import FailoverTransfer
from DIRAC.Resources.Catalog.PoolXMLFile                            import getGUID
from DIRAC.RequestManagementSystem.Client.RequestContainer          import RequestContainer
from DIRAC.RequestManagementSystem.Client.RequestClient             import RequestClient
from DIRAC.WorkloadManagementSystem.Client.SandboxStoreClient       import SandboxStoreClient
from DIRAC.WorkloadManagementSystem.JobWrapper.WatchdogFactory      import WatchdogFactory
from DIRAC.AccountingSystem.Client.Types.Job                        import Job as AccountingJob
from DIRAC.ConfigurationSystem.Client.PathFinder                    import getSystemSection
from DIRAC.ConfigurationSystem.Client.Helpers.Registry              import getVOForGroup
from DIRAC.WorkloadManagementSystem.Client.JobReport                import JobReport
from DIRAC.Core.DISET.RPCClient                                     import RPCClient
from DIRAC.Core.Utilities.SiteSEMapping                             import getSEsForSite
from DIRAC.Core.Utilities.ModuleFactory                             import ModuleFactory
from DIRAC.Core.Utilities.Subprocess                                import systemCall
from DIRAC.Core.Utilities.Subprocess                                import Subprocess
from DIRAC.Core.Utilities.File                                      import getGlobbedTotalSize, getGlobbedFiles
from DIRAC.Core.Utilities.Version                                   import getCurrentVersion
from DIRAC.Core.Utilities                                           import List
from DIRAC                                                          import S_OK, S_ERROR, gConfig, gLogger, List, Time
from DIRAC.FrameworkSystem.Client.NotificationClient                import NotificationClient

import DIRAC

import os, re, sys, time, shutil, threading, tarfile, glob, types

EXECUTION_RESULT = {}

class JobWrapper:

  #############################################################################
  def __init__( self, jobID = None, jobReport = None ):
    """ Standard constructor
    """
    self.initialTiming = os.times()
    self.section = os.path.join( getSystemSection( 'WorkloadManagement/JobWrapper' ), 'JobWrapper' )
    self.log = gLogger
    #Create the accounting report
    self.accountingReport = AccountingJob()
    # Initialize for accounting
    self.wmsMajorStatus = "unknown"
    self.wmsMinorStatus = "unknown"
    #Set now as start time
    self.accountingReport.setStartTime()
    if not jobID:
      self.jobID = 0
    else:
      self.jobID = jobID
    self.siteName = gConfig.getValue( '/LocalSite/Site', 'Unknown' )
    if jobReport:
      self.jobReport = jobReport
    else:
      self.jobReport = JobReport( self.jobID, 'JobWrapper@%s' % self.siteName )

    # self.root is the path the Wrapper is running at
    self.root = os.getcwd()
    # self.localSiteRoot is the path where the local DIRAC installation used to run the payload
    # is taken from
    self.localSiteRoot = gConfig.getValue( '/LocalSite/Root', DIRAC.rootPath )
    # FIXME: Why do we need to load any .cfg file here????
    self.__loadLocalCFGFiles( self.localSiteRoot )
    result = getCurrentVersion()
    if result['OK']:
      self.diracVersion = result['Value']
    else:
      self.diracVersion = 'DIRAC version %s' % DIRAC.buildVersion
    self.maxPeekLines = gConfig.getValue( self.section + '/MaxJobPeekLines', 20 )
    if self.maxPeekLines < 0:
      self.maxPeekLines = 0
    self.defaultCPUTime = gConfig.getValue( self.section + '/DefaultCPUTime', 600 )
    self.defaultOutputFile = gConfig.getValue( self.section + '/DefaultOutputFile', 'std.out' )
    self.defaultErrorFile = gConfig.getValue( self.section + '/DefaultErrorFile', 'std.err' )
    self.diskSE = gConfig.getValue( self.section + '/DiskSE', ['-disk', '-DST', '-USER'] )
    self.tapeSE = gConfig.getValue( self.section + '/TapeSE', ['-tape', '-RDST', '-RAW'] )
    self.sandboxSizeLimit = gConfig.getValue( self.section + '/OutputSandboxLimit', 1024 * 1024 * 10 )
    self.cleanUpFlag = gConfig.getValue( self.section + '/CleanUpFlag', True )
    self.pilotRef = gConfig.getValue( '/LocalSite/PilotReference', 'Unknown' )
    self.cpuNormalizationFactor = gConfig.getValue ( "/LocalSite/CPUNormalizationFactor", 0.0 )
    self.bufferLimit = gConfig.getValue( self.section + '/BufferLimit', 10485760 )
    self.defaultOutputSE = gConfig.getValue( '/Resources/StorageElementGroups/SE-USER', [] )
    self.defaultCatalog = gConfig.getValue( self.section + '/DefaultCatalog', [] )
    self.defaultFailoverSE = gConfig.getValue( '/Resources/StorageElementGroups/Tier1-Failover', [] )
    self.defaultOutputPath = ''
    self.rm = ReplicaManager()
    self.log.verbose( '===========================================================================' )
    self.log.verbose( 'SVN version %s' % ( __RCSID__ ) )
    self.log.verbose( self.diracVersion )
    self.log.verbose( 'Developer tag: 2' )
    self.currentPID = os.getpid()
    self.log.verbose( 'Job Wrapper started under PID: %s' % self.currentPID )
    # Define a new process group for the job wrapper
    self.parentPGID = os.getpgid( self.currentPID )
    self.log.verbose( 'Job Wrapper parent process group ID: %s' % self.parentPGID )
    os.setpgid( self.currentPID, self.currentPID )
    self.currentPGID = os.getpgid( self.currentPID )
    self.log.verbose( 'Job Wrapper process group ID: %s' % self.currentPGID )
    self.log.verbose( '==========================================================================' )
    self.log.verbose( 'sys.path is: \n%s' % '\n'.join( sys.path ) )
    self.log.verbose( '==========================================================================' )
    if not os.environ.has_key( 'PYTHONPATH' ):
      self.log.verbose( 'PYTHONPATH is: null' )
    else:
      pypath = os.environ['PYTHONPATH']
      self.log.verbose( 'PYTHONPATH is: \n%s' % '\n'.join( pypath.split( ':' ) ) )
      self.log.verbose( '==========================================================================' )
    if os.environ.has_key( 'LD_LIBRARY_PATH_SAVE' ):
      if os.environ.has_key( 'LD_LIBRARY_PATH' ):
        os.environ['LD_LIBRARY_PATH'] = os.environ['LD_LIBRARY_PATH'] + ':' + os.environ['LD_LIBRARY_PATH_SAVE']
      else:
        os.environ['LD_LIBRARY_PATH'] = os.environ['LD_LIBRARY_PATH_SAVE']

    if not os.environ.has_key( 'LD_LIBRARY_PATH' ):
      self.log.verbose( 'LD_LIBRARY_PATH is: null' )
    else:
      ldpath = os.environ['LD_LIBRARY_PATH']
      self.log.verbose( 'LD_LIBRARY_PATH is: \n%s' % '\n'.join( ldpath.split( ':' ) ) )
      self.log.verbose( '==========================================================================' )
    if not self.cleanUpFlag:
      self.log.verbose( 'CleanUp Flag is disabled by configuration' )
    #Failure flag
    self.failedFlag = True
    #Set defaults for some global parameters to be defined for the accounting report
    self.owner = 'unknown'
    self.jobGroup = 'unknown'
    self.jobType = 'unknown'
    self.processingType = 'unknown'
    self.userGroup = 'unknown'
    self.jobClass = 'unknown'
    self.inputDataFiles = 0
    self.outputDataFiles = 0
    self.inputDataSize = 0
    self.inputSandboxSize = 0
    self.outputSandboxSize = 0
    self.outputDataSize = 0
    self.processedEvents = 0
    self.wmsAccountingSent = False

    self.jobArgs = {}
    self.optArgs = {}
    self.ceArgs = {}

  #############################################################################
  def initialize( self, arguments ):
    """ Initializes parameters and environment for job.
    """
    self.__report( 'Running', 'Job Initialization' )
    self.log.info( 'Starting Job Wrapper Initialization for Job %s' % ( self.jobID ) )
    self.jobArgs = arguments['Job']
    self.log.verbose( self.jobArgs )
    self.ceArgs = arguments ['CE']
    self.log.verbose( self.ceArgs )
    self.__setInitialJobParameters()
    if arguments.has_key( 'Optimizer' ):
      self.optArgs = arguments['Optimizer']
    else:
      self.optArgs = {}
    #Fill some parameters for the accounting report
    if self.jobArgs.has_key( 'Owner' ):
      self.owner = self.jobArgs['Owner']
    if self.jobArgs.has_key( 'JobGroup' ):
      self.jobGroup = self.jobArgs['JobGroup']
    if self.jobArgs.has_key( 'JobType' ):
      self.jobType = self.jobArgs['JobType']
    if self.jobArgs.has_key( 'InputData' ):
      dataParam = self.jobArgs['InputData']
      if dataParam and not type( dataParam ) == type( [] ):
        dataParam = [dataParam]
      self.inputDataFiles = len( dataParam )
    if self.jobArgs.has_key( 'OutputData' ):
      dataParam = self.jobArgs['OutputData']
      if dataParam and not type( dataParam ) == type( [] ):
        dataParam = [dataParam]
      self.outputDataFiles = len( dataParam )
    if self.jobArgs.has_key( 'ProcessingType' ):
      self.processingType = self.jobArgs['ProcessingType']
    if self.jobArgs.has_key( 'OwnerGroup' ):
      self.userGroup = self.jobArgs['OwnerGroup']
    if self.jobArgs.has_key( 'JobSplitType' ):
      self.jobClass = self.jobArgs['JobSplitType']

    # Prepare the working directory and cd to there
    if self.jobID:
      if os.path.exists( str( self.jobID ) ):
        shutil.rmtree( str( self.jobID ) )
      os.mkdir( str( self.jobID ) )
      os.chdir( str( self.jobID ) )
    else:
      self.log.info( 'JobID is not defined, running in current directory' )

    infoFile = open( 'job.info', 'w' )
    infoFile.write( self.__dictAsInfoString( self.jobArgs, '/Job' ) )
    infoFile.close()

  #############################################################################
  def __setInitialJobParameters( self ):
    """Sets some initial job parameters
    """
    parameters = []
    if self.ceArgs.has_key( 'LocalSE' ):
      parameters.append( ( 'AgentLocalSE', ','.join( self.ceArgs['LocalSE'] ) ) )
    if self.ceArgs.has_key( 'CompatiblePlatforms' ):
      parameters.append( ( 'AgentCompatiblePlatforms', ','.join( self.ceArgs['CompatiblePlatforms'] ) ) )
    if self.ceArgs.has_key( 'PilotReference' ):
      parameters.append( ( 'Pilot_Reference', self.ceArgs['PilotReference'] ) )
    if self.ceArgs.has_key( 'CPUScalingFactor' ):
      parameters.append( ( 'CPUScalingFactor', self.ceArgs['CPUScalingFactor'] ) )
    if self.ceArgs.has_key( 'CPUNormalizationFactor' ):
      parameters.append( ( 'CPUNormalizationFactor', self.ceArgs['CPUNormalizationFactor'] ) )

    parameters.append( ( 'PilotAgent', self.diracVersion ) )
    parameters.append( ( 'JobWrapperPID', self.currentPID ) )
    result = self.__setJobParamList( parameters )
    return result

  #############################################################################
  def __loadLocalCFGFiles( self, localRoot ):
    """Loads any extra CFG files residing in the local DIRAC site root.
    """
    files = os.listdir( localRoot )
    self.log.debug( 'Checking directory %s for *.cfg files' % localRoot )
    for i in files:
      if re.search( '.cfg$', i ):
        gConfig.loadFile( '%s/%s' % ( localRoot, i ) )
        self.log.debug( 'Found local .cfg file %s' % i )

  #############################################################################
  def __dictAsInfoString( self, dData, infoString = '', currentBase = "" ):
    for key in dData:
      value = dData[ key ]
      if type( value ) == types.DictType:
        infoString = self.__dictAsInfoString( value, infoString, "%s/%s" % ( currentBase, key ) )
      elif type( value ) in ( types.ListType, types.TupleType ):
        if len( value ) and value[0] == '[':
          infoString += "%s/%s = %s\n" % ( currentBase, key, " ".join( value ) )
        else:
          infoString += "%s/%s = %s\n" % ( currentBase, key, ", ".join( value ) )
      else:
        infoString += "%s/%s = %s\n" % ( currentBase, key, str( value ) )

    return infoString


  #############################################################################
  def execute( self, arguments ):
    """The main execution method of the Job Wrapper
    """
    self.log.info( 'Job Wrapper is starting execution phase for job %s' % ( self.jobID ) )
    os.environ['DIRACJOBID'] = str( self.jobID )
    os.environ['DIRACROOT'] = self.localSiteRoot
    self.log.verbose( 'DIRACROOT = %s' % ( self.localSiteRoot ) )
    os.environ['DIRACPYTHON'] = sys.executable
    self.log.verbose( 'DIRACPYTHON = %s' % ( sys.executable ) )
    os.environ['DIRACSITE'] = DIRAC.siteName()
    self.log.verbose( 'DIRACSITE = %s' % ( DIRAC.siteName() ) )

    outputFile = self.defaultOutputFile
    errorFile = self.defaultErrorFile
    if self.jobArgs.has_key( 'StdError' ):
      errorFile = self.jobArgs['StdError']
    if self.jobArgs.has_key( 'StdOutput' ):
      outputFile = self.jobArgs['StdOutput']

    if self.jobArgs.has_key( 'MaxCPUTime' ):
      jobCPUTime = int( self.jobArgs['MaxCPUTime'] )
    else:
      self.log.info( 'Job %s has no CPU time limit specified, '
                     'applying default of %s' % ( self.jobID, self.defaultCPUTime ) )
      jobCPUTime = self.defaultCPUTime

    if self.jobArgs.has_key( 'Executable' ):
      executable = self.jobArgs['Executable'].strip()
      #HACK: To be removed after SVN migration is successful
      if executable == "$DIRACROOT/scripts/jobexec":
        executable = "$DIRACROOT/scripts/dirac-jobexec"
      #END HACK
    else:
      msg = 'Job %s has no specified executable' % ( self.jobID )
      self.log.warn( msg )
      return S_ERROR( msg )

    jobArguments = ''
    if self.jobArgs.has_key( 'Arguments' ):
      jobArguments = self.jobArgs['Arguments']

    executable = os.path.expandvars( executable )
    exeThread = None
    spObject = None

    if re.search( 'DIRACROOT', executable ):
      executable = executable.replace( '$DIRACROOT', self.localSiteRoot )
      self.log.verbose( 'Replaced $DIRACROOT for executable as %s' % ( self.localSiteRoot ) )

    # Make the full path since . is not always in the PATH
    executable = os.path.abspath( executable )
    if not os.access( executable, os.X_OK ):
      try:
        os.chmod( executable, 0775 )
      except Exception:
        self.log.warn( 'Failed to change mode to 775 for the executable', executable )

    exeEnv = dict( os.environ )
    if self.jobArgs.has_key( 'ExecutionEnvironment' ):
      self.log.verbose( 'Adding variables to execution environment' )
      variableList = self.jobArgs['ExecutionEnvironment']
      if type( variableList ) == type( " " ):
        variableList = [variableList]
      for var in variableList:
        nameEnv = var.split( '=' )[0]
        valEnv = var.split( '=' )[1]
        exeEnv[nameEnv] = valEnv
        self.log.verbose( '%s = %s' % ( nameEnv, valEnv ) )

    if os.path.exists( executable ):
      self.__report( 'Running', 'Application', sendFlag = True )
      spObject = Subprocess( timeout = False, bufferLimit = int( self.bufferLimit ) )
      command = executable
      if jobArguments:
        command += ' ' + jobArguments
      self.log.verbose( 'Execution command: %s' % ( command ) )
      maxPeekLines = self.maxPeekLines
      exeThread = ExecutionThread( spObject, command, maxPeekLines, outputFile, errorFile, exeEnv )
      exeThread.start()
      time.sleep( 10 )
      payloadPID = spObject.getChildPID()
      if not payloadPID:
        return S_ERROR( 'Payload process could not start after 10 seconds' )
    else:
      self.__report( 'Failed', 'Application not found', sendFlag = True )
      return S_ERROR( 'Path to executable %s not found' % ( executable ) )

    self.__setJobParam( 'PayloadPID', payloadPID )

    watchdogFactory = WatchdogFactory()
    watchdogInstance = watchdogFactory.getWatchdog( self.currentPID, exeThread, spObject, jobCPUTime )
    if not watchdogInstance['OK']:
      self.log.warn( watchdogInstance['Message'] )
      return S_ERROR( 'Could not create Watchdog instance' )

    self.log.verbose( 'WatchdogInstance %s' % ( watchdogInstance ) )
    watchdog = watchdogInstance['Value']

    self.log.verbose( 'Initializing Watchdog instance' )
    watchdog.initialize()
    self.log.verbose( 'Calibrating Watchdog instance' )
    watchdog.calibrate()
    # do not kill SAM jobs by CPU time
    if self.jobArgs.has_key( 'JobType' ) and self.jobArgs['JobType'] == 'SAM':
      watchdog.testCPUConsumed = False

    if self.jobArgs.has_key( 'DisableCPUCheck' ):
      watchdog.testCPUConsumed = False

    if exeThread.isAlive():
      self.log.info( 'Application thread is started in Job Wrapper' )
      watchdog.run()
    else:
      self.log.warn( 'Application thread stopped very quickly...' )

    if exeThread.isAlive():
      self.log.warn( 'Watchdog exited before completion of execution thread' )
      while exeThread.isAlive():
        time.sleep( 5 )

    outputs = None
    if EXECUTION_RESULT.has_key( 'Thread' ):
      threadResult = EXECUTION_RESULT['Thread']
      if not threadResult['OK']:
        self.log.error( 'Failed to execute the payload', threadResult['Message'] )
        
        self.__report( 'Failed', 'Application failed, check job parameters', sendFlag = True )
        if 'Value' in threadResult:
          outputs = threadResult['Value']
        if outputs:
          self.__setJobParam( 'ApplicationError', outputs[-200:], sendFlag = True )
        else:
          self.__setJobParam( 'ApplicationError', 'None reported', sendFlag = True )
      else:
        outputs = threadResult['Value']

    if EXECUTION_RESULT.has_key( 'CPU' ):
      self.log.info( 'EXECUTION_RESULT[CPU] in JobWrapper execute', str( EXECUTION_RESULT['CPU'] ) )


    if watchdog.checkError:
      # In this case, the Watchdog has killed the Payload and the ExecutionThread can not get the CPU statistics
      # os.times only reports for waited children
      # Take the CPU from the last value recorded by the Watchdog
      self.__report( 'Failed', watchdog.checkError, sendFlag = True )
      if EXECUTION_RESULT.has_key( 'CPU' ):
        if 'LastUpdateCPU(s)' in watchdog.currentStats:
          EXECUTION_RESULT['CPU'][0] = 0
          EXECUTION_RESULT['CPU'][0] = 0
          EXECUTION_RESULT['CPU'][0] = 0
          EXECUTION_RESULT['CPU'][0] = watchdog.currentStats['LastUpdateCPU(s)']

    if watchdog.currentStats:
      self.log.info( 'Statistics collected by the Watchdog:\n ',
                        '\n  '.join( ['%s: %s' % items for items in watchdog.currentStats.items() ] ) )
    if outputs:
      status = threadResult['Value'][0]
      #Send final heartbeat of a configurable number of lines here
      self.log.verbose( 'Sending final application standard output heartbeat' )
      self.__sendFinalStdOut( exeThread )
      self.log.verbose( 'Execution thread status = %s' % ( status ) )

      if not watchdog.checkError and not status:
        self.failedFlag = False
        self.__report( 'Completed', 'Application Finished Successfully', sendFlag = True )
      elif not watchdog.checkError:
        self.__report( 'Completed', 'Application Finished With Errors', sendFlag = True )

    else:
      return S_ERROR( 'No outputs generated from job execution' )

    self.log.info( 'Checking directory contents after execution:' )
    res = systemCall( 5, ['ls', '-al'] )
    if not res['OK']:
      self.log.error( 'Failed to list the current directory', res['Message'] )
    elif res['Value'][0]:
      self.log.error( 'Failed to list the current directory', res['Value'][2] )
    else:
      # no timeout and exit code is 0
      self.log.info( res['Value'][1] )

    return S_OK()

  #############################################################################
  def __sendFinalStdOut( self, exeThread ):
    """After the Watchdog process has finished, this function sends a final
       report to be presented in the StdOut in the web page via the heartbeat
       mechanism.
    """
    cpuConsumed = self.__getCPU()['Value']
    self.log.info( 'Total CPU Consumed is: %s' % cpuConsumed[1] )
    self.__setJobParam( 'TotalCPUTime(s)', cpuConsumed[0] )
    normCPU = cpuConsumed[0] * self.cpuNormalizationFactor
    self.__setJobParam( 'NormCPUTime(s)', normCPU )
    if self.cpuNormalizationFactor:
      self.log.info( 'Normalized CPU Consumed is:', normCPU )

    result = exeThread.getOutput( self.maxPeekLines )
    if not result['OK']:
      lines = 0
      appStdOut = ''
    else:
      lines = len( result['Value'] )
      appStdOut = '\n'.join( result['Value'] )

    header = 'Last %s lines of application output from JobWrapper on %s :' % ( lines, Time.toString() )
    border = '=' * len( header )

    cpuTotal = 'CPU Total: %s (h:m:s)' % cpuConsumed[1]
    cpuTotal += " Normalized CPU Total %.1f s @ HEP'06" % normCPU
    header = '\n%s\n%s\n%s\n%s\n' % ( border, header, cpuTotal, border )
    appStdOut = header + appStdOut
    self.log.info( appStdOut )
    heartBeatDict = {}
    staticParamDict = {'StandardOutput':appStdOut}
    if self.jobID:
      jobReport = RPCClient( 'WorkloadManagement/JobStateUpdate', timeout = 120 )
      result = jobReport.sendHeartBeat( self.jobID, heartBeatDict, staticParamDict )
      if not result['OK']:
        self.log.error( 'Problem sending final heartbeat from JobWrapper', result['Message'] )

    return

  #############################################################################
  def __getCPU( self ):
    """Uses os.times() to get CPU time and returns HH:MM:SS after conversion.
    """
    #TODO: normalize CPU consumed via scale factor
    self.log.info( 'EXECUTION_RESULT[CPU] in __getCPU', str( EXECUTION_RESULT['CPU'] ) )
    utime, stime, cutime, cstime, elapsed = EXECUTION_RESULT['CPU']
    cpuTime = utime + stime + cutime + cstime
    self.log.verbose( "Total CPU time consumed = %s" % ( cpuTime ) )
    result = self.__getCPUHMS( cpuTime )
    return result

  #############################################################################
  def __getCPUHMS( self, cpuTime ):
    mins, secs = divmod( cpuTime, 60 )
    hours, mins = divmod( mins, 60 )
    humanTime = '%02d:%02d:%02d' % ( hours, mins, secs )
    self.log.verbose( 'Human readable CPU time is: %s' % humanTime )
    return S_OK( ( cpuTime, humanTime ) )

  #############################################################################
  def resolveInputData( self ):
    """Input data is resolved here using a VO specific plugin module.
    """
    self.__report( 'Running', 'Input Data Resolution', sendFlag = True )

    if self.ceArgs.has_key( 'LocalSE' ):
      localSEList = self.ceArgs['LocalSE']
    else:
      localSEList = gConfig.getValue( '/LocalSite/LocalSE', [] )
      if not localSEList:
        msg = 'Job has input data requirement but no site LocalSE defined'
        self.log.warn( msg )
        return S_ERROR( msg )

    inputData = self.jobArgs['InputData']
    self.log.verbose( 'Input Data is: \n%s' % ( inputData ) )
    if type( inputData ) in types.StringTypes:
      inputData = [inputData]

    if type( localSEList ) in types.StringTypes:
      localSEList = List.fromChar( localSEList )

    msg = 'Job Wrapper cannot resolve local replicas of input data with null '
    if not inputData:
      msg += 'job input data parameter '
      self.log.warn( msg )
      return S_ERROR( msg )
    if not localSEList:
      msg += 'site localSEList list'
      self.log.warn( msg )
#      return S_ERROR( msg )

    if not self.jobArgs.has_key( 'InputDataModule' ):
      msg = 'Job has no input data resolution module specified'
      self.log.warn( msg )
      # Use the default one
      inputDataPolicy = 'DIRAC.WorkloadManagementSystem.Client.InputDataResolution'
    else:
      inputDataPolicy = self.jobArgs['InputDataModule']

    self.log.verbose( 'Job input data requirement is \n%s' % ',\n'.join( inputData ) )
    self.log.verbose( 'Job input data resolution policy module is %s' % ( inputDataPolicy ) )
    self.log.info( 'Site has the following local SEs: %s' % ', '.join( localSEList ) )
    lfns = [ fname.replace( 'LFN:', '' ) for fname in inputData ]

    optReplicas = {}
    if self.optArgs:
      optDict = None
      try:
        optDict = eval( self.optArgs['InputData'] )
        optReplicas = optDict['Value']
        self.log.info( 'Found optimizer catalogue result' )
        self.log.verbose( optReplicas )
      except Exception, x:
        optDict = None
        self.log.warn( str( x ) )
        self.log.warn( 'Optimizer information could not be converted to a dictionary will call catalogue directly' )

    resolvedData = {}
    result = self.__checkFileCatalog( lfns, optReplicas )
    if not result['OK']:
      self.log.info( 'Could not obtain replica information from Optimizer File Catalog information' )
      self.log.warn( result )
      result = self.__checkFileCatalog( lfns )
      if not result['OK']:
        self.log.warn( 'Could not obtain replica information from File Catalog directly' )
        self.log.warn( result )
        return S_ERROR( result['Message'] )
      else:
        resolvedData = result
    else:
      resolvedData = result

    #add input data size to accounting report (since resolution successful)
    for lfn, mdata in resolvedData['Value']['Successful'].items():
      if mdata.has_key( 'Size' ):
        lfnSize = mdata['Size']
        if not type( lfnSize ) == type( long( 1 ) ):
          try:
            lfnSize = long( lfnSize )
          except Exception, x:
            lfnSize = 0
            self.log.info( 'File size for LFN:%s was not a long integer, setting size to 0' % ( lfn ) )
        self.inputDataSize += lfnSize

    configDict = {'JobID':self.jobID, 'LocalSEList':localSEList, 'DiskSEList':self.diskSE, 'TapeSEList':self.tapeSE}
    self.log.info( configDict )
    argumentsDict = {'FileCatalog':resolvedData, 'Configuration':configDict, 'InputData':lfns, 'Job':self.jobArgs}
    self.log.info( argumentsDict )
    moduleFactory = ModuleFactory()
    moduleInstance = moduleFactory.getModule( inputDataPolicy, argumentsDict )
    if not moduleInstance['OK']:
      return moduleInstance

    module = moduleInstance['Value']
    result = module.execute()
    if not result['OK']:
      self.log.warn( 'Input data resolution failed' )
      return result

    return S_OK()

  #############################################################################
  def __checkFileCatalog( self, lfns, optReplicaInfo = None ):
    """This function returns dictionaries containing all relevant parameters
       to allow data access from the relevant file catalogue.  Optionally, optimizer
       parameters can be supplied here but if these are not sufficient, the file catalogue
       is subsequently consulted.

       N.B. this will be considerably simplified when the DMS evolves to have a
       generic FC interface and a single call for all available information.
    """
    replicas = optReplicaInfo
    if not replicas:
      replicas = self.__getReplicaMetadata( lfns )
      if not replicas['OK']:
        return replicas

    self.log.verbose( replicas )

    failedGUIDs = []
    for lfn, reps in replicas['Value']['Successful'].items():
      if not reps.has_key( 'GUID' ):
        failedGUIDs.append( lfn )

    if failedGUIDs:
      self.log.info( 'The following file(s) were found not to have a GUID:\n%s' % ',\n'.join( failedGUIDs ) )

    if failedGUIDs:
      return S_ERROR( 'File metadata is not available' )
    else:
      return replicas

  #############################################################################
  def __getReplicaMetadata( self, lfns ):
    """ Wrapper function to consult catalog for all necessary file metadata
        and check the result.
    """
    start = time.time()
    repsResult = self.rm.getReplicas( lfns )
    timing = time.time() - start
    self.log.info( 'Replica Lookup Time: %.2f seconds ' % ( timing ) )
    if not repsResult['OK']:
      self.log.warn( repsResult['Message'] )
      return repsResult

    badLFNCount = 0
    badLFNs = []
    catalogResult = repsResult['Value']

    if catalogResult.has_key( 'Failed' ):
      for lfn, cause in catalogResult['Failed'].items():
        badLFNCount += 1
        badLFNs.append( 'LFN:%s Problem: %s' % ( lfn, cause ) )

    if catalogResult.has_key( 'Successful' ):
      for lfn, replicas in catalogResult['Successful'].items():
        if not replicas:
          badLFNCount += 1
          badLFNs.append( 'LFN:%s Problem: Null replica value' % ( lfn ) )

    if badLFNCount:
      self.log.warn( 'Job Wrapper found %s problematic LFN(s) for job %s' % ( badLFNCount, self.jobID ) )
      param = '\n'.join( badLFNs )
      self.log.info( param )
      self.__setJobParam( 'MissingLFNs', param )
      return S_ERROR( 'Input Data Not Available' )

    #Must retrieve GUIDs from LFC for files
    start = time.time()
    guidDict = self.rm.getCatalogFileMetadata( lfns )
    timing = time.time() - start
    self.log.info( 'GUID Lookup Time: %.2f seconds ' % ( timing ) )
    if not guidDict['OK']:
      self.log.warn( 'Failed to retrieve GUIDs from file catalogue' )
      self.log.warn( guidDict['Message'] )
      return guidDict

    failed = guidDict['Value']['Failed']
    if failed:
      self.log.warn( 'Could not retrieve GUIDs from catalogue for the following files' )
      self.log.warn( failed )
      return S_ERROR( 'Missing GUIDs' )

    for lfn, reps in repsResult['Value']['Successful'].items():
      guidDict['Value']['Successful'][lfn].update( reps )

    catResult = guidDict
    return catResult

  #############################################################################
  def processJobOutputs( self, arguments ):
    """Outputs for a job may be treated here.
    """

    #first iteration of this, no checking of wildcards or oversize sandbox files etc.
    outputSandbox = []
    if self.jobArgs.has_key( 'OutputSandbox' ):
      outputSandbox = self.jobArgs['OutputSandbox']
      if not type( outputSandbox ) == type( [] ):
        outputSandbox = [ outputSandbox ]
      self.log.verbose( 'OutputSandbox files are: %s' % ', '.join( outputSandbox ) )
    outputData = []
    if self.jobArgs.has_key( 'OutputData' ):
      outputData = self.jobArgs['OutputData']
      if not type( outputData ) == type( [] ):
        outputData = outputData.split( ';' )
      self.log.verbose( 'OutputData files are: %s' % ', '.join( outputData ) )

    #First resolve any wildcards for output files and work out if any files are missing
    resolvedSandbox = self.__resolveOutputSandboxFiles( outputSandbox )
    if not resolvedSandbox['OK']:
      self.log.warn( 'Output sandbox file resolution failed:' )
      self.log.warn( resolvedSandbox['Message'] )
      self.__report( 'Failed', 'Resolving Output Sandbox' )

    fileList = resolvedSandbox['Value']['Files']
    missingFiles = resolvedSandbox['Value']['Missing']
    if missingFiles:
      self.jobReport.setJobParameter( 'OutputSandboxMissingFiles', ', '.join( missingFiles ), sendFlag = False )

    if not self.jobArgs.has_key( 'Owner' ):
      msg = 'Job has no owner specified'
      self.log.warn( msg )
      return S_OK( msg )

    # Do not overwrite in case of Error
    if not self.failedFlag:
      self.__report( 'Completed', 'Uploading Output Sandbox' )

    if fileList and self.jobID:
      self.outputSandboxSize = getGlobbedTotalSize( fileList )
      self.log.info( 'Attempting to upload Sandbox with limit:', self.sandboxSizeLimit )
      sandboxClient = SandboxStoreClient()
      result = sandboxClient.uploadFilesAsSandboxForJob( fileList, self.jobID,
                                                         'Output', self.sandboxSizeLimit ) # 1024*1024*10
      if not result['OK']:
        self.log.error( 'Output sandbox upload failed with message', result['Message'] )
        if result.has_key( 'SandboxFileName' ):
          outputSandboxData = result['SandboxFileName']
          self.log.info( 'Attempting to upload %s as output data' % ( outputSandboxData ) )
          outputData.append( outputSandboxData )
          self.jobReport.setJobParameter( 'OutputSandbox', 'Sandbox uploaded to grid storage', sendFlag = False )
          self.jobReport.setJobParameter( 'OutputSandboxLFN',
                                          self.__getLFNfromOutputFile( outputSandboxData )[0], sendFlag = False )
        else:
          self.log.info( 'Could not get SandboxFileName to attempt upload to Grid storage' )
          return S_ERROR( 'Output sandbox upload failed and no file name supplied for failover to Grid storage' )
      else:
        # Do not overwrite in case of Error
        if not self.failedFlag:
          self.__report( 'Completed', 'Output Sandbox Uploaded' )
        self.log.info( 'Sandbox uploaded successfully' )

    if outputData and not self.failedFlag:
      #Do not upload outputdata if the job has failed.
      if self.jobArgs.has_key( 'OutputSE' ):
        outputSE = self.jobArgs['OutputSE']
        if type( outputSE ) in types.StringTypes:
          outputSE = [outputSE]
      else:
        outputSE = self.defaultOutputSE

      if self.jobArgs.has_key( 'OutputPath' ) and type( self.jobArgs['OutputPath'] ) in types.StringTypes:
        outputPath = self.jobArgs['OutputPath']
      else:
        outputPath = self.defaultOutputPath

      if not outputSE and not self.defaultFailoverSE:
        return S_ERROR( 'No output SEs defined in VO configuration' )

      result = self.__transferOutputDataFiles( outputData, outputSE, outputPath )
      if not result['OK']:
        return result

    return S_OK( 'Job outputs processed' )

  #############################################################################
  def __resolveOutputSandboxFiles( self, outputSandbox ):
    """Checks the output sandbox file list and resolves any specified wildcards.
       Also tars any specified directories.
    """
    missing = []
    okFiles = []
    for i in outputSandbox:
      self.log.verbose( 'Looking at OutputSandbox file/directory/wildcard: %s' % i )
      globList = glob.glob( i )
      for check in globList:
        if os.path.isfile( check ):
          self.log.verbose( 'Found locally existing OutputSandbox file: %s' % check )
          okFiles.append( check )
        if os.path.isdir( check ):
          self.log.verbose( 'Found locally existing OutputSandbox directory: %s' % check )
          cmd = ['tar', 'cf', '%s.tar' % check, check]
          result = systemCall( 60, cmd )
          if not result['OK']:
            self.log.error( 'Failed to create OutputSandbox tar', result['Message'] )
          elif result['Value'][0]:
            self.log.error( 'Failed to create OutputSandbox tar', result['Value'][2] )
          if os.path.isfile( '%s.tar' % ( check ) ):
            self.log.verbose( 'Appending %s.tar to OutputSandbox' % check )
            okFiles.append( '%s.tar' % ( check ) )
          else:
            self.log.warn( 'Could not tar OutputSandbox directory: %s' % check )
            missing.append( check )

    for i in outputSandbox:
      if not i in okFiles:
        if not '%s.tar' % i in okFiles:
          if not re.search( '\*', i ):
            if not i in missing:
              missing.append( i )

    result = {'Missing':missing, 'Files':okFiles}
    return S_OK( result )

  #############################################################################
  def __transferOutputDataFiles( self, outputData, outputSE, outputPath ):
    """Performs the upload and registration in the LFC
    """
    self.log.verbose( 'Uploading output data files' )
    self.__report( 'Completed', 'Uploading Output Data' )
    self.log.info( 'Output data files %s to be uploaded to %s SE' % ( ', '.join( outputData ), outputSE ) )
    missing = []
    uploaded = []

    # Separate outputdata in the form of lfns and local files
    lfnList = []
    nonlfnList = []
    for out in outputData:
      if out.lower().find( 'lfn:' ) != -1:
        lfnList.append( out )
      else:
        nonlfnList.append( out )

    # Check whether list of outputData has a globbable pattern    
    globbedOutputList = List.uniqueElements( getGlobbedFiles( nonlfnList ) )
    if not globbedOutputList == nonlfnList and globbedOutputList:
      self.log.info( 'Found a pattern in the output data file list, files to upload are:',
                     ', '.join( globbedOutputList ) )
      nonlfnList = globbedOutputList
    outputData = lfnList + nonlfnList

    pfnGUID = {}
    result = getGUID( outputData )
    if not result['OK']:
      self.log.warn( 'Failed to determine POOL GUID(s) for output file list (OK if not POOL files)',
                     result['Message'] )
    else:
      pfnGUID = result['Value']

    #Instantiate the failover transfer client
    failoverTransfer = FailoverTransfer()

    for outputFile in outputData:
      ( lfn, localfile ) = self.__getLFNfromOutputFile( outputFile, outputPath )
      if not os.path.exists( localfile ):
        self.log.error( 'Missing specified output data file:', outputFile )
        continue

      self.outputDataSize += getGlobbedTotalSize( localfile )
      outputFilePath = os.path.join( os.getcwd(), localfile )
      fileGUID = None
      if pfnGUID.has_key( localfile ):
        fileGUID = pfnGUID[localfile]
        self.log.verbose( 'Found GUID for file from POOL XML catalogue %s' % localfile )

      outputSEList = self.__getSortedSEList( outputSE )
      upload = failoverTransfer.transferAndRegisterFile( localfile, outputFilePath, lfn,
                                                         outputSEList, fileGUID, self.defaultCatalog )
      if upload['OK']:
        self.log.info( '"%s" successfully uploaded to "%s" as "LFN:%s"' % ( localfile,
                                                                            upload['Value']['uploadedSE'],
                                                                            lfn ) )
        uploaded.append( lfn )
        continue

      self.log.error( 'Could not putAndRegister file',
                      '%s with LFN %s to %s with GUID %s trying failover storage' % ( localfile, lfn,
                                                                                      ', '.join( outputSEList ),
                                                                                      fileGUID ) )
      if not self.defaultFailoverSE:
        self.log.info( 'No failover SEs defined for JobWrapper,',
                       'cannot try to upload output file %s anywhere else.' % outputFile )
        missing.append( outputFile )
        continue

      failoverSEs = self.__getSortedSEList( self.defaultFailoverSE )
      targetSE = outputSEList[0]
      result = failoverTransfer.transferAndRegisterFileFailover( localfile, outputFilePath,
                                                                 lfn, targetSE, failoverSEs,
                                                                 fileGUID, self.defaultCatalog )
      if not result['OK']:
        self.log.error( 'Completely failed to upload file to failover SEs with result:\n%s' % result )
        missing.append( outputFile )
      else:
        self.log.info( 'File %s successfully uploaded to failover storage element' % lfn )
        uploaded.append( lfn )


    #For files correctly uploaded must report LFNs to job parameters
    if uploaded:
      report = ', '.join( uploaded )
      #In case the VO payload has also uploaded data using the same parameter 
      #name this should be checked prior to setting. 
      monitoring = RPCClient( 'WorkloadManagement/JobMonitoring', timeout = 120 )
      result = monitoring.getJobParameter( int( self.jobID ), 'UploadedOutputData' )
      if result['OK']:
        if result['Value'].has_key( 'UploadedOutputData' ):
          report += ', %s' % result['Value']['UploadedOutputData']

      self.jobReport.setJobParameter( 'UploadedOutputData', report, sendFlag = False )

    #Write out failover transfer request object in case of deferred operations 
    result = failoverTransfer.getRequestObject()
    if not result['OK']:
      self.log.error( result )
      return S_ERROR( 'Could not retrieve modified request' )

    request = result['Value']
    if not request.isEmpty()['Value']:
      request.toFile( 'transferOutputDataFiles_request.xml' )

    #TODO Notify the user of any output data / output sandboxes
    if missing:
      self.__setJobParam( 'OutputData', 'MissingFiles: %s' % ', '.join( missing ) )
      self.__report( 'Failed', 'Uploading Job OutputData' )
      return S_ERROR( 'Failed to upload OutputData' )

    self.__report( 'Completed', 'Output Data Uploaded' )
    return S_OK( 'OutputData uploaded successfully' )

  #############################################################################
  def __getSortedSEList( self, seList ):
    """ Randomize SE, putting first those that are Local/Close to the Site
    """
    if not seList:
      return seList

    localSEs = []
    otherSEs = []
    siteSEs = []
    seMapping = getSEsForSite( DIRAC.siteName() )

    if seMapping['OK'] and seMapping['Value']:
      siteSEs = seMapping['Value']

    for seName in seList:
      if seName in siteSEs:
        localSEs.append( seName )
      else:
        otherSEs.append( seName )

    return List.randomize( localSEs ) + List.randomize( otherSEs )


  #############################################################################
  def __getLFNfromOutputFile( self, outputFile, outputPath = '' ):
    """Provides a generic convention for VO output data
       files if no path is specified.
    """

    if not re.search( '^LFN:', outputFile ):
      localfile = outputFile
      initial = self.owner[:1]
      vo = getVOForGroup( self.userGroup )
      if not vo:
        vo = 'dirac'
      basePath = '/' + vo + '/user/' + initial + '/' + self.owner
      if outputPath:
        # If output path is given, append it to the user path and put output files in this directory
        if outputPath.startswith( '/' ):
          outputPath = outputPath[1:]
      else:
        # By default the output path is constructed from the job id 
        subdir = str( self.jobID / 1000 )
        outputPath = subdir + '/' + str( self.jobID )
      lfn = os.path.join( basePath, outputPath, os.path.basename( localfile ) )
    else:
      # if LFN is given, take it as it is
      localfile = os.path.basename( outputFile.replace( "LFN:", "" ) )
      lfn = outputFile.replace( "LFN:", "" )

    return ( lfn, localfile )

  #############################################################################
  def transferInputSandbox( self, inputSandbox ):
    """Downloads the input sandbox for the job
    """
    sandboxFiles = []
    registeredISB = []
    lfns = []
    self.__report( 'Running', 'Downloading InputSandbox' )
    if type( inputSandbox ) not in ( types.TupleType, types.ListType ):
      inputSandbox = [ inputSandbox ]
    for isb in inputSandbox:
      if isb.find( "LFN:" ) == 0 or isb.find( "lfn:" ) == 0:
        lfns.append( isb )
      else:
        if isb.find( "SB:" ) == 0:
          registeredISB.append( isb )
        else:
          sandboxFiles.append( os.path.basename( isb ) )


    self.log.info( 'Downloading InputSandbox for job %s: %s' % ( self.jobID, ', '.join( sandboxFiles ) ) )
    if os.path.exists( '%s/inputsandbox' % ( self.root ) ):
      # This is a debugging tool, get the file from local storage to debug Job Wrapper
      sandboxFiles.append( 'jobDescription.xml' )
      for inputFile in sandboxFiles:
        if os.path.exists( '%s/inputsandbox/%s' % ( self.root, inputFile ) ):
          self.log.info( 'Getting InputSandbox file %s from local directory for testing' % ( inputFile ) )
          shutil.copy( self.root + '/inputsandbox/' + inputFile, inputFile )
      result = S_OK( sandboxFiles )
    else:
      if registeredISB:
        for isb in registeredISB:
          self.log.info( "Downloading Input SandBox %s" % isb )
          result = SandboxStoreClient().downloadSandbox( isb )
          if not result[ 'OK' ]:
            self.__report( 'Running', 'Failed Downloading InputSandbox' )
            return S_ERROR( "Cannot download Input sandbox %s: %s" % ( isb, result[ 'Message' ] ) )
          else:
            self.inputSandboxSize += result[ 'Value' ]

    if lfns:
      self.__report( 'Running', 'Downloading InputSandbox LFN(s)' )
      lfns = [fname.replace( 'LFN:', '' ).replace( 'lfn:', '' ) for fname in lfns]
      download = self.rm.getFile( lfns )
      if not download['OK']:
        self.log.warn( download )
        self.__report( 'Running', 'Failed Downloading InputSandbox LFN(s)' )
        return S_ERROR( download['Message'] )
      failed = download['Value']['Failed']
      if failed:
        self.log.warn( 'Could not download InputSandbox LFN(s)' )
        self.log.warn( failed )
        return S_ERROR( str( failed ) )
      for lfn in lfns:
        if os.path.exists( '%s/%s' % ( self.root, os.path.basename( download['Value']['Successful'][lfn] ) ) ):
          sandboxFiles.append( os.path.basename( download['Value']['Successful'][lfn] ) )

    userFiles = sandboxFiles + [ os.path.basename( lfn ) for lfn in lfns ]
    for possibleTarFile in userFiles:
      if not os.path.exists( possibleTarFile ):
        continue
      try:
        if tarfile.is_tarfile( possibleTarFile ):
          self.log.info( 'Unpacking input sandbox file %s' % ( possibleTarFile ) )
          tarFile = tarfile.open( possibleTarFile, 'r' )
          for member in tarFile.getmembers():
            tarFile.extract( member, os.getcwd() )
      except Exception, x :
        return S_ERROR( 'Could not untar %s with exception %s' % ( possibleTarFile, str( x ) ) )

    if userFiles:
      self.inputSandboxSize = getGlobbedTotalSize( userFiles )
      self.log.info( "Total size of input sandbox:",
                     "%0.2f MiB (%s bytes)" % ( self.inputSandboxSize / 1048576.0, self.inputSandboxSize ) )

    return S_OK( 'InputSandbox downloaded' )

  #############################################################################
  def finalize( self, arguments ):
    """Perform any final actions to clean up after job execution.
    """
    self.log.info( 'Running JobWrapper finalization' )
    requests = self.__getRequestFiles()
    if self.failedFlag and requests:
      self.log.info( 'Application finished with errors and there are pending requests for this job.' )
      self.__report( 'Failed', 'Pending Requests' )
    elif not self.failedFlag and requests:
      self.log.info( 'Application finished successfully with pending requests for this job.' )
      self.__report( 'Completed', 'Pending Requests' )
    elif self.failedFlag and not requests:
      self.log.info( 'Application finished with errors with no pending requests.' )
      self.__report( 'Failed' )
    elif not self.failedFlag and not requests:
      self.log.info( 'Application finished successfully with no pending requests for this job.' )
      self.__report( 'Done', 'Execution Complete' )

    self.sendFailoverRequest()
    self.__cleanUp()
    if self.failedFlag:
      return 1
    else:
      return 0

  #############################################################################
  def sendWMSAccounting( self, status = '', minorStatus = '' ):
    """Send WMS accounting data.
    """
    if self.wmsAccountingSent:
      return S_OK()
    if status:
      self.wmsMajorStatus = status
    if minorStatus:
      self.wmsMinorStatus = minorStatus

    self.accountingReport.setEndTime()
    #CPUTime and ExecTime
    if not 'CPU' in EXECUTION_RESULT:
      # If the payload has not started execution (error with input data, SW, SB,...)
      # Execution result is not filled use self.initialTiming
      self.log.info( 'EXECUTION_RESULT[CPU] missing in sendWMSAccounting' )
      finalStat = os.times()
      EXECUTION_RESULT['CPU'] = []
      for i in range( len( finalStat ) ):
        EXECUTION_RESULT['CPU'].append( finalStat[i] - self.initialTiming[i] )

    self.log.info( 'EXECUTION_RESULT[CPU] in sendWMSAccounting', str( EXECUTION_RESULT['CPU'] ) )

    utime, stime, cutime, cstime, elapsed = EXECUTION_RESULT['CPU']
    cpuTime = utime + stime + cutime + cstime
    execTime = elapsed
    diskSpaceConsumed = getGlobbedTotalSize( os.path.join( self.root, str( self.jobID ) ) )
    #Fill the data
    acData = {
               'User' : self.owner,
               'UserGroup' : self.userGroup,
               'JobGroup' : self.jobGroup,
               'JobType' : self.jobType,
               'JobClass' : self.jobClass,
               'ProcessingType' : self.processingType,
               'FinalMajorStatus' : self.wmsMajorStatus,
               'FinalMinorStatus' : self.wmsMinorStatus,
               'CPUTime' : cpuTime,
               # Based on the factor to convert raw CPU to Normalized units (based on the CPU Model)
               'NormCPUTime' : cpuTime * self.cpuNormalizationFactor,
               'ExecTime' : execTime,
               'InputDataSize' : self.inputDataSize,
               'OutputDataSize' : self.outputDataSize,
               'InputDataFiles' : self.inputDataFiles,
               'OutputDataFiles' : self.outputDataFiles,
               'DiskSpace' : diskSpaceConsumed,
               'InputSandBoxSize' : self.inputSandboxSize,
               'OutputSandBoxSize' : self.outputSandboxSize,
               'ProcessedEvents' : self.processedEvents
             }
    self.log.verbose( 'Accounting Report is:' )
    self.log.verbose( acData )
    self.accountingReport.setValuesFromDict( acData )
    result = self.accountingReport.commit()
    # Even if it fails a failover request will be created
    self.wmsAccountingSent = True
    return result

  #############################################################################
  def sendFailoverRequest( self, status = '', minorStatus = '' ):
    """ Create and send a combined job failover request if any
    """
    request = RequestContainer()
    requestName = '%s.xml' % self.jobID
    if self.jobArgs.has_key( 'JobName' ):
      #To make the request names more appealing for users
      jobName = self.jobArgs['JobName']
      if type( jobName ) == type( ' ' ) and jobName:
        jobName = jobName.replace( ' ', '' ).replace( '(', '' ).replace( ')', '' ).replace( '"', '' )
        jobName = jobName.replace( '.', '' ).replace( '{', '' ).replace( '}', '' ).replace( ':', '' )
        requestName = '%s_%s' % ( jobName, requestName )

    if '"' in requestName: 
      requestName = requestName.replace( '"', '' )
    request.setRequestName( requestName )
    request.setJobID( self.jobID )
    request.setSourceComponent( "Job_%s" % self.jobID )

    # JobReport part first
    result = self.jobReport.generateRequest()
    if result['OK']:
      reportRequest = result['Value']
      if reportRequest:
        request.update( reportRequest )

    # Accounting part
    if not self.jobID:
      self.log.verbose( 'No accounting to be sent since running locally' )
    else:
      result = self.sendWMSAccounting( status, minorStatus )
      if not result['OK']:
        self.log.warn( 'Could not send WMS accounting with result: \n%s' % result )
        if result.has_key( 'rpcStub' ):
          self.log.verbose( 'Adding accounting report to failover request object' )
          request.setDISETRequest( result['rpcStub'] )
        else:
          self.log.warn( 'No rpcStub found to construct failover request for WMS accounting report' )

    # Any other requests in the current directory
    rfiles = self.__getRequestFiles()
    for rfname in rfiles:
      rfile = open( rfname, 'r' )
      reqString = rfile.read()
      rfile.close()
      requestStored = RequestContainer( reqString )
      request.update( requestStored )

    # The request is ready, send it now
    if not request.isEmpty()['Value']:
      requestClient = RequestClient()
      requestString = request.toXML()['Value']
      result = requestClient.setRequest( requestName, requestString )
      if result['OK']:
        resDigest = request.getDigest()
        digest = resDigest['Value']
        self.jobReport.setJobParameter( 'PendingRequest', digest )
      else:
        self.__report( 'Failed', 'Failover Request Failed' )
        self.log.error( 'Failed to set failover request', result['Message'] )
      return result
    else:
      return S_OK()

  #############################################################################
  def __getRequestFiles( self ):
    """Simple wrapper to return the list of request files.
    """
    return glob.glob( '*_request.xml' )

  #############################################################################
  def __cleanUp( self ):
    """Cleans up after job processing. Can be switched off via environment
       variable DO_NOT_DO_JOB_CLEANUP or by JobWrapper configuration option.
    """
    #Environment variable is a feature for DIRAC (helps local debugging).
    if os.environ.has_key( 'DO_NOT_DO_JOB_CLEANUP' ) or not self.cleanUpFlag:
      cleanUp = False
    else:
      cleanUp = True

    os.chdir( self.root )
    if cleanUp:
      self.log.verbose( 'Cleaning up job working directory' )
      if os.path.exists( str( self.jobID ) ):
        shutil.rmtree( str( self.jobID ) )

  #############################################################################
  def __report( self, status = '', minorStatus = '', sendFlag = False ):
    """Wraps around setJobStatus of state update client
    """
    if status:
      self.wmsMajorStatus = status
    if minorStatus:
      self.wmsMinorStatus = minorStatus
    jobStatus = self.jobReport.setJobStatus( status = status, minor = minorStatus, sendFlag = sendFlag )
    if not jobStatus['OK']:
      self.log.warn( jobStatus['Message'] )
    if self.jobID:
      self.log.verbose( 'setJobStatus(%s,%s,%s,%s)' % ( self.jobID, status, minorStatus, 'JobWrapper' ) )

    return jobStatus

  #############################################################################
  def __setJobParam( self, name, value, sendFlag = False ):
    """Wraps around setJobParameter of state update client
    """
    jobParam = self.jobReport.setJobParameter( str( name ), str( value ), sendFlag )
    if not jobParam['OK']:
      self.log.warn( jobParam['Message'] )
    if self.jobID:
      self.log.verbose( 'setJobParameter(%s,%s,%s)' % ( self.jobID, name, value ) )

    return jobParam

  #############################################################################
  def __setJobParamList( self, value, sendFlag = False ):
    """Wraps around setJobParameters of state update client
    """
    jobParam = self.jobReport.setJobParameters( value, sendFlag )
    if not jobParam['OK']:
      self.log.warn( jobParam['Message'] )
    if self.jobID:
      self.log.verbose( 'setJobParameters(%s,%s)' % ( self.jobID, value ) )

    return jobParam

###############################################################################
###############################################################################

class ExecutionThread( threading.Thread ):

  #############################################################################
  def __init__( self, spObject, cmd, maxPeekLines, stdoutFile, stderrFile, exeEnv ):
    threading.Thread.__init__( self )
    self.cmd = cmd
    self.spObject = spObject
    self.outputLines = []
    self.maxPeekLines = maxPeekLines
    self.stdout = stdoutFile
    self.stderr = stderrFile
    self.exeEnv = exeEnv

  #############################################################################
  def run( self ):
    # FIXME: why local instances of object variables are created?
    cmd = self.cmd
    spObject = self.spObject
    start = time.time()
    initialStat = os.times()
    output = spObject.systemCall( cmd, env = self.exeEnv, callbackFunction = self.sendOutput, shell = True )
    EXECUTION_RESULT['Thread'] = output
    timing = time.time() - start
    EXECUTION_RESULT['Timing'] = timing
    finalStat = os.times()
    EXECUTION_RESULT['CPU'] = []
    for i in range( len( finalStat ) ):
      EXECUTION_RESULT['CPU'].append( finalStat[i] - initialStat[i] )
    gLogger.info( 'EXECUTION_RESULT[CPU] after Execution of spObject.systemCall', str( EXECUTION_RESULT['CPU'] ) )
    gLogger.info( 'EXECUTION_RESULT[Thread] after Execution of spObject.systemCall', str( EXECUTION_RESULT['Thread'] ) )

  #############################################################################
  def getCurrentPID( self ):
    return self.spObject.getChildPID()

  #############################################################################
  def sendOutput( self, stdid, line ):
    if stdid == 0 and self.stdout:
      outputFile = open( self.stdout, 'a+' )
      print >> outputFile, line
      outputFile.close()
    elif stdid == 1 and self.stderr:
      errorFile = open( self.stderr, 'a+' )
      print >> errorFile, line
      errorFile.close()
    self.outputLines.append( line )
    size = len( self.outputLines )
    if size > self.maxPeekLines:
      # reduce max size of output peeking
      self.outputLines.pop( 0 )

  #############################################################################
  def getOutput( self, lines = 0 ):
    if self.outputLines:
      #restrict to smaller number of lines for regular
      #peeking by the watchdog
      # FIXME: this is multithread, thus single line would be better
      if lines:
        size = len( self.outputLines )
        cut = size - lines
        self.outputLines = self.outputLines[cut:]

      result = S_OK()
      result['Value'] = self.outputLines
    else:
      result = S_ERROR( 'No Job output found' )

    return result

def rescheduleFailedJob( jobID, message, jobReport = None ):
  try:

    gLogger.warn( 'Failure during %s' % ( message ) )

    #Setting a job parameter does not help since the job will be rescheduled,
    #instead set the status with the cause and then another status showing the
    #reschedule operation.

    if not jobReport:
      gLogger.info( 'Creating a new JobReport Object' )
      jobReport = JobReport( int( jobID ), 'JobWrapper' )

    jobReport.setApplicationStatus( 'Failed %s ' % message, sendFlag = False )
    jobReport.setJobStatus( 'Rescheduled', message, sendFlag = False )

    # We must send Job States and Parameters before it gets reschedule
    jobReport.sendStoredStatusInfo()
    jobReport.sendStoredJobParameters()

    gLogger.info( 'Job will be rescheduled after exception during execution of the JobWrapper' )

    jobManager = RPCClient( 'WorkloadManagement/JobManager' )
    result = jobManager.rescheduleJob( int( jobID ) )
    if not result['OK']:
      gLogger.warn( result )

    # Send mail to debug errors
    mailAddress = DIRAC.alarmMail
    site = DIRAC.siteName()
    subject = 'Job rescheduled at %s' % site
    ret = systemCall( 0, 'hostname' )
    wn = ret['Value'][1]
    msg = 'Job %s rescheduled at %s, wn=%s\n' % ( jobID, site, wn )
    msg += message

    NotificationClient().sendMail( mailAddress, subject, msg, fromAddress = "lhcb-dirac@cern.ch", localAttempt = False )

    return
  except Exception:
    gLogger.exception( 'JobWrapperTemplate failed to reschedule Job' )
    return


#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
