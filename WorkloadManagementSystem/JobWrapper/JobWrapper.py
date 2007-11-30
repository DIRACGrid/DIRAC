#!/usr/bin/env python
########################################################################
# $Id: JobWrapper.py,v 1.1 2007/11/30 17:50:55 paterson Exp $
# File :   JobWrapper.py
# Author : Stuart Paterson
########################################################################

""" The Job Wrapper Class is instantiated with arguments tailored for running
    a particular job. The JobWrapper starts a thread for execution of the job
    and a Watchdog Agent that can monitor progress.
"""

__RCSID__ = "$Id: JobWrapper.py,v 1.1 2007/11/30 17:50:55 paterson Exp $"

#from DIRAC.WorkloadManagementSystem.DB.JobLoggingDB      import JobLoggingDB
from DIRAC.WorkloadManagementSystem.Client.SandboxClient        import SandboxClient
from DIRAC.WorkloadManagementSystem.JobWrapper.WatchdogFactory  import WatchdogFactory
from DIRAC.Core.DISET.RPCClient                                 import RPCClient
from DIRAC.Core.Utilities.Subprocess                            import shellCall
from DIRAC.Core.Utilities.Subprocess                            import Subprocess
from DIRAC                                                      import S_OK, S_ERROR, gConfig, gLogger
import DIRAC

import os, re, sys, string, time, shutil, threading, tarfile

COMPONENT_NAME = 'WorkloadManagement/JobWrapper'

EXECUTION_RESULT = {}

class JobWrapper:

  #############################################################################
  def __init__(self, jobID=None):
    """ Standard constructor
    """
    self.section = COMPONENT_NAME
    self.log = gLogger
    #self.log.setLevel('debug')
    self.jobID = jobID
    self.root = os.getcwd()
    self.jobReport  = RPCClient('WorkloadManagement/JobStateUpdate')
    self.sandboxClient = SandboxClient()
    self.diracVersion = 'DIRAC version v%dr%d build %d' %(DIRAC.majorVersion,DIRAC.minorVersion,DIRAC.patchLevel)
    self.maxPeekLines = gConfig.getValue(self.section+'/MaxJobPeekLines',200)
    self.defaultCPUTime = gConfig.getValue(self.section+'/DefaultCPUTime',600)
    self.defaultOutputFile = gConfig.getValue(self.section+'/DefaultOutputFile','std.out')
    self.defaultErrorFile = gConfig.getValue(self.section+'/DefaultErrorFile','std.err')
    self.cleanUpFlag  = gConfig.getValue(self.section+'/CleanUpFlag',False)
    self.log.debug('===========================================================================')
    self.log.debug('CVS version %s' %(__RCSID__))
    self.log.debug(self.diracVersion)
    self.log.debug('Developer tag: 1')
    currentPID = os.getpid()
    self.log.debug('Job Wrapper started under PID: %s' % currentPID )
    self.log.debug('==========================================================================')
    if not self.cleanUpFlag:
      self.log.debug('CleanUp Flag is disabled by configuration')

  #############################################################################

  def initialize(self, arguments):
    """ Initializes parameters and environment for job.
    """
    self.__report('Running','Job Initialization')
    self.log.info('Starting Job Wrapper Initialization for Job %s' %(self.jobID))
    jobArgs = arguments['Job']
    ceArgs = arguments ['CE']
    self.__setInitialJobParameters(arguments)

    # Prepare the working directory and cd to there
    if os.path.exists(self.jobID):
      shutil.rmtree(str(self.jobID))
    os.mkdir(str(self.jobID))
    os.chdir(str(self.jobID))

  #############################################################################
  def execute(self, arguments):
    """The main execution method of the Job Wrapper
    """
    self.log.info('Job Wrapper is starting execution phase for job %s' %(self.jobID))
    jobArgs = arguments['Job']
    ceArgs = arguments ['CE']

    if jobArgs.has_key('MaxCPUTime'):
      jobCPUTime = int(jobArgs['MaxCPUTime'])
    else:
      self.log.info('Job %s has no CPU time limit specified, applying default of %s' %(self.jobID,self.defaultCPUTime))
      jobCPUTime = self.defaultCPUTime

    if jobArgs.has_key('Executable'):
      executable = jobArgs['Executable']
    else:
      msg = 'Job %s has no specified executable' %(self.jobID)
      self.log.warn(msg)
      return S_ERROR(msg)

    jobArguments = ' '
    if jobArgs.has_key('Arguments'):
      jobArguments = jobArgs['Arguments']

    executable = os.path.expandvars(executable)
    thread = None
    spObject = None
    if os.path.exists(executable):
      self.__report('Running','Application')
      spObject = Subprocess( 0 )
#      command = sys.executable+' '+executable+' '+jobArguments
      command = '%s %s' % (executable,os.path.basename(jobArguments))
      self.log.verbose('Execution command: %s' %(command))
      maxPeekLines = self.maxPeekLines
      thread = ExecutionThread(spObject,command, maxPeekLines)
      thread.start()
    else:
      return S_ERROR('Path to executable %s not found' %(executable))

    pid = os.getpid()
    watchdogFactory = WatchdogFactory()
    watchdogInstance = watchdogFactory.getWatchdog(pid, thread, spObject, jobCPUTime)
    if not watchdogInstance['OK']:
      return watchdogInstance

    watchdog = watchdogInstance['Value']
    watchdog.calibrate()
    if thread.isAlive():
      self.log.info('Application thread is started in Job Wrapper')
      watchdog.run()
    else:
      self.log.warn('Application thread stopped very quickly...')

    self.log.debug( 'Execution Result is : ')
    self.log.debug( EXECUTION_RESULT )
    outputs = None
    if EXECUTION_RESULT.has_key('Thread'):
      threadResult = EXECUTION_RESULT['Thread']
      if not threadResult['OK']:
        self.log.warn(threadResult['Message'])
      else:
        outputs = threadResult['Value']

    if outputs:
      errorFileName = self.defaultErrorFile
      outputFileName = self.defaultOutputFile
      status = threadResult['Value'][0]
      stdout = threadResult['Value'][1]
      stderr = threadResult['Value'][2]
      self.log.debug('Execution thread status = %s' %(status))
      if jobArgs.has_key('StdError'):
        errorFileName = jobArgs['StdError']
      if jobArgs.has_key('StdOutput'):
        outputFileName = jobArgs['StdOutput']
      outputFile = open(outputFileName,'w')
      print >> outputFile, stdout
      outputFile.close()
      errorFile = open(errorFileName,'w')
      print >> errorFile, stderr
      errorFile.close()
    else:
      self.log.warn('No outputs generated from job execution')

    return S_OK()

  #############################################################################
  def resolveInputData(self,arguments):
    """Input data is resolved here.
    """
    self.__report('Running','InputData Resolution')
    self.log.info('To implement: resolveInputData()')
    return S_OK()

  #############################################################################
  def processJobOutputs(self,arguments):
    """Outputs for a job may be treated here.
    """
    self.__report('Running','Uploading Job Outputs')
    self.log.info('To implement: processJobOutputs()')
    return S_OK()

  #############################################################################
  def transferInputSandbox(self,inputSandbox):
    """Downloads the input sandbox for the job
    """
    sandboxFiles = []
    self.__report('Running','Downloading InputSandbox')
    for i in inputSandbox: sandboxFiles.append(os.path.basename(i))
    self.log.info('Downloading InputSandbox for job %s: %s' %(self.jobID,string.join(sandboxFiles)))

    if os.path.exists('%s/inputsandbox' %(self.root)):
      # This is a debugging tool
      # Get the file from local storage to debug Job Wrapper
      sandboxFiles.append('jobDescription.xml')
      for inputFile in sandboxFiles:
        if os.path.exists('%s/inputsandbox/%s' %(self.root,inputFile)):
          self.log.info('Getting InputSandbox file %s from local directory for testing' %(inputFile))
          shutil.copy(self.root+'/inputsandbox/'+inputFile,inputFile)
      result = S_OK(sandboxFiles)
    else:
      result =  self.sandboxClient.getSandbox(int(self.jobID))
      if not result['OK']:
        self.__report('Running','Failed Downloading InputSandbox')
        return S_ERROR('InputSandbox download failed for job %s and sandbox %s' %(self.jobID,sandboxFiles))

    self.log.verbose('Sandbox download result: %s' %(result))
    return result

  #############################################################################
  def finalize(self,arguments):
    """Perform any final actions to clean up after job execution.
    """
    self.__cleanUp()
    return S_OK()

  #############################################################################
  def __cleanUp(self):
    """Cleans up after job processing. Can be switched off via environment
       variable DO_NOT_DO_JOB_CLEANUP or by JobWrapper configuration option.
    """
    if os.environ.has_key('DO_NOT_DO_JOB_CLEANUP') or not self.cleanUpFlag:
      cleanUp = False
    else:
      cleanUp = True

    os.chdir(self.root)
    if cleanUp:
      self.log.verbose('Cleaning up job working directory')
      if os.path.exists(self.jobID):
        shutil.rmtree(self.jobID)

  #############################################################################
  def __setInitialJobParameters(self,arguments):
    """Sets some initial job parameters
    """
    parameters = []
    if os.environ.has_key('EDG_WL_JOBID'):
      parameters.append(('EDG_WL_JOBID', os.environ['EDG_WL_JOBID']))
    if os.environ.has_key('GLITE_WMS_JOBID'):
      parameters.append(('GLITE_WMS_JOBID', os.environ['GLITE_WMS_JOBID']))

    ceArgs = arguments['CE']
    if ceArgs.has_key('LocalSE'):
      parameters.append(('AgentLocalSE',string.join(ceArgs['LocalSE'],',')))
    if ceArgs.has_key('CompatiblePlatforms'):
      parameters.append(('AgentCompatiblePlatforms',string.join(ceArgs['CompatiblePlatforms'],',')))

    parameters.append (('PilotAgent',self.diracVersion))
    result = self.__setJobParamList(parameters)
    return result

  #############################################################################
  def __report(self,status,minorStatus):
    """Wraps around setJobStatus of state update client
    """
    jobStatus = self.jobReport.setJobStatus(int(self.jobID),status,minorStatus,'JobWrapper')
    self.log.debug('setJobStatus(%s,%s,%s,%s)' %(self.jobID,status,minorStatus,'JobWrapper'))
    if not jobStatus['OK']:
        self.log.warn(jobStatus['Message'])

    return jobStatus

  #############################################################################
  def __setJobParam(self,name,value):
    """Wraps around setJobParameter of state update client
    """
    jobParam = self.jobReport.setJobParameter(int(self.jobID),str(name),str(value))
    self.log.debug('setJobParameter(%s,%s,%s)' %(self.jobID,name,value))
    if not jobParam['OK']:
        self.log.warn(jobParam['Message'])

    return jobParam

  #############################################################################
  def __setJobParamList(self,value):
    """Wraps around setJobParameters of state update client
    """
    jobParam = self.jobReport.setJobParameters(int(self.jobID),value)
    self.log.debug('setJobParameters(%s,%s)' %(self.jobID,value))
    if not jobParam['OK']:
        self.log.warn(jobParam['Message'])

    return jobParam

###############################################################################
###############################################################################

class ExecutionThread(threading.Thread):

  #############################################################################
  def __init__(self,spObject,cmd,maxPeekLines):
    threading.Thread.__init__(self)
    self.cmd = cmd
    self.spObject = spObject
    self.outputLines = []
    self.maxPeekLines = maxPeekLines
    self.outputFile = 'appstd.out'

  #############################################################################
  def run(self):
    cmd = self.cmd
    spObject = self.spObject
    pid = os.getpid()
    start = time.time()
    output = spObject.systemCall( cmd, callbackFunction = self.sendOutput, shell = True )
    EXECUTION_RESULT['Thread'] = output
    timing = time.time() - start
    EXECUTION_RESULT['PID']=pid
    EXECUTION_RESULT['Timing']=timing

  #############################################################################
  def sendOutput(self,stdid,line):
    self.outputLines.append(line)

  #############################################################################
  def getOutput(self,lines=0):
    if self.outputLines:
      size = len(self.outputLines)
      #reduce max size of output peeking
      if size > self.maxPeekLines:
        cut = size - self.maxPeekLines
        self.outputLines = self.outputLines[cut:]
      #restrict to smaller number of lines for regular
      #peeking by the watchdog
      if lines:
        size = len(self.outputLines)
        cut  = size - lines
        self.outputLines = self.outputLines[cut:]

      result = S_OK()
      result['Value'] = self.outputLines
    else:
      result = S_ERROR('No Job output found')

    return result

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#