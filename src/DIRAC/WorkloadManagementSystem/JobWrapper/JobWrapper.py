"""
The Job Wrapper Class is instantiated with arguments tailored for running
a particular job. The JobWrapper starts a thread for execution of the job
and a Watchdog Agent that can monitor its progress.


.. literalinclude:: ../ConfigTemplate.cfg
  :start-after: ##BEGIN JobWrapper
  :end-before: ##END
  :caption: JobWrapper options

"""

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

__RCSID__ = "$Id$"

import os
import stat
import re
import sys
import time
import shutil
import threading
import tarfile
import glob
import json
import six
import distutils.spawn
import datetime

from six.moves.urllib.parse import unquote as urlunquote

import DIRAC
from DIRAC import S_OK, S_ERROR, gConfig, gLogger
from DIRAC.Core.Utilities import DErrno
from DIRAC.Core.Utilities import List
from DIRAC.Core.Utilities import DEncode
from DIRAC.Core.Utilities import Time
from DIRAC.Core.Utilities.SiteSEMapping import getSEsForSite
from DIRAC.Core.Utilities.ModuleFactory import ModuleFactory
from DIRAC.Core.Utilities.Subprocess import systemCall
from DIRAC.Core.Utilities.Subprocess import Subprocess
from DIRAC.Core.Utilities.File import getGlobbedTotalSize, getGlobbedFiles
from DIRAC.Core.Utilities.Version import getCurrentVersion
from DIRAC.Core.Utilities.Adler import fileAdler
from DIRAC.AccountingSystem.Client.Types.Job import Job as AccountingJob
from DIRAC.ConfigurationSystem.Client.PathFinder import getSystemSection
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOForGroup
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.DataManagementSystem.Client.DataManager import DataManager
from DIRAC.DataManagementSystem.Client.FailoverTransfer import FailoverTransfer
from DIRAC.DataManagementSystem.Utilities.DMSHelpers import resolveSEGroup
from DIRAC.Resources.Catalog.PoolXMLFile import getGUID
from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
from DIRAC.RequestManagementSystem.private.RequestValidator import RequestValidator
from DIRAC.WorkloadManagementSystem.JobWrapper.Watchdog import Watchdog
from DIRAC.WorkloadManagementSystem.Client.JobStateUpdateClient import JobStateUpdateClient
from DIRAC.WorkloadManagementSystem.Client.JobMonitoringClient import JobMonitoringClient
from DIRAC.WorkloadManagementSystem.Client.JobManagerClient import JobManagerClient
from DIRAC.WorkloadManagementSystem.Client.SandboxStoreClient import SandboxStoreClient
from DIRAC.WorkloadManagementSystem.Client.JobReport import JobReport
from DIRAC.WorkloadManagementSystem.Client import JobStatus
from DIRAC.WorkloadManagementSystem.Client import JobMinorStatus

EXECUTION_RESULT = {}


class JobWrapper(object):
  """ The only user of the JobWrapper is the JobWrapperTemplate
  """

  #############################################################################
  def __init__(self, jobID=None, jobReport=None):
    """ Standard constructor
    """
    self.initialTiming = os.times()
    self.section = os.path.join(getSystemSection('WorkloadManagement/JobWrapper'), 'JobWrapper')
    # Create the accounting report
    self.accountingReport = AccountingJob()
    # Initialize for accounting
    self.wmsMajorStatus = "unknown"
    self.wmsMinorStatus = "unknown"
    # Set now as start time
    self.accountingReport.setStartTime()
    if not jobID:
      self.jobID = 0
    else:
      self.jobID = jobID
    self.siteName = DIRAC.siteName()
    if jobReport:
      self.jobReport = jobReport
    else:
      self.jobReport = JobReport(self.jobID, 'JobWrapper@%s' % self.siteName)
    self.failoverTransfer = FailoverTransfer()

    self.log = gLogger.getSubLogger('JobWrapper[%s]' % self.jobID)
    self.log.showHeaders(True)

    # self.root is the path the Wrapper is running at
    self.root = os.getcwd()
    # self.localSiteRoot is the path where the local DIRAC installation used to run the payload
    # is taken from
    self.localSiteRoot = gConfig.getValue('/LocalSite/Root', DIRAC.rootPath)
    # FIXME: Why do we need to load any .cfg file here????
    self.__loadLocalCFGFiles(self.localSiteRoot)
    result = getCurrentVersion()
    if result['OK']:
      self.diracVersion = result['Value']
    else:
      self.diracVersion = 'DIRAC version %s' % DIRAC.version
    self.maxPeekLines = gConfig.getValue(self.section + '/MaxJobPeekLines', 20)
    if self.maxPeekLines < 0:
      self.maxPeekLines = 0
    self.defaultCPUTime = gConfig.getValue(self.section + '/DefaultCPUTime', 600)
    self.defaultOutputFile = gConfig.getValue(self.section + '/DefaultOutputFile', 'std.out')
    self.defaultErrorFile = gConfig.getValue(self.section + '/DefaultErrorFile', 'std.err')
    self.diskSE = gConfig.getValue(self.section + '/DiskSE', ['-disk', '-DST', '-USER'])
    self.tapeSE = gConfig.getValue(self.section + '/TapeSE', ['-tape', '-RDST', '-RAW'])
    self.sandboxSizeLimit = gConfig.getValue(self.section + '/OutputSandboxLimit', 1024 * 1024 * 10)
    self.cleanUpFlag = gConfig.getValue(self.section + '/CleanUpFlag', True)
    self.boincUserID = gConfig.getValue('/LocalSite/BoincUserID', 0)
    self.pilotRef = gConfig.getValue('/LocalSite/PilotReference', 'Unknown')
    self.cpuNormalizationFactor = gConfig.getValue("/LocalSite/CPUNormalizationFactor", 0.0)
    self.bufferLimit = gConfig.getValue(self.section + '/BufferLimit', 10485760)
    self.defaultOutputSE = resolveSEGroup(gConfig.getValue('/Resources/StorageElementGroups/SE-USER', []))
    self.defaultCatalog = gConfig.getValue(self.section + '/DefaultCatalog', [])
    self.masterCatalogOnlyFlag = gConfig.getValue(self.section + '/MasterCatalogOnlyFlag', True)
    self.defaultFailoverSE = resolveSEGroup(gConfig.getValue('/Resources/StorageElementGroups/Tier1-Failover', []))
    self.defaultOutputPath = ''
    self.retryUpload = gConfig.getValue(self.section + '/RetryUpload', False)
    self.dm = DataManager()
    self.fc = FileCatalog()
    self.log.verbose('===========================================================================')
    self.log.verbose('Version %s' % (__RCSID__))
    self.log.verbose(self.diracVersion)
    self.currentPID = os.getpid()
    self.log.verbose('Job Wrapper started under PID: %s' % self.currentPID)
    # Define a new process group for the job wrapper
    self.parentPGID = os.getpgid(self.currentPID)
    self.log.verbose('Job Wrapper parent process group ID: %s' % self.parentPGID)
    os.setpgid(self.currentPID, self.currentPID)
    self.currentPGID = os.getpgid(self.currentPID)
    self.log.verbose('Job Wrapper process group ID: %s' % self.currentPGID)
    self.log.verbose('==========================================================================')
    self.log.verbose('sys.path is: \n%s' % '\n'.join(sys.path))
    self.log.verbose('==========================================================================')
    if 'PYTHONPATH' not in os.environ:
      self.log.verbose('PYTHONPATH is: null')
    else:
      pypath = os.environ['PYTHONPATH']
      self.log.verbose('PYTHONPATH is: \n%s' % '\n'.join(pypath.split(':')))
      self.log.verbose('==========================================================================')
    if 'LD_LIBRARY_PATH_SAVE' in os.environ:
      if 'LD_LIBRARY_PATH' in os.environ:
        os.environ['LD_LIBRARY_PATH'] += ':' + os.environ['LD_LIBRARY_PATH_SAVE']
      else:
        os.environ['LD_LIBRARY_PATH'] = os.environ['LD_LIBRARY_PATH_SAVE']

    if 'LD_LIBRARY_PATH' not in os.environ:
      self.log.verbose('LD_LIBRARY_PATH is: null')
    else:
      ldpath = os.environ['LD_LIBRARY_PATH']
      self.log.verbose('LD_LIBRARY_PATH is: \n%s' % '\n'.join(ldpath.split(':')))
      self.log.verbose('==========================================================================')
    if not self.cleanUpFlag:
      self.log.verbose('CleanUp Flag is disabled by configuration')
    # Failure flag
    self.failedFlag = True
    # Set defaults for some global parameters to be defined for the accounting report
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
    self.numberOfProcessors = 1
    self.jobAccountingSent = False

    self.jobArgs = {}
    self.optArgs = {}
    self.ceArgs = {}

  #############################################################################
  def initialize(self, arguments):
    """ Initializes parameters and environment for job.
    """
    self.__report(status=JobStatus.RUNNING,
                  minorStatus=JobMinorStatus.JOB_INITIALIZATION)
    self.log.info('Starting Job Wrapper Initialization for Job', self.jobID)
    self.jobArgs = arguments['Job']
    self.log.verbose(self.jobArgs)
    self.ceArgs = arguments['CE']
    self.log.verbose(self.ceArgs)
    self.__setInitialJobParameters()
    self.optArgs = arguments.get('Optimizer', {})
    # Fill some parameters for the accounting report
    self.owner = self.jobArgs.get('Owner', self.owner)
    self.jobGroup = self.jobArgs.get('JobGroup', self.jobGroup)
    self.jobType = self.jobArgs.get('JobType', self.jobType)
    dataParam = self.jobArgs.get('InputData', [])
    if dataParam and not isinstance(dataParam, list):
      dataParam = [dataParam]
    self.inputDataFiles = len(dataParam)
    dataParam = self.jobArgs.get('OutputData', [])
    if dataParam and not isinstance(dataParam, list):
      dataParam = [dataParam]
    self.outputDataFiles = len(dataParam)
    self.processingType = self.jobArgs.get('ProcessingType', self.processingType)
    self.userGroup = self.jobArgs.get('OwnerGroup', self.userGroup)
    self.jobClass = self.jobArgs.get('JobSplitType', self.jobClass)

    # Prepare the working directory, cd to there, and copying eventual extra arguments in it
    if self.jobID:
      if os.path.exists(str(self.jobID)):
        shutil.rmtree(str(self.jobID))
      os.mkdir(str(self.jobID))
      os.chdir(str(self.jobID))
      extraOpts = self.jobArgs.get('ExtraOptions', '')
      if extraOpts and 'dirac-jobexec' in self.jobArgs.get('Executable', '').strip():
        if os.path.exists('%s/%s' % (self.root, extraOpts)):
          shutil.copyfile('%s/%s' % (self.root, extraOpts), extraOpts)
        self.__loadLocalCFGFiles(self.localSiteRoot)

    else:
      self.log.info('JobID is not defined, running in current directory')

    with open('job.info', 'w') as infoFile:
      infoFile.write(self.__dictAsInfoString(self.jobArgs, '/Job'))

    self.log.debug("Environment used")
    self.log.debug("================")
    self.log.debug(json.dumps(dict(os.environ), indent=4))

  #############################################################################
  def __setInitialJobParameters(self):
    """Sets some initial job parameters
    """
    parameters = []
    parameters.append(('Pilot_Reference', self.ceArgs.get('PilotReference', self.pilotRef)))
    if 'LocalSE' in self.ceArgs:
      parameters.append(('AgentLocalSE', ','.join(self.ceArgs['LocalSE'])))
    if 'CPUScalingFactor' in self.ceArgs:
      parameters.append(('CPUScalingFactor', self.ceArgs['CPUScalingFactor']))
    if 'CPUNormalizationFactor' in self.ceArgs:
      parameters.append(('CPUNormalizationFactor', self.ceArgs['CPUNormalizationFactor']))
    if self.boincUserID:
      parameters.append(('BoincUserID', self.boincUserID))

    parameters.append(('PilotAgent', self.diracVersion))
    parameters.append(('JobWrapperPID', self.currentPID))
    result = self.__setJobParamList(parameters)
    return result

  #############################################################################
  def __loadLocalCFGFiles(self, localRoot):
    """Loads any extra CFG files residing in the local DIRAC site root.
    """
    files = os.listdir(localRoot)
    self.log.debug('Checking directory %s for *.cfg files' % localRoot)
    for localFile in files:
      if re.search('.cfg$', localFile):
        gConfig.loadFile('%s/%s' % (localRoot, localFile))
        self.log.verbose("Found local .cfg file '%s'" % localFile)

  #############################################################################
  def __dictAsInfoString(self, dData, infoString='', currentBase=""):
    for key in dData:
      value = dData[key]
      if isinstance(value, dict):
        infoString = self.__dictAsInfoString(value, infoString, "%s/%s" % (currentBase, key))
      elif isinstance(value, (list, tuple)):
        if value and value[0] == '[':
          infoString += "%s/%s = %s\n" % (currentBase, key, " ".join(value))
        else:
          infoString += "%s/%s = %s\n" % (currentBase, key, ", ".join(value))
      else:
        infoString += "%s/%s = %s\n" % (currentBase, key, str(value))

    return infoString

  #############################################################################
  def execute(self):
    """The main execution method of the Job Wrapper
    """
    self.log.info('Job Wrapper is starting execution phase for job %s' % (self.jobID))
    os.environ['DIRACJOBID'] = str(self.jobID)
    os.environ['DIRACROOT'] = self.localSiteRoot
    self.log.verbose('DIRACROOT = %s' % (self.localSiteRoot))
    os.environ['DIRACSITE'] = DIRAC.siteName()
    self.log.verbose('DIRACSITE = %s' % (DIRAC.siteName()))

    os.environ['DIRAC_PROCESSORS'] = str(self.ceArgs.get('Processors', 1))
    self.log.verbose('DIRAC_PROCESSORS = %s' % (self.ceArgs.get('Processors', 1)))

    os.environ['DIRAC_WHOLENODE'] = str(self.ceArgs.get('WholeNode', False))
    self.log.verbose('DIRAC_WHOLENODE = %s' % (self.ceArgs.get('WholeNode', False)))

    errorFile = self.jobArgs.get('StdError', self.defaultErrorFile)
    outputFile = self.jobArgs.get('StdOutput', self.defaultOutputFile)

    if 'CPUTime' in self.jobArgs:
      jobCPUTime = int(self.jobArgs['CPUTime'])
    else:
      self.log.info('Job has no CPU time limit specified, ',
                    'applying default of %s to %s' % (self.defaultCPUTime, self.jobID))
      jobCPUTime = self.defaultCPUTime
    self.numberOfProcessors = int(self.jobArgs.get('NumberOfProcessors', 1))

    jobMemory = 0.
    if "Memory" in self.jobArgs:
      # Job specifies memory in GB, internally use KB
      jobMemory = int(self.jobArgs['Memory']) * 1024. * 1024.

    if 'Executable' in self.jobArgs:
      executable = self.jobArgs['Executable'].strip()  # This is normally dirac-jobexec script, but not necessarily
    else:
      msg = 'Job %s has no specified executable' % (self.jobID)
      self.log.warn(msg)
      return S_ERROR(msg)

    # In case the executable is dirac-jobexec,
    # the argument should include the jobDescription.xml file
    jobArguments = self.jobArgs.get('Arguments', '')

    # This is a workaround for Python 2 style installations
    if six.PY3 and executable == "$DIRACROOT/scripts/dirac-jobexec":
      self.log.warn(
          'Replaced job executable "$DIRACROOT/scripts/dirac-jobexec" with '
          '"dirac-jobexec". Please fix your submission script!'
      )
      executable = "dirac-jobexec"

    executable = os.path.expandvars(executable)
    exeThread = None
    spObject = None

    if re.search('DIRACROOT', executable):
      executable = executable.replace('$DIRACROOT', self.localSiteRoot)
      self.log.verbose('Replaced $DIRACROOT for executable as %s' % (self.localSiteRoot))

    # Try to find the executable on PATH
    if "/" not in executable:
      # Returns None if the executable is not found so use "or" to leave it unchanged
      executable = distutils.spawn.find_executable(executable) or executable

    # Make the full path since . is not always in the PATH
    executable = os.path.abspath(executable)
    if not os.access(executable, os.X_OK):
      try:
        os.chmod(executable, stat.S_IRWXU | stat.S_IRWXG | stat.S_IROTH | stat.S_IXOTH)
      except OSError:
        self.log.warn('Failed to change mode to 775 for the executable', executable)

    exeEnv = dict(os.environ)
    if 'ExecutionEnvironment' in self.jobArgs:
      self.log.verbose('Adding variables to execution environment')
      variableList = self.jobArgs['ExecutionEnvironment']
      if isinstance(variableList, six.string_types):
        variableList = [variableList]
      for var in variableList:
        nameEnv = var.split('=')[0]
        valEnv = urlunquote(var.split('=')[1])
        exeEnv[nameEnv] = valEnv
        self.log.verbose('%s = %s' % (nameEnv, valEnv))

    if os.path.exists(executable):
      # it's in fact not yet running: it will be in few lines
      self.__report(status=JobStatus.RUNNING, minorStatus=JobMinorStatus.APPLICATION, sendFlag=True)
      spObject = Subprocess(timeout=False, bufferLimit=int(self.bufferLimit))
      command = executable
      if jobArguments:
        command += ' ' + jobArguments
      self.log.verbose('Execution command: %s' % (command))
      maxPeekLines = self.maxPeekLines
      exeThread = ExecutionThread(spObject, command, maxPeekLines, outputFile, errorFile, exeEnv)
      exeThread.start()
      time.sleep(10)
      payloadPID = spObject.getChildPID()
      if not payloadPID:
        return S_ERROR('Payload process could not start after 10 seconds')
    else:
      self.__report(status=JobStatus.FAILED, minorStatus=JobMinorStatus.APP_NOT_FOUND, sendFlag=True)
      return S_ERROR('Path to executable %s not found' % (executable))

    self.__setJobParam('PayloadPID', payloadPID)

    watchdog = Watchdog(pid=self.currentPID,
                        exeThread=exeThread,
                        spObject=spObject,
                        jobCPUTime=jobCPUTime,
                        memoryLimit=jobMemory,
                        processors=self.numberOfProcessors,
                        jobArgs=self.jobArgs)

    self.log.verbose('Initializing Watchdog instance')
    watchdog.initialize()
    self.log.verbose('Calibrating Watchdog instance')
    watchdog.calibrate()
    # do not kill Test jobs by CPU time
    if self.jobArgs.get('JobType', '') == 'Test':
      watchdog.testCPUConsumed = False

    if 'DisableCPUCheck' in self.jobArgs:
      watchdog.testCPUConsumed = False

    if exeThread.is_alive():
      self.log.info('Application thread is started in Job Wrapper')
      watchdog.run()
    else:
      self.log.warn('Application thread stopped very quickly...')

    if exeThread.is_alive():
      self.log.warn('Watchdog exited before completion of execution thread')
      while exeThread.is_alive():
        time.sleep(5)

    outputs = None
    if 'Thread' in EXECUTION_RESULT:
      threadResult = EXECUTION_RESULT['Thread']
      if not threadResult['OK']:
        self.log.error('Failed to execute the payload', threadResult['Message'])

        self.__report(status=JobStatus.FAILED, minorStatus=JobMinorStatus.APP_THREAD_FAILED, sendFlag=True)
        if 'Value' in threadResult:
          outs = threadResult['Value']
        if outs:
          self.__setJobParam('ApplicationError', outs[0], sendFlag=True)
        else:
          self.__setJobParam('ApplicationError', 'None reported', sendFlag=True)
      else:
        outputs = threadResult['Value']
    else:  # if the execution thread didn't complete
      self.log.error('Application thread did not complete')
      self.__report(status=JobStatus.FAILED, minorStatus=JobMinorStatus.APP_THREAD_NOT_COMPLETE, sendFlag=True)
      self.__setJobParam('ApplicationError', JobMinorStatus.APP_THREAD_NOT_COMPLETE, sendFlag=True)
      return S_ERROR('No outputs generated from job execution')

    if 'CPU' in EXECUTION_RESULT:
      cpuString = ' '.join(['%.2f' % x for x in EXECUTION_RESULT['CPU']])
      self.log.info('EXECUTION_RESULT[CPU] in JobWrapper execute', cpuString)

    if watchdog.checkError:
      # In this case, the Watchdog has killed the Payload and the ExecutionThread can not get the CPU statistics
      # os.times only reports for waited children
      # Take the CPU from the last value recorded by the Watchdog
      self.__report(status=JobStatus.FAILED, minorStatus=watchdog.checkError, sendFlag=True)
      if 'CPU' in EXECUTION_RESULT:
        if 'LastUpdateCPU(s)' in watchdog.currentStats:
          EXECUTION_RESULT['CPU'][0] = 0
          EXECUTION_RESULT['CPU'][0] = 0
          EXECUTION_RESULT['CPU'][0] = 0
          EXECUTION_RESULT['CPU'][0] = watchdog.currentStats['LastUpdateCPU(s)']

    if watchdog.currentStats:
      self.log.info('Statistics collected by the Watchdog:\n ',
                    '\n  '.join(['%s: %s' % items for items in watchdog.currentStats.items()]))  # can be an iterator
    if outputs:
      status = threadResult['Value'][0]  # the status of the payload execution
      # Send final heartbeat of a configurable number of lines here
      self.log.verbose('Sending final application standard output heartbeat')
      self.__sendFinalStdOut(exeThread)
      self.log.verbose('Execution thread status = %s' % (status))

      if not watchdog.checkError and not status:
        self.failedFlag = False
        self.__report(status=JobStatus.COMPLETING, minorStatus=JobMinorStatus.APP_SUCCESS, sendFlag=True)
      elif not watchdog.checkError:
        self.__report(status=JobStatus.COMPLETING, minorStatus=JobMinorStatus.APP_ERRORS, sendFlag=True)
        if status in (DErrno.EWMSRESC, DErrno.EWMSRESC & 255):  # the status will be truncated to 0xDE (222)
          self.log.verbose("job will be rescheduled")
          self.__report(status=JobStatus.COMPLETING, minorStatus=JobMinorStatus.GOING_RESCHEDULE, sendFlag=True)
          return S_ERROR(DErrno.EWMSRESC, 'Job will be rescheduled')

    else:
      return S_ERROR('No outputs generated from job execution')

    self.log.info('Checking directory contents after execution:')
    res = systemCall(5, ['ls', '-al'])
    if not res['OK']:
      self.log.error('Failed to list the current directory', res['Message'])
    elif res['Value'][0]:
      self.log.error('Failed to list the current directory', res['Value'][2])
    else:
      # no timeout and exit code is 0
      self.log.info(res['Value'][1])

    return S_OK()

  #############################################################################
  def __sendFinalStdOut(self, exeThread):
    """After the Watchdog process has finished, this function sends a final
       report to be presented in the StdOut in the web page via the heartbeat
       mechanism.
    """
    cpuConsumed = self.__getCPU()['Value']
    self.log.info('Total CPU Consumed is: %s' % cpuConsumed[1])
    self.__setJobParam('TotalCPUTime(s)', cpuConsumed[0])
    normCPU = cpuConsumed[0] * self.cpuNormalizationFactor
    self.__setJobParam('NormCPUTime(s)', normCPU)
    if self.cpuNormalizationFactor:
      self.log.info('Normalized CPU Consumed is:', normCPU)

    result = exeThread.getOutput(self.maxPeekLines)
    if not result['OK']:
      lines = 0
      appStdOut = ''
    else:
      lines = len(result['Value'])
      appStdOut = '\n'.join(result['Value'])

    header = 'Last %s lines of application output from JobWrapper on %s :' % (lines, Time.toString())
    border = '=' * len(header)

    cpuTotal = 'CPU Total: %s (h:m:s)' % cpuConsumed[1]
    cpuTotal += " Normalized CPU Total %.1f s @ HEP'06" % normCPU
    header = '\n%s\n%s\n%s\n%s\n' % (border, header, cpuTotal, border)
    appStdOut = header + appStdOut
    self.log.info(appStdOut)
    heartBeatDict = {}
    staticParamDict = {'StandardOutput': appStdOut}
    if self.jobID:
      result = JobStateUpdateClient().sendHeartBeat(self.jobID, heartBeatDict, staticParamDict)
      if not result['OK']:
        self.log.error('Problem sending final heartbeat from JobWrapper', result['Message'])

    return

  #############################################################################
  def __getCPU(self):
    """Uses os.times() to get CPU time and returns HH:MM:SS after conversion.
    """
    # TODO: normalize CPU consumed via scale factor
    cpuString = ' '.join(['%.2f' % x for x in EXECUTION_RESULT['CPU']])
    self.log.info('EXECUTION_RESULT[CPU] in __getCPU', cpuString)
    utime, stime, cutime, cstime, _elapsed = EXECUTION_RESULT['CPU']
    cpuTime = utime + stime + cutime + cstime
    self.log.verbose("Total CPU time consumed = %s" % (cpuTime))
    result = self.__getCPUHMS(cpuTime)
    return result

  #############################################################################
  def __getCPUHMS(self, cpuTime):
    mins, secs = divmod(cpuTime, 60)
    hours, mins = divmod(mins, 60)
    humanTime = '%02d:%02d:%02d' % (hours, mins, secs)
    self.log.verbose('Human readable CPU time is: %s' % humanTime)
    return S_OK((cpuTime, humanTime))

  #############################################################################
  def resolveInputData(self):
    """Input data is resolved here using a VO specific plugin module.
    """
    self.__report(status=JobStatus.RUNNING, minorStatus=JobMinorStatus.INPUT_DATA_RESOLUTION, sendFlag=True)

    # What is this input data? - and exit if there's no input
    inputData = self.jobArgs['InputData']
    if not inputData:
      msg = "Job Wrapper cannot resolve local replicas of input data with null job input data parameter "
      self.log.error(msg)
      return S_ERROR(msg)
    else:
      if isinstance(inputData, six.string_types):
        inputData = [inputData]
      lfns = [fname.replace('LFN:', '') for fname in inputData]
      self.log.verbose('Job input data requirement is \n%s' % ',\n'.join(lfns))

    # Does this site have local SEs? - not failing if it doesn't
    if 'LocalSE' in self.ceArgs:
      localSEList = self.ceArgs['LocalSE']
    else:
      localSEList = gConfig.getValue('/LocalSite/LocalSE', [])

    if not localSEList:
      self.log.warn("Job has input data requirement but no site LocalSE defined")
    else:
      if isinstance(localSEList, six.string_types):
        localSEList = List.fromChar(localSEList)
      self.log.info("Site has the following local SEs: %s" % ', '.join(localSEList))

    # How to get this data?
    if 'InputDataModule' not in self.jobArgs:
      self.log.warn("Job has no input data resolution module specified, using the default one")
      inputDataPolicy = 'DIRAC.WorkloadManagementSystem.Client.InputDataResolution'
    else:
      inputDataPolicy = self.jobArgs['InputDataModule']

    self.log.verbose("Job input data resolution policy module is %s" % (inputDataPolicy))

    # Now doing the real stuff
    optReplicas = {}
    if self.optArgs:
      try:
        optDict, _length = DEncode.decode(self.optArgs['InputData'])
        optReplicas = optDict['Value']
        self.log.info('Found optimizer catalog result')
        self.log.verbose(optReplicas)
      except Exception as x:
        self.log.warn(str(x))
        self.log.warn('Optimizer information could not be converted to a dictionary will call catalog directly')

    result = self.__checkFileCatalog(lfns, optReplicas)
    if not result['OK']:
      self.log.info('Could not obtain replica information from Optimizer File Catalog information')
      self.log.warn(result)
      result = self.__checkFileCatalog(lfns)
      if not result['OK']:
        self.log.warn('Could not obtain replica information from File Catalog directly')
        self.log.warn(result)
        return S_ERROR(result['Message'])
      else:
        resolvedData = result
    else:
      resolvedData = result

    # add input data size to accounting report (since resolution successful)
    for lfn, mdata in resolvedData['Value']['Successful'].items():  # can be an iterator
      if 'Size' in mdata:
        lfnSize = mdata['Size']
        if not isinstance(lfnSize, six.integer_types):
          try:
            lfnSize = int(lfnSize)
          except ValueError:
            lfnSize = 0
            self.log.info('File size for LFN was not an integer, setting size to 0', lfn)
        self.inputDataSize += lfnSize

    configDict = {'JobID': self.jobID,
                  'LocalSEList': localSEList,
                  'DiskSEList': self.diskSE,
                  'TapeSEList': self.tapeSE}
    self.log.info(configDict)
    argumentsDict = {'FileCatalog': resolvedData, 'Configuration': configDict, 'InputData': lfns, 'Job': self.jobArgs}
    self.log.info(argumentsDict)
    moduleFactory = ModuleFactory()
    self.log.verbose("Now starting execution of input data policy module")
    moduleInstance = moduleFactory.getModule(inputDataPolicy, argumentsDict)
    if not moduleInstance['OK']:
      return moduleInstance

    module = moduleInstance['Value']
    result = module.execute()
    if not result['OK']:
      self.log.warn('Input data resolution failed')
      return result

    return S_OK()

  #############################################################################
  def __checkFileCatalog(self, lfns, optReplicaInfo=None):
    """This function returns dictionaries containing all relevant parameters
       to allow data access from the relevant file catalogue.  Optionally, optimizer
       parameters can be supplied here but if these are not sufficient, the file catalogue
       is subsequently consulted.

       N.B. this will be considerably simplified when the DMS evolves to have a
       generic FC interface and a single call for all available information.
    """
    replicas = optReplicaInfo
    if not replicas:
      replicas = self.__getReplicaMetadata(lfns)
      if not replicas['OK']:
        return replicas

    self.log.verbose(replicas)

    failedGUIDs = []
    for lfn, reps in replicas['Value']['Successful'].items():  # can be an iterator
      if 'GUID' not in reps:
        failedGUIDs.append(lfn)

    if failedGUIDs:
      self.log.info('The following file(s) were found not to have a GUID:\n%s' % ',\n'.join(failedGUIDs))

    if failedGUIDs:
      return S_ERROR('File metadata is not available')
    return replicas

  #############################################################################
  def __getReplicaMetadata(self, lfns):
    """ Wrapper function to consult catalog for all necessary file metadata
        and check the result.
    """
    start = time.time()
    # We are in a job, therefore interested in replicas for jobs
    repsResult = self.dm.getReplicasForJobs(lfns)
    timing = time.time() - start
    self.log.info('Replica Lookup Time: %.2f seconds ' % (timing))
    if not repsResult['OK']:
      self.log.warn(repsResult['Message'])
      return repsResult

    badLFNCount = 0
    badLFNs = []
    catalogResult = repsResult['Value']

    for lfn, cause in catalogResult.get('Failed', {}).items():  # can be an iterator
      badLFNCount += 1
      badLFNs.append('LFN:%s Problem: %s' % (lfn, cause))

    for lfn, replicas in catalogResult.get('Successful', {}).items():  # can be an iterator
      if not replicas:
        badLFNCount += 1
        badLFNs.append('LFN:%s Problem: Null replica value' % (lfn))

    if badLFNCount:
      self.log.warn('Job Wrapper found %s problematic LFN(s) for job %s' % (badLFNCount, self.jobID))
      param = '\n'.join(badLFNs)
      self.log.info(param)
      self.__setJobParam('MissingLFNs', param)
      return S_ERROR(JobMinorStatus.INPUT_NOT_AVAILABLE)

    # Must retrieve GUIDs from FC for files
    start = time.time()
    guidDict = self.fc.getFileMetadata(lfns)
    timing = time.time() - start
    self.log.info('GUID Lookup Time: %.2f seconds ' % (timing))
    if not guidDict['OK']:
      self.log.warn('Failed to retrieve GUIDs from file catalog')
      self.log.warn(guidDict['Message'])
      return guidDict

    failed = guidDict['Value']['Failed']
    if failed:
      self.log.warn('Could not retrieve GUIDs from catalog for the following files')
      self.log.warn(failed)
      return S_ERROR('Missing GUIDs')

    for lfn, reps in repsResult['Value']['Successful'].items():  # can be an iterator
      guidDict['Value']['Successful'][lfn].update(reps)

    catResult = guidDict
    return catResult

  #############################################################################
  def processJobOutputs(self):
    """Outputs for a job may be treated here.
    """

    # first iteration of this, no checking of wildcards or oversize sandbox files etc.
    outputSandbox = self.jobArgs.get('OutputSandbox', [])
    if isinstance(outputSandbox, six.string_types):
      outputSandbox = [outputSandbox]
    if outputSandbox:
      self.log.verbose('OutputSandbox files are: %s' % ', '.join(outputSandbox))
    outputData = self.jobArgs.get('OutputData', [])
    if outputData and isinstance(outputData, six.string_types):
      outputData = outputData.split(';')
    if outputData:
      self.log.verbose('OutputData files are: %s' % ', '.join(outputData))

    # First resolve any wildcards for output files and work out if any files are missing
    resolvedSandbox = self.__resolveOutputSandboxFiles(outputSandbox)
    if not resolvedSandbox['OK']:
      self.log.warn('Output sandbox file resolution failed:')
      self.log.warn(resolvedSandbox['Message'])
      self.__report(status=JobStatus.FAILED, minorStatus=JobMinorStatus.RESOLVING_OUTPUT_SANDBOX)

    fileList = resolvedSandbox['Value']['Files']
    missingFiles = resolvedSandbox['Value']['Missing']
    if missingFiles:
      self.jobReport.setJobParameter('OutputSandboxMissingFiles', ', '.join(missingFiles), sendFlag=False)

    if 'Owner' not in self.jobArgs:
      msg = 'Job has no owner specified'
      self.log.warn(msg)
      return S_OK(msg)

    # Do not overwrite in case of Error
    if not self.failedFlag:
      self.__report(status=JobStatus.COMPLETING, minorStatus=JobMinorStatus.UPLOADING_OUTPUT_SANDBOX)

    uploadOutputDataInAnyCase = False

    if fileList and self.jobID:
      self.outputSandboxSize = getGlobbedTotalSize(fileList)
      self.log.info('Attempting to upload Sandbox with limit:', self.sandboxSizeLimit)
      sandboxClient = SandboxStoreClient()
      result_sbUpload = sandboxClient.uploadFilesAsSandboxForJob(
          fileList, self.jobID, 'Output', self.sandboxSizeLimit)  # 1024*1024*10
      if not result_sbUpload['OK']:
        self.log.error('Output sandbox upload failed with message', result_sbUpload['Message'])
        outputSandboxData = result_sbUpload.get('SandboxFileName')
        if outputSandboxData:

          self.log.info('Attempting to upload %s as output data' % (outputSandboxData))
          if self.failedFlag:
            outputData = [outputSandboxData]
            uploadOutputDataInAnyCase = True
          else:
            outputData.append(outputSandboxData)
          self.jobReport.setJobParameter(
              'OutputSandbox', 'Sandbox uploaded to grid storage', sendFlag=False)
          self.jobReport.setJobParameter(
              'OutputSandboxLFN',
              self.__getLFNfromOutputFile(outputSandboxData)[0], sendFlag=False)
        else:
          self.log.info('Could not get SandboxFileName to attempt upload to Grid storage')
          return S_ERROR('Output sandbox upload failed and no file name supplied for failover to Grid storage')
      else:
        # Do not overwrite in case of Error
        if not self.failedFlag:
          self.__report(status=JobStatus.COMPLETING, minorStatus=JobMinorStatus.OUTPUT_SANDBOX_UPLOADED)
        self.log.info('Sandbox uploaded successfully')

    if (outputData and not self.failedFlag) or uploadOutputDataInAnyCase:
      # Do not upload outputdata if the job has failed.
      # The exception is when the outputData is what was the OutputSandbox, which should be uploaded in any case
      outputSE = self.jobArgs.get('OutputSE', self.defaultOutputSE)
      if isinstance(outputSE, six.string_types):
        outputSE = [outputSE]

      outputPath = self.jobArgs.get('OutputPath', self.defaultOutputPath)
      if not isinstance(outputPath, six.string_types):
        outputPath = self.defaultOutputPath

      if not outputSE and not self.defaultFailoverSE:
        return S_ERROR('No output SEs defined in VO configuration')

      result_transferODF = self.__transferOutputDataFiles(outputData, outputSE, outputPath)

      # now that we (tried to) transfer the output files,
      # including possibly oversized Output Sandboxes,
      # we delete the local output sandbox tarfile in case it's still there.
      if not result_sbUpload['OK']:
        outputSandboxData = result_sbUpload.get('SandboxFileName')
        if outputSandboxData:
          try:
            os.unlink(outputSandboxData)
          except OSError:
            pass

      if not result_transferODF['OK']:
        return result_transferODF

    return S_OK('Job outputs processed')

  #############################################################################
  def __resolveOutputSandboxFiles(self, outputSandbox):
    """Checks the output sandbox file list and resolves any specified wildcards.
       Also tars any specified directories.
    """
    missing = []
    okFiles = []
    for i in outputSandbox:
      self.log.verbose('Looking at OutputSandbox file/directory/wildcard: %s' % i)
      globList = glob.glob(i)
      for check in globList:
        if os.path.isfile(check):
          self.log.verbose('Found locally existing OutputSandbox file: %s' % check)
          okFiles.append(check)
        if os.path.isdir(check):
          self.log.verbose('Found locally existing OutputSandbox directory: %s' % check)
          cmd = ['tar', 'cf', '%s.tar' % check, check]
          result = systemCall(60, cmd)
          if not result['OK']:
            self.log.error('Failed to create OutputSandbox tar', result['Message'])
          elif result['Value'][0]:
            self.log.error('Failed to create OutputSandbox tar', result['Value'][2])
          if os.path.isfile('%s.tar' % (check)):
            self.log.verbose('Appending %s.tar to OutputSandbox' % check)
            okFiles.append('%s.tar' % (check))
          else:
            self.log.warn('Could not tar OutputSandbox directory: %s' % check)
            missing.append(check)

    for i in outputSandbox:
      if i not in okFiles:
        if not '%s.tar' % i in okFiles:
          if not re.search(r'\*', i):
            if i not in missing:
              missing.append(i)

    result = {'Missing': missing, 'Files': okFiles}
    return S_OK(result)

  #############################################################################
  def __transferOutputDataFiles(self, outputData, outputSE, outputPath):
    """ Performs the upload and registration in the File Catalog(s)
    """
    self.log.verbose('Uploading output data files')
    self.__report(status=JobStatus.COMPLETING, minorStatus=JobMinorStatus.UPLOADING_OUTPUT_DATA)
    self.log.info('Output data files %s to be uploaded to %s SE' % (', '.join(outputData), outputSE))
    missing = []
    uploaded = []

    # Separate outputdata in the form of lfns and local files
    lfnList = []
    nonlfnList = []
    for out in outputData:
      if out.lower().find('lfn:') != -1:
        lfnList.append(out)
      else:
        nonlfnList.append(out)

    # Check whether list of outputData has a globbable pattern
    globbedOutputList = List.uniqueElements(getGlobbedFiles(nonlfnList))
    if globbedOutputList != nonlfnList and globbedOutputList:
      self.log.info('Found a pattern in the output data file list, files to upload are:',
                    ', '.join(globbedOutputList))
      nonlfnList = globbedOutputList
    outputData = lfnList + nonlfnList

    pfnGUID = {}
    result = getGUID(outputData)
    if not result['OK']:
      self.log.warn('Failed to determine POOL GUID(s) for output file list (OK if not POOL files)',
                    result['Message'])
    else:
      pfnGUID = result['Value']

    for outputFile in outputData:
      (lfn, localfile) = self.__getLFNfromOutputFile(outputFile, outputPath)
      if not os.path.exists(localfile):
        self.log.error('Missing specified output data file:', outputFile)
        continue

      # # file size
      localfileSize = getGlobbedTotalSize(localfile)

      self.outputDataSize += getGlobbedTotalSize(localfile)

      outputFilePath = os.path.join(os.getcwd(), localfile)

      # # file GUID
      fileGUID = pfnGUID[localfile] if localfile in pfnGUID else None
      if fileGUID:
        self.log.verbose('Found GUID for file from POOL XML catalogue %s' % localfile)

      # #  file checksum
      cksm = fileAdler(outputFilePath)

      fileMetaDict = {"Size": localfileSize,
                      "LFN": lfn,
                      "ChecksumType": "Adler32",
                      "Checksum": cksm,
                      "GUID": fileGUID}

      outputSEList = self.__getSortedSEList(outputSE)
      upload = self.failoverTransfer.transferAndRegisterFile(fileName=localfile,
                                                             localPath=outputFilePath,
                                                             lfn=lfn,
                                                             destinationSEList=outputSEList,
                                                             fileMetaDict=fileMetaDict,
                                                             fileCatalog=self.defaultCatalog,
                                                             masterCatalogOnly=self.masterCatalogOnlyFlag,
                                                             retryUpload=self.retryUpload)
      if upload['OK']:
        self.log.info('"%s" successfully uploaded to "%s" as "LFN:%s"' % (localfile,
                                                                          upload['Value']['uploadedSE'],
                                                                          lfn))
        uploaded.append(lfn)
        continue

      self.log.error('Could not putAndRegister file',
                     '%s with LFN %s to %s with GUID %s trying failover storage' % (localfile, lfn,
                                                                                    ', '.join(outputSEList),
                                                                                    fileGUID))
      if not self.defaultFailoverSE:
        self.log.info('No failover SEs defined for JobWrapper,',
                      'cannot try to upload output file %s anywhere else.' % outputFile)
        missing.append(outputFile)
        continue

      failoverSEs = self.__getSortedSEList(self.defaultFailoverSE)
      targetSE = outputSEList[0]
      result = self.failoverTransfer.transferAndRegisterFileFailover(fileName=localfile,
                                                                     localPath=outputFilePath,
                                                                     lfn=lfn,
                                                                     targetSE=targetSE,
                                                                     failoverSEList=failoverSEs,
                                                                     fileMetaDict=fileMetaDict,
                                                                     fileCatalog=self.defaultCatalog,
                                                                     masterCatalogOnly=self.masterCatalogOnlyFlag)
      if not result['OK']:
        self.log.error('Completely failed to upload file to failover SEs', result['Message'])
        missing.append(outputFile)
      else:
        self.log.info('File %s successfully uploaded to failover storage element' % lfn)
        uploaded.append(lfn)

    # For files correctly uploaded must report LFNs to job parameters
    if uploaded:
      report = ', '.join(uploaded)
      # In case the VO payload has also uploaded data using the same parameter
      # name this should be checked prior to setting.
      result = JobMonitoringClient().getJobParameter(int(self.jobID), 'UploadedOutputData')
      if result['OK']:
        if 'UploadedOutputData' in result['Value']:
          report += ', %s' % result['Value']['UploadedOutputData']

      self.jobReport.setJobParameter(
          'UploadedOutputData', report, sendFlag=False)

    # TODO Notify the user of any output data / output sandboxes
    if missing:
      self.__setJobParam('OutputData', 'MissingFiles: %s' % ', '.join(missing))
      self.__report(status=JobStatus.FAILED, minorStatus=JobMinorStatus.UPLOADING_OUTPUT_DATA)
      return S_ERROR('Failed to upload OutputData')

    self.__report(status=JobStatus.COMPLETING, minorStatus=JobMinorStatus.OUTPUT_DATA_UPLOADED)
    return S_OK('OutputData uploaded successfully')

  #############################################################################
  def __getSortedSEList(self, seList):
    """ Randomize SE, putting first those that are Local/Close to the Site
    """
    if not seList:
      return seList

    localSEs = []
    otherSEs = []
    siteSEs = []
    seMapping = getSEsForSite(DIRAC.siteName())

    if seMapping['OK'] and seMapping['Value']:
      siteSEs = seMapping['Value']

    for seName in seList:
      if seName in siteSEs:
        localSEs.append(seName)
      else:
        otherSEs.append(seName)

    return List.randomize(localSEs) + List.randomize(otherSEs)

  #############################################################################
  def __getLFNfromOutputFile(self, outputFile, outputPath=''):
    """Provides a generic convention for VO output data
       files if no path is specified.
    """

    if not re.search('^LFN:', outputFile):
      localfile = outputFile
      initial = self.owner[:1]
      vo = getVOForGroup(self.userGroup)
      if not vo:
        vo = 'dirac'

      ops = Operations(vo=vo)
      user_prefix = ops.getValue("LFNUserPrefix", 'user')
      basePath = '/' + vo + '/' + user_prefix + '/' + initial + '/' + self.owner
      if outputPath:
        # If output path is given, append it to the user path and put output files in this directory
        if outputPath.startswith('/'):
          outputPath = outputPath[1:]
      else:
        # By default the output path is constructed from the job id
        subdir = str(int(self.jobID / 1000))
        outputPath = subdir + '/' + str(self.jobID)
      lfn = os.path.join(basePath, outputPath, os.path.basename(localfile))
    else:
      # if LFN is given, take it as it is
      localfile = os.path.basename(outputFile.replace("LFN:", ""))
      lfn = outputFile.replace("LFN:", "")

    return (lfn, localfile)

  #############################################################################
  def transferInputSandbox(self, inputSandbox):
    """Downloads the input sandbox for the job
    """
    sandboxFiles = []
    registeredISB = []
    lfns = []
    self.__report(status=JobStatus.RUNNING, minorStatus=JobMinorStatus.DOWNLOADING_INPUT_SANDBOX)
    if not isinstance(inputSandbox, (list, tuple)):
      inputSandbox = [inputSandbox]
    for isb in inputSandbox:
      if isb.find("LFN:") == 0 or isb.find("lfn:") == 0:
        lfns.append(isb)
      else:
        if isb.find("SB:") == 0:
          registeredISB.append(isb)
        else:
          sandboxFiles.append(os.path.basename(isb))

    self.log.info('Downloading InputSandbox for job %s: %s' % (self.jobID, ', '.join(sandboxFiles)))
    if os.path.exists('%s/inputsandbox' % (self.root)):
      # This is a debugging tool, get the file from local storage to debug Job Wrapper
      sandboxFiles.append('jobDescription.xml')
      for inputFile in sandboxFiles:
        if os.path.exists('%s/inputsandbox/%s' % (self.root, inputFile)):
          self.log.info('Getting InputSandbox file %s from local directory for testing' % (inputFile))
          shutil.copy(self.root + '/inputsandbox/' + inputFile, inputFile)
      result = S_OK(sandboxFiles)
    else:
      if registeredISB:
        for isb in registeredISB:
          self.log.info("Downloading Input SandBox %s" % isb)
          result = SandboxStoreClient().downloadSandbox(isb)
          if not result['OK']:
            self.__report(status=JobStatus.RUNNING, minorStatus=JobMinorStatus.FAILED_DOWNLOADING_INPUT_SANDBOX)
            return S_ERROR("Cannot download Input sandbox %s: %s" % (isb, result['Message']))
          else:
            self.inputSandboxSize += result['Value']

    if lfns:
      self.log.info("Downloading Input SandBox LFNs, number of files to get", len(lfns))
      self.__report(status=JobStatus.RUNNING, minorStatus=JobMinorStatus.DOWNLOADING_INPUT_SANDBOX_LFN)
      lfns = [fname.replace('LFN:', '').replace('lfn:', '') for fname in lfns]
      download = self.dm.getFile(lfns)
      if not download['OK']:
        self.log.warn(download)
        self.__report(status=JobStatus.RUNNING, minorStatus=JobMinorStatus.FAILED_DOWNLOADING_INPUT_SANDBOX_LFN)
        return S_ERROR(download['Message'])
      failed = download['Value']['Failed']
      if failed:
        self.log.warn('Could not download InputSandbox LFN(s)')
        self.log.warn(failed)
        return S_ERROR(str(failed))
      for lfn in lfns:
        if os.path.exists('%s/%s' % (self.root, os.path.basename(download['Value']['Successful'][lfn]))):
          sandboxFiles.append(os.path.basename(download['Value']['Successful'][lfn]))

    userFiles = sandboxFiles + [os.path.basename(lfn) for lfn in lfns]
    for possibleTarFile in userFiles:
      if not os.path.exists(possibleTarFile):
        continue
      try:
        if os.path.isfile(possibleTarFile) and tarfile.is_tarfile(possibleTarFile):
          self.log.info('Unpacking input sandbox file %s' % (possibleTarFile))
          with tarfile.open(possibleTarFile, 'r') as tarFile:
            for member in tarFile.getmembers():
              tarFile.extract(member, os.getcwd())
      except Exception as x:
        return S_ERROR('Could not untar %s with exception %s' % (possibleTarFile, str(x)))

    if userFiles:
      self.inputSandboxSize = getGlobbedTotalSize(userFiles)
      self.log.info("Total size of input sandbox:",
                    "%0.2f MiB (%s bytes)" % (self.inputSandboxSize / 1048576.0, self.inputSandboxSize))

    return S_OK('InputSandbox downloaded')

  #############################################################################
  def finalize(self):
    """Perform any final actions to clean up after job execution.
    """
    self.log.info('Running JobWrapper finalization')

    # find if there are pending failover requests
    requests = self.__getRequestFiles()
    outputDataRequest = self.failoverTransfer.getRequest()

    requestFlag = len(requests) > 0 or not outputDataRequest.isEmpty()

    finalStatus = ''
    finalMinorStatus = ''
    prString = "Job finished "
    if self.failedFlag:
      prString += "with errors"
      finalStatus = JobStatus.FAILED
    else:
      prString += "successfully"
      if requestFlag:
        finalStatus = JobStatus.COMPLETED
      else:
        finalStatus = JobStatus.DONE
        finalMinorStatus = JobMinorStatus.EXEC_COMPLETE

    # send the failover request if any (also for failed jobs)
    res = self.sendFailoverRequest()
    if not res['OK']:  # This means that the request could not be set (this should "almost never" happen)
      finalStatus = JobStatus.FAILED
      finalMinorStatus = JobMinorStatus.FAILED_SENDING_REQUESTS
      self.failedFlag = True
    elif res['Value']:
      # A request was sent, change the minor status
      finalMinorStatus = JobMinorStatus.PENDING_REQUESTS
      requestFlag = True

    # Set the final status of the job
    self.log.info(prString, "with%s pending requests" % ('' if requestFlag else ' no'))
    self.log.info("Final job status", "%s ; %s" % (finalStatus, finalMinorStatus))
    self.__report(status=finalStatus, minorStatus=finalMinorStatus, sendFlag=True)

    # Sending the last accounting report
    if not self.jobID:
      self.log.debug('No accounting to be sent since running locally')
    else:
      # Final status and minorStatus are already set
      result = self.sendJobAccounting()
      if not result['OK']:
        # This should be really rare, as the accounting report also sends failover requests
        self.log.warn('JobAccountingFailure',
                      "Could not send job accounting with result: \n%s" % result['Message'])
        self.log.warn('JobAccountingFailure',
                      "The job won't fail, but the accounting for this job won't be sent")

    self.__cleanUp()
    return 1 if self.failedFlag else 0

  #############################################################################
  def sendJobAccounting(self, status='', minorStatus=''):
    """Send WMS accounting data.
    """
    if self.jobAccountingSent:
      return S_OK()
    if status:
      self.wmsMajorStatus = status
    if minorStatus:
      self.wmsMinorStatus = minorStatus

    self.accountingReport.setEndTime()
    # CPUTime and ExecTime
    if 'CPU' not in EXECUTION_RESULT:
      # If the payload has not started execution (error with input data, SW, SB,...)
      # Execution result is not filled use self.initialTiming
      self.log.info('EXECUTION_RESULT[CPU] missing in sendJobAccounting')
      finalStat = os.times()
      EXECUTION_RESULT['CPU'] = []
      for i, _ in enumerate(finalStat):
        EXECUTION_RESULT['CPU'].append(finalStat[i] - self.initialTiming[i])

    cpuString = ' '.join(['%.2f' % x for x in EXECUTION_RESULT['CPU']])
    self.log.info('EXECUTION_RESULT[CPU] in sendJobAccounting', cpuString)

    utime, stime, cutime, cstime, elapsed = EXECUTION_RESULT['CPU']
    cpuTime = utime + stime + cutime + cstime
    execTime = elapsed
    diskSpaceConsumed = getGlobbedTotalSize(os.path.join(self.root, str(self.jobID)))
    # Fill the data
    acData = {'User': self.owner,
              'UserGroup': self.userGroup,
              'JobGroup': self.jobGroup,
              'JobType': self.jobType,
              'JobClass': self.jobClass,
              'ProcessingType': self.processingType,
              'FinalMajorStatus': self.wmsMajorStatus,
              'FinalMinorStatus': self.wmsMinorStatus,
              'CPUTime': cpuTime,
              # Based on the factor to convert raw CPU to Normalized units (based on the CPU Model)
              'NormCPUTime': cpuTime * self.cpuNormalizationFactor,
              'ExecTime': execTime * self.numberOfProcessors,
              'InputDataSize': self.inputDataSize,
              'OutputDataSize': self.outputDataSize,
              'InputDataFiles': self.inputDataFiles,
              'OutputDataFiles': self.outputDataFiles,
              'DiskSpace': diskSpaceConsumed,
              'InputSandBoxSize': self.inputSandboxSize,
              'OutputSandBoxSize': self.outputSandboxSize,
              'ProcessedEvents': self.processedEvents}
    self.log.verbose('Accounting Report is:')
    self.log.verbose(acData)
    self.accountingReport.setValuesFromDict(acData)
    result = self.accountingReport.commit()
    # Even if it fails a failover request will be created
    self.jobAccountingSent = True
    return result

  #############################################################################
  def sendFailoverRequest(self):
    """ Create and send a combined job failover request if any
    """
    request = Request()
    # Forbid the request to be executed within the next 2 minutes
    request.NotBefore = datetime.datetime.utcnow() + datetime.timedelta(seconds=120)

    requestName = 'job_%s' % self.jobID
    if 'JobName' in self.jobArgs:
      # To make the request names more appealing for users
      jobName = self.jobArgs['JobName']
      if isinstance(jobName, six.string_types) and jobName:
        jobName = jobName.replace(' ', '').replace('(', '').replace(')', '').replace('"', '')
        jobName = jobName.replace('.', '').replace('{', '').replace('}', '').replace(':', '')
        requestName = '%s_%s' % (jobName, requestName)

    request.RequestName = requestName.replace('"', '')
    request.JobID = self.jobID
    request.SourceComponent = "Job_%s" % self.jobID

    # JobReport part first
    result = self.jobReport.generateForwardDISET()
    if result['OK']:
      if isinstance(result["Value"], Operation):
        self.log.info('Adding a job state update DISET operation to the request')
        request.addOperation(result["Value"])
    else:
      self.log.warn('JobReportFailure', "Could not generate a forwardDISET operation: %s" % result['Message'])
      self.log.warn('JobReportFailure', "The job won't fail, but the jobLogging info might be incomplete")

    # Failover transfer requests
    for storedOperation in self.failoverTransfer.request:
      request.addOperation(storedOperation)

    # Any other requests in the current directory
    rfiles = self.__getRequestFiles()
    for rfname in rfiles:
      with open(rfname, 'r') as rFile:
        requestStored = Request(json.load(rFile))
      for storedOperation in requestStored:
        request.addOperation(storedOperation)

    if len(request):
      # The request is ready, send it now
      isValid = RequestValidator().validate(request)
      if not isValid["OK"]:
        self.log.error("Failover request is not valid", isValid["Message"])
        self.log.error("Job will fail, first trying to print out the content of the request")
        reqToJSON = request.toJSON()
        if reqToJSON['OK']:
          print(str(reqToJSON['Value']))
        else:
          self.log.error("Something went wrong creating the JSON from request", reqToJSON['Message'])
        return S_ERROR()
      else:
        # We try several times to put the request before failing the job:
        # it is very important that requests go through,
        # or the job will be in an unclear status
        # (workflow ok, but, e.g., the output files won't be registered).
        # It's a poor man solution, but I don't see fancy alternatives
        for counter in range(10):
          requestClient = ReqClient()
          result = requestClient.putRequest(request)
          if result['OK']:
            resDigest = request.getDigest()
            digest = resDigest['Value']
            self.jobReport.setJobParameter('PendingRequest', digest)
            return S_OK(request)
          else:
            self.log.error('Failed to set failover request',
                           '%d: %s. Re-trying...' % (counter, result['Message']))
            del requestClient
            time.sleep(counter ** 3)

        if not result['OK']:
          return result

    return S_OK()

  #############################################################################
  def __getRequestFiles(self):
    """Simple wrapper to return the list of request files.
    """
    return glob.glob('*_request.json')

  #############################################################################
  def __cleanUp(self):
    """Cleans up after job processing. Can be switched off via environment
       variable DO_NOT_DO_JOB_CLEANUP or by JobWrapper configuration option.
    """
    # Environment variable is a feature for DIRAC (helps local debugging).
    if 'DO_NOT_DO_JOB_CLEANUP' in os.environ or not self.cleanUpFlag:
      cleanUp = False
    else:
      cleanUp = True

    os.chdir(self.root)
    if cleanUp:
      self.log.verbose('Cleaning up job working directory')
      if os.path.exists(str(self.jobID)):
        shutil.rmtree(str(self.jobID))

  #############################################################################
  def __report(self, status='', minorStatus='', sendFlag=False):
    """Wraps around setJobStatus of jobReport object
    """
    self.log.verbose('setJobStatus', '(%s,%s,%s,%s)' % (self.jobID, status, minorStatus, 'JobWrapper'))

    if status:
      self.wmsMajorStatus = status
    if minorStatus:
      self.wmsMinorStatus = minorStatus
    jobStatus = self.jobReport.setJobStatus(status=status,
                                            minorStatus=minorStatus,
                                            sendFlag=sendFlag)
    if not jobStatus['OK']:
      self.log.warn('Failed setting job status', jobStatus['Message'])

    return jobStatus

  #############################################################################
  def __setJobParam(self, name, value, sendFlag=False):
    """Wraps around setJobParameter of JobReport client
    """
    jobParam = self.jobReport.setJobParameter(
        str(name), str(value), sendFlag)
    if not jobParam['OK']:
      self.log.warn('Failed setting job parameter', jobParam['Message'])
    if self.jobID:
      self.log.verbose('setJobParameter', '(%s,%s,%s)' % (self.jobID, name, value))

    return jobParam

  #############################################################################
  def __setJobParamList(self, value, sendFlag=False):
    """Wraps around setJobParameters of JobReport client
    """
    jobParam = self.jobReport.setJobParameters(value, sendFlag)
    if not jobParam['OK']:
      self.log.warn(jobParam['Message'])
    if self.jobID:
      self.log.verbose('setJobParameters(%s,%s)' % (self.jobID, value))

    return jobParam

###############################################################################
###############################################################################


class ExecutionThread(threading.Thread):

  #############################################################################
  def __init__(self, spObject, cmd, maxPeekLines, stdoutFile, stderrFile, exeEnv):
    threading.Thread.__init__(self)
    self.cmd = cmd
    self.spObject = spObject
    self.outputLines = []
    self.maxPeekLines = maxPeekLines
    self.stdout = stdoutFile
    self.stderr = stderrFile
    self.exeEnv = exeEnv

  #############################################################################
  def run(self):
    """ Method representing the thread activity.
        This one overrides the ~threading.Thread `run` method
    """
    log = gLogger.getSubLogger("ExecutionThread")

    # FIXME: why local instances of object variables are created?
    cmd = self.cmd
    spObject = self.spObject
    start = time.time()
    initialStat = os.times()
    log.verbose("Cmd called", cmd)
    output = spObject.systemCall(cmd, env=self.exeEnv, callbackFunction=self.sendOutput, shell=True)
    log.verbose("Output of system call within execution thread: %s" % output)
    EXECUTION_RESULT['Thread'] = output
    timing = time.time() - start
    EXECUTION_RESULT['Timing'] = timing
    finalStat = os.times()
    EXECUTION_RESULT['CPU'] = []
    for i, _ in enumerate(finalStat):
      EXECUTION_RESULT['CPU'].append(finalStat[i] - initialStat[i])
    cpuString = ' '.join(['%.2f' % x for x in EXECUTION_RESULT['CPU']])
    log.info('EXECUTION_RESULT[CPU] after Execution of spObject.systemCall', cpuString)
    log.info('EXECUTION_RESULT[Thread] after Execution of spObject.systemCall', str(EXECUTION_RESULT['Thread']))

  #############################################################################
  def getCurrentPID(self):
    return self.spObject.getChildPID()

  #############################################################################
  def sendOutput(self, stdid, line):
    if stdid == 0 and self.stdout:
      with open(self.stdout, 'a+') as outputFile:
        print(line, file=outputFile)
    elif stdid == 1 and self.stderr:
      with open(self.stderr, 'a+') as errorFile:
        print(line, file=errorFile)
    self.outputLines.append(line)
    size = len(self.outputLines)
    if size > self.maxPeekLines:
      # reduce max size of output peeking
      self.outputLines.pop(0)

  #############################################################################
  def getOutput(self, lines=0):
    if self.outputLines:
      # restrict to smaller number of lines for regular
      # peeking by the watchdog
      # FIXME: this is multithread, thus single line would be better
      if lines:
        size = len(self.outputLines)
        cut = size - lines
        self.outputLines = self.outputLines[cut:]
      return S_OK(self.outputLines)
    return S_ERROR('No Job output found')


def rescheduleFailedJob(jobID, minorStatus, jobReport=None):
  """ Function for rescheduling a jobID, setting a minorStatus
  """

  rescheduleResult = JobStatus.RESCHEDULED

  try:

    gLogger.warn('Failure during %s' % (minorStatus))

    # Setting a job parameter does not help since the job will be rescheduled,
    # instead set the status with the cause and then another status showing the
    # reschedule operation.

    if not jobReport:
      gLogger.info('Creating a new JobReport Object')
      jobReport = JobReport(int(jobID), 'JobWrapper')

    jobReport.setApplicationStatus('Failed %s ' % minorStatus, sendFlag=False)
    jobReport.setJobStatus(status=JobStatus.RESCHEDULED, minorStatus=minorStatus, sendFlag=False)

    # We must send Job States and Parameters before it gets reschedule
    jobReport.sendStoredStatusInfo()
    jobReport.sendStoredJobParameters()

    gLogger.info('Job will be rescheduled after exception during execution of the JobWrapper')

    jobManager = JobManagerClient()
    result = jobManager.rescheduleJob(int(jobID))
    if not result['OK']:
      gLogger.warn(result['Message'])
      if 'Maximum number of reschedulings is reached' in result['Message']:
        rescheduleResult = JobStatus.FAILED

    return rescheduleResult
  except Exception:
    gLogger.exception('JobWrapperTemplate failed to reschedule Job')
    return JobStatus.FAILED
