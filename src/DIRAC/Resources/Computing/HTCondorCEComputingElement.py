########################################################################
# File :   HTCondorCEComputingElement.py
# Author : A.S.
########################################################################

"""HTCondorCE Computing Element

Allows direct submission to HTCondorCE Computing Elements with a SiteDirector Agent

**Configuration Parameters**

Configuration for the HTCondorCE submission can be done via the configuration system. See the page about
configuring :ref:`resourcesComputing` for where the options can be placed.

WorkingDirectory:
   Location to store the pilot and condor log files locally. It should exist on the server and be accessible (both
   readable and writeable).  Also temporary files like condor submit files are kept here. This option is only read
   from the global Resources/Computing/HTCondorCE location.

DaysToKeepRemoteLogs:
   How long to keep the log files on the remote schedd until they are removed

DaysToKeepLogs:
   How long to keep the log files locally until they are removed

ExtraSubmitString:
   Additional options for the condor submit file, separate options with '\\n', for example::

     request_cpus = 8 \\n periodic_remove = ...

   CERN proposes additional features to the standard HTCondor implementation. Among these features, one can find
   an option to limit the allocation runtime (`+MaxRuntime`), that does not exist in the standard HTCondor version:
   no explicit way to define a runtime limit (`maxCPUTime` would act as the limit). On CERN-HTCondor CEs, one can use
   CERN-specific features via the `ExtraSubmitString` configuration parameter.

UseLocalSchedd:
   If False, directly submit to a remote condor schedule daemon,
   then one does not need to run condor daemons on the submit machine.
   If True requires the condor grid middleware (condor_submit, condor_history, condor_q, condor_rm)

**Proxy renewal or lifetime**

When not using a local condor_schedd, add ``delegate_job_GSI_credentials_lifetime = 0`` to the ``ExtraSubmitString``.

When using a local condor_schedd look at the HTCondor documenation for enabling the proxy refresh.

**Code Documentation**
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
# Note: if you read this documentation in the source code and not via the sphinx
# created documentation, there should only be one slash when setting the option,
# but "\n" gets rendered as a linebreak in sphinx

import six
import os
import tempfile
# TODO: This should be modernised to use subprocess(32)
try:
  import commands
except ImportError:
  # Python 3's subprocess module contains a compatibility layer
  import subprocess as commands
import datetime
import errno
import threading

from DIRAC import S_OK, S_ERROR, gConfig
from DIRAC.Resources.Computing.ComputingElement import ComputingElement
from DIRAC.Core.Utilities.Grid import executeGridCommand
from DIRAC.Core.Utilities.File import mkDir
from DIRAC.Core.Utilities.List import breakListIntoChunks

# BEWARE: this import makes it impossible to instantiate this CE client side
from DIRAC.WorkloadManagementSystem.DB.PilotAgentsDB import PilotAgentsDB

from DIRAC.WorkloadManagementSystem.Agent.SiteDirector import WAITING_PILOT_STATUS
from DIRAC.Core.Utilities.File import makeGuid
from DIRAC.Core.Utilities.Subprocess import Subprocess

from DIRAC.Resources.Computing.BatchSystems.Condor import parseCondorStatus, treatCondorHistory

__RCSID__ = "$Id$"

CE_NAME = 'HTCondorCE'
MANDATORY_PARAMETERS = ['Queue']
DEFAULT_WORKINGDIRECTORY = '/opt/dirac/pro/runit/WorkloadManagement/SiteDirectorHT'
DEFAULT_DAYSTOKEEPREMOTELOGS = 1
DEFAULT_DAYSTOKEEPLOGS = 15


def logDir(ceName, stamp):
  """ Return path to log and output files for pilot.

  :param str ceName: Name of the CE
  :param str stamp: pilot stamp from/for jobRef
  """
  return os.path.join(ceName, stamp[0], stamp[1:3])


def condorIDAndPathToResultFromJobRef(jobRef):
  """ Extract tuple of jobURL and jobID from the jobRef string.
  The condorID as well as the path leading to the job results are also extracted from the jobID.

  :param str jobRef: PilotJobReference of the following form: ``htcondorce://<ceName>/<condorID>:::<pilotStamp>``

  :return: tuple composed of the jobURL, the path to the job results and the condorID of the given jobRef
  """
  splits = jobRef.split(":::")
  jobURL = splits[0]
  stamp = splits[1] if len(splits) > 1 else ''
  _, _, ceName, condorID = jobURL.split("/")

  # Reconstruct the path leading to the result (log, output)
  # Construction of the path can be found in submitJob()
  pathToResult = logDir(ceName, stamp) if len(stamp) >= 3 else ''

  return jobURL, pathToResult, condorID


def findFile(workingDir, fileName, pathToResult=None):
  """ Find a file in a file system.

  :param str workingDir: the name of the directory containing the given file to search for
  :param str fileName: the name of the file to find
  :param str pathToResult: the path to follow from workingDir to find the file

  :return: list of paths leading to the file
  """

  # In the case pathToResult is defined, we just have to check the path exists
  if pathToResult:
    path = os.path.join(workingDir, pathToResult, fileName)
    if os.path.exists(path):
      # We put the path in a list to be consistent
      return S_OK([path])

  # In the case pathToResult is not defined or not correct
  # We have to search for the file in workingDir and can get multiple results
  res = Subprocess().systemCall("find %s -name '%s'" % (workingDir, fileName), shell=True)
  if not res['OK']:
    return res
  paths = res['Value'][1].splitlines()
  if not paths:
    return S_ERROR(errno.ENOENT, "Could not find %s in directory %s" % (fileName, workingDir))
  return S_OK(paths)


def getCondorLogFile(pilotRef):
  """ Return the location of the logFile belonging to the pilot reference.
  """
  _jobUrl, pathToResult, condorID = condorIDAndPathToResultFromJobRef(pilotRef)
  # FIXME: This gets called from the WMSAdministrator, so we don't have the same
  # working directory as for the SiteDirector unless we force it, there is also
  # no CE instantiated when this function is called so we can only pick this option up from one place
  workingDirectory = gConfig.getValue("Resources/Computing/HTCondorCE/WorkingDirectory",
                                      DEFAULT_WORKINGDIRECTORY)

  resLog = findFile(workingDirectory, '%s.log' % condorID, pathToResult)
  return resLog


class HTCondorCEComputingElement(ComputingElement):
  """ HTCondorCE computing element class
      implementing the functions jobSubmit, getJobOutput
  """

  # static variables to ensure single cleanup every minute
  _lastCleanupTime = datetime.datetime.utcnow()
  _cleanupLock = threading.Lock()

  #############################################################################
  def __init__(self, ceUniqueID):
    """ Standard constructor.
    """
    super(HTCondorCEComputingElement, self).__init__(ceUniqueID)

    self.ceType = CE_NAME
    self.submittedJobs = 0
    self.mandatoryParameters = MANDATORY_PARAMETERS
    self.pilotProxy = ''
    self.queue = ''
    self.outputURL = 'gsiftp://localhost'
    self.gridEnv = ''
    self.proxyRenewal = 0
    self.daysToKeepLogs = DEFAULT_DAYSTOKEEPLOGS
    self.daysToKeepRemoteLogs = DEFAULT_DAYSTOKEEPREMOTELOGS
    self.extraSubmitString = ''
    # see note on getCondorLogFile, why we can only use the global setting
    self.workingDirectory = gConfig.getValue("Resources/Computing/HTCondorCE/WorkingDirectory",
                                             DEFAULT_WORKINGDIRECTORY)
    self.useLocalSchedd = True
    self.remoteScheddOptions = ""

  #############################################################################
  def __writeSub(self, executable, nJobs, location, processors):
    """ Create the Sub File for submission.

    :param str executable: name of the script to execute
    :param int nJobs: number of desired jobs
    :param str location: directory that should contain the result of the jobs
    :param int processors: number of CPU cores to allocate
    """

    self.log.debug("Working directory: %s " % self.workingDirectory)
    mkDir(os.path.join(self.workingDirectory, location))

    self.log.debug("InitialDir: %s" % os.path.join(self.workingDirectory, location))

    self.log.debug("ExtraSubmitString:\n### \n %s \n###" % self.extraSubmitString)

    fd, name = tempfile.mkstemp(suffix='.sub', prefix='HTCondorCE_', dir=self.workingDirectory)
    subFile = os.fdopen(fd, 'w')

    executable = os.path.join(self.workingDirectory, executable)

    # This is used to remove outputs from the remote schedd
    # Used in case a local schedd is not used
    periodicRemove = "periodic_remove = "
    periodicRemove += "(JobStatus == 4) && "
    periodicRemove += "(time() - EnteredCurrentStatus) > (%s * 24 * 3600)" % self.daysToKeepRemoteLogs

    localScheddOptions = """
ShouldTransferFiles = YES
WhenToTransferOutput = ON_EXIT_OR_EVICT
""" if self.useLocalSchedd else periodicRemove

    targetUniverse = "grid" if self.useLocalSchedd else "vanilla"

    sub = """
executable = %(executable)s
universe = %(targetUniverse)s
use_x509userproxy = true
output = $(Cluster).$(Process).out
error = $(Cluster).$(Process).err
log = $(Cluster).$(Process).log
environment = "HTCONDOR_JOBID=$(Cluster).$(Process)"
initialdir = %(initialDir)s
grid_resource = condor %(ceName)s %(ceName)s:9619
transfer_output_files = ""
request_cpus = %(processors)s
%(localScheddOptions)s

kill_sig=SIGTERM

%(extraString)s

Queue %(nJobs)s

""" % dict(executable=executable,
           nJobs=nJobs,
           processors=processors,
           ceName=self.ceName,
           extraString=self.extraSubmitString,
           initialDir=os.path.join(self.workingDirectory, location),
           localScheddOptions=localScheddOptions,
           targetUniverse=targetUniverse,
           )
    subFile.write(sub)
    subFile.close()
    return name

  def _reset(self):
    self.queue = self.ceParameters['Queue']
    self.outputURL = self.ceParameters.get('OutputURL', 'gsiftp://localhost')
    self.gridEnv = self.ceParameters.get('GridEnv')
    self.daysToKeepLogs = self.ceParameters.get('DaysToKeepLogs', DEFAULT_DAYSTOKEEPLOGS)
    self.extraSubmitString = str(self.ceParameters.get('ExtraSubmitString', '').encode().decode('unicode_escape'))
    self.daysToKeepRemoteLogs = self.ceParameters.get('DaysToKeepRemoteLogs', DEFAULT_DAYSTOKEEPREMOTELOGS)
    self.useLocalSchedd = self.ceParameters.get('UseLocalSchedd', self.useLocalSchedd)
    if isinstance(self.useLocalSchedd, six.string_types):
      if self.useLocalSchedd == "False":
        self.useLocalSchedd = False

    self.remoteScheddOptions = "" if self.useLocalSchedd else "-pool %s:9619 -name %s " % (self.ceName, self.ceName)

    self.log.debug("Using local schedd: %r " % self.useLocalSchedd)
    self.log.debug("Remote scheduler option: '%s' " % self.remoteScheddOptions)
    return S_OK()

  #############################################################################
  def submitJob(self, executableFile, proxy, numberOfJobs=1):
    """ Method to submit job
    """

    self.log.verbose("Executable file path: %s" % executableFile)
    if not os.access(executableFile, 5):
      os.chmod(executableFile, 0o755)

    # The submitted pilots are going to have a common part of the stamp to construct a path to retrieve results
    # Then they also have an individual part to make them unique
    jobStamps = []
    commonJobStampPart = makeGuid()[:3]
    for _i in range(numberOfJobs):
      jobStamp = commonJobStampPart + makeGuid()[:5]
      jobStamps.append(jobStamp)

    # We randomize the location of the pilot output and log, because there are just too many of them
    location = logDir(self.ceName, commonJobStampPart)
    nProcessors = self.ceParameters.get('NumberOfProcessors', 1)
    subName = self.__writeSub(executableFile, numberOfJobs, location, nProcessors)

    cmd = ['condor_submit', '-terse', subName]
    # the options for submit to remote are different than the other remoteScheddOptions
    scheddOptions = [] if self.useLocalSchedd else ['-pool', '%s:9619' % self.ceName, '-remote', self.ceName]
    for op in scheddOptions:
      cmd.insert(-1, op)

    result = executeGridCommand(self.proxy, cmd, self.gridEnv)
    self.log.verbose(result)
    os.unlink(subName)
    if not result['OK']:
      self.log.error("Failed to submit jobs to htcondor", result['Message'])
      return result

    if result['Value'][0]:
      # We have got a non-zero status code
      errorString = result['Value'][2] if result['Value'][2] else result['Value'][1]
      return S_ERROR('Pilot submission failed with error: %s ' % errorString.strip())

    pilotJobReferences = self.__getPilotReferences(result['Value'][1].strip())
    if not pilotJobReferences['OK']:
      return pilotJobReferences
    pilotJobReferences = pilotJobReferences['Value']

    self.log.verbose("JobStamps: %s " % jobStamps)
    self.log.verbose("pilotRefs: %s " % pilotJobReferences)

    result = S_OK(pilotJobReferences)
    result['PilotStampDict'] = dict(zip(pilotJobReferences, jobStamps))
    self.log.verbose("Result for submission: %s " % result)
    return result

  def killJob(self, jobIDList):
    """ Kill the specified jobs
    """
    if not jobIDList:
      return S_OK()
    if isinstance(jobIDList, six.string_types):
      jobIDList = [jobIDList]

    self.log.verbose("KillJob jobIDList: %s" % jobIDList)

    for jobRef in jobIDList:
      job, _, jobID = condorIDAndPathToResultFromJobRef(jobRef)
      self.log.verbose("Killing pilot %s " % job)
      status, stdout = commands.getstatusoutput('condor_rm %s %s' % (self.remoteScheddOptions, jobID))
      if status != 0:
        return S_ERROR("Failed to kill pilot %s: %s" % (job, stdout))

    return S_OK()

#############################################################################
  def getCEStatus(self):
    """ Method to return information on running and pending jobs.
    """
    result = S_OK()
    result['SubmittedJobs'] = 0
    result['RunningJobs'] = 0
    result['WaitingJobs'] = 0

    # getWaitingPilots
    condDict = {'DestinationSite': self.ceName,
                'Status': WAITING_PILOT_STATUS}
    res = PilotAgentsDB().countPilots(condDict)
    if res['OK']:
      result['WaitingJobs'] = int(res['Value'])
    else:
      self.log.warn("Failure getting pilot count for %s: %s " % (self.ceName, res['Message']))

    # getRunningPilots
    condDict = {'DestinationSite': self.ceName,
                'Status': 'Running'}
    res = PilotAgentsDB().countPilots(condDict)
    if res['OK']:
      result['RunningJobs'] = int(res['Value'])
    else:
      self.log.warn("Failure getting pilot count for %s: %s " % (self.ceName, res['Message']))

    return result

  def getJobStatus(self, jobIDList):
    """ Get the status information for the given list of jobs
    """
    self.__cleanup()

    self.log.verbose("Job ID List for status: %s " % jobIDList)
    if isinstance(jobIDList, six.string_types):
      jobIDList = [jobIDList]

    resultDict = {}
    condorIDs = {}
    # Get all condorIDs so we can just call condor_q and condor_history once
    for jobRef in jobIDList:
      job, _, jobID = condorIDAndPathToResultFromJobRef(jobRef)
      condorIDs[job] = jobID

    qList = []
    for _condorIDs in breakListIntoChunks(condorIDs.values(), 100):

      # This will return a list of 1245.75 3
      status, stdout_q = commands.getstatusoutput('condor_q %s %s -af:j JobStatus ' % (self.remoteScheddOptions,
                                                                                       ' '.join(_condorIDs)))
      if status != 0:
        return S_ERROR(stdout_q)
      _qList = stdout_q.strip().split('\n')
      qList.extend(_qList)

      # FIXME: condor_history does only support j for autoformat from 8.5.3,
      # format adds whitespace for each field This will return a list of 1245 75 3
      # needs to cocatenate the first two with a dot
      condorHistCall = 'condor_history %s %s -af ClusterId ProcId JobStatus' % (self.remoteScheddOptions,
                                                                                ' '.join(_condorIDs))

      treatCondorHistory(condorHistCall, qList)

    for job, jobID in condorIDs.items():

      pilotStatus = parseCondorStatus(qList, jobID)
      if pilotStatus == 'HELD':
        # make sure the pilot stays dead and gets taken out of the condor_q
        _rmStat, _rmOut = commands.getstatusoutput('condor_rm %s %s ' % (self.remoteScheddOptions, jobID))
        # self.log.debug( "condor job killed: job %s, stat %s, message %s " % ( jobID, rmStat, rmOut ) )
        pilotStatus = 'Aborted'

      resultDict[job] = pilotStatus

    self.log.verbose("Pilot Statuses: %s " % resultDict)
    return S_OK(resultDict)

  def getJobOutput(self, jobID, _localDir=None):
    """ TODO: condor can copy the output automatically back to the
    submission, so we just need to pick it up from the proper folder
    """
    self.log.verbose("Getting job output for jobID: %s " % jobID)
    _job, pathToResult, condorID = condorIDAndPathToResultFromJobRef(jobID)
    # FIXME: the WMSAdministrator does not know about the
    # SiteDirector WorkingDirectory, it might not even run on the
    # same machine
    # workingDirectory = self.ceParameters.get( 'WorkingDirectory', DEFAULT_WORKINGDIRECTORY )

    if not self.useLocalSchedd:
      iwd = None

      # TOREMOVE: once v7r0 will mainly be used, remove the following block that was only useful
      # when path to output was not deterministic
      status, stdout_q = commands.getstatusoutput('condor_q %s %s -af SUBMIT_Iwd' % (self.remoteScheddOptions,
                                                                                     condorID))
      self.log.verbose('condor_q:', stdout_q)
      if status == 0 and self.workingDirectory in stdout_q:
        iwd = stdout_q
        pathToResult = iwd

      # Use the path extracted from the pilotID
      if iwd is None:
        iwd = os.path.join(self.workingDirectory, pathToResult)

      try:
        mkDir(iwd)
      except OSError as e:
        errorMessage = "Failed to create the pilot output directory"
        self.log.exception(errorMessage, iwd)
        return S_ERROR(e.errno, '%s (%s)' % (errorMessage, iwd))

      cmd = ['condor_transfer_data', '-pool', '%s:9619' % self.ceName, '-name', self.ceName, condorID]
      result = executeGridCommand(self.proxy, cmd, self.gridEnv)
      self.log.verbose(result)

      errorMessage = "Failed to get job output from htcondor"
      if not result['OK']:
        self.log.error(errorMessage, result['Message'])
        return result
      # Even if result is OK, the actual exit code of cmd can still be an error
      if result['OK'] and result['Value'][0] != 0:
        varMessage = result['Value'][1].strip()
        self.log.error(errorMessage, varMessage)
        return S_ERROR('%s: %s' % (errorMessage, varMessage))

    output = ''
    error = ''

    resOut = findFile(self.workingDirectory, '%s.out' % condorID, pathToResult)
    if not resOut['OK']:
      self.log.error("Failed to find output file for condor job", jobID)
      return resOut
    outputfilename = resOut['Value'][0]

    resErr = findFile(self.workingDirectory, '%s.err' % condorID, pathToResult)
    if not resErr['OK']:
      self.log.error("Failed to find error file for condor job", jobID)
      return resErr
    errorfilename = resErr['Value'][0]

    try:
      with open(outputfilename) as outputfile:
        output = outputfile.read()
    except IOError as e:
      self.log.error("Failed to open outputfile", str(e))
      return S_ERROR("Failed to get pilot output")
    try:
      with open(errorfilename) as errorfile:
        error = errorfile.read()
    except IOError as e:
      self.log.error("Failed to open errorfile", str(e))
      return S_ERROR("Failed to get pilot error")

    return S_OK((output, error))

  def __getPilotReferences(self, jobString):
    """ Get the references from the condor_submit output.
    Cluster ids look like " 107.0 - 107.0 " or " 107.0 - 107.4 "

    :param str jobString: the output of condor_submit

    :return: job references such as htcondorce://<CE name>/<path to result>-<clusterID>.<i>
    """
    self.log.verbose("getPilotReferences: %s" % jobString)
    clusterIDs = jobString.split('-')
    if len(clusterIDs) != 2:
      return S_ERROR("Something wrong with the condor_submit output: %s" % jobString)
    clusterIDs = [clu.strip() for clu in clusterIDs]
    self.log.verbose("Cluster IDs parsed: %s " % clusterIDs)
    try:
      clusterID = clusterIDs[0].split('.')[0]
      numJobs = clusterIDs[1].split('.')[1]
    except IndexError:
      return S_ERROR("Something wrong with the condor_submit output: %s" % jobString)
    cePrefix = "htcondorce://%s/" % self.ceName
    jobReferences = ["%s%s.%s" % (cePrefix, clusterID, i) for i in range(int(numJobs) + 1)]
    return S_OK(jobReferences)

  def __cleanup(self):
    """ Clean the working directory of old jobs"""

    # FIXME: again some issue with the working directory...
    # workingDirectory = self.ceParameters.get( 'WorkingDirectory', DEFAULT_WORKINGDIRECTORY )

    if not HTCondorCEComputingElement._cleanupLock.acquire(False):
      return

    now = datetime.datetime.utcnow()
    if (now - HTCondorCEComputingElement._lastCleanupTime).total_seconds() < 60:
      HTCondorCEComputingElement._cleanupLock.release()
      return

    HTCondorCEComputingElement._lastCleanupTime = now

    self.log.debug("Cleaning working directory: %s" % self.workingDirectory)

    # remove all files older than 120 minutes starting with DIRAC_ Condor will
    # push files on submission, but it takes at least a few seconds until this
    # happens so we can't directly unlink after condor_submit
    status, stdout = commands.getstatusoutput('find -O3 %s -maxdepth 1 -mmin +120 -name "DIRAC_*" -delete ' %
                                              self.workingDirectory)
    if status:
      self.log.error("Failure during HTCondorCE __cleanup", stdout)

    # remove all out/err/log files older than "DaysToKeepLogs" days in the working directory
    # not running this for each CE so we do global cleanup
    findPars = dict(workDir=self.workingDirectory, days=self.daysToKeepLogs)
    # remove all out/err/log files older than "DaysToKeepLogs" days
    status, stdout = commands.getstatusoutput(
        r'find %(workDir)s -mtime +%(days)s -type f \( -name "*.out" -o -name "*.err" -o -name "*.log" \) -delete ' %
        findPars)
    if status:
      self.log.error("Failure during HTCondorCE __cleanup", stdout)
    self._cleanupLock.release()
