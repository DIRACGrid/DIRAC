########################################################################
# File :   LocalComputingElement.py
# Author : Ricardo Graciani, A.T.
########################################################################

""" LocalComputingElement is a class to handle non-grid computing clusters

Allows direct submission to underlying Batch Systems.

**Configuration Parameters**

Configuration for the LocalComputingElement submission can be done via the configuration system.

BatchSystem:
   Underlying batch system that is going to be used to orchestrate executable files. The Batch System has to be
   accessible from the LocalCE. By default, the LocalComputingElement submits directly on the host via the Host class.

ParallelLibrary:
   Underlying parallel library used to generate a wrapper around the executable files to run them in parallel on
   multiple nodes.

SharedArea:
   Area used to store executable/output/error files if they are not aready defined via BatchOutput, BatchError,
   InfoArea, ExecutableArea and/or WorkArea. The path should be absolute.

BatchOutput:
   Area where the job outputs are stored.
   If not defined: SharedArea + '/data' is used.
   If not absolute: SharedArea + path is used.

BatchError:
   Area where the job errors are stored.
   If not defined: SharedArea + '/data' is used.
   If not absolute: SharedArea + path is used.

ExecutableArea:
   Area where the executable files are stored if necessary: this is the case when a parallel library is used.
   Indeed, the executable has to be accessible to the batch system. This might not be the case
   if multiple file systems are present on the host.
   If not defined: SharedArea + '/data' is used.
   If not absolute: SharedArea + path is used.

**Code Documentation**

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import stat
import shutil
import tempfile
import getpass
import errno
from six.moves.urllib.parse import urlparse

from DIRAC import S_OK, S_ERROR
from DIRAC import gConfig

from DIRAC.Resources.Computing.ComputingElement import ComputingElement
from DIRAC.Resources.Computing.PilotBundle import bundleProxy, writeScript
from DIRAC.Core.Utilities.List import uniqueElements
from DIRAC.Core.Utilities.File import makeGuid
from DIRAC.Core.Utilities.Subprocess import systemCall


class LocalComputingElement(ComputingElement):

  def __init__(self, ceUniqueID):
    """ Standard constructor.
    """
    super(LocalComputingElement, self).__init__(ceUniqueID)

    self.ceType = ''
    self.execution = "Local"
    self.submittedJobs = 0
    self.userName = getpass.getuser()

  def _reset(self):
    """ Process CE parameters and make necessary adjustments
    """
    batchSystemName = self.ceParameters.get('BatchSystem', 'Host')
    result = self.loadBatchSystem(batchSystemName)
    if not result['OK']:
      self.log.error('Failed to load the batch system plugin %s', self.batchSystem)
      return result

    self.queue = self.ceParameters['Queue']
    if 'ExecQueue' not in self.ceParameters or not self.ceParameters['ExecQueue']:
      self.ceParameters['ExecQueue'] = self.ceParameters.get('Queue', '')
    self.execQueue = self.ceParameters['ExecQueue']
    self.log.info("Using queue: ", self.queue)

    self.sharedArea = self.ceParameters['SharedArea']
    self.batchOutput = self.ceParameters['BatchOutput']
    if not self.batchOutput.startswith('/'):
      self.batchOutput = os.path.join(self.sharedArea, self.batchOutput)
    self.batchError = self.ceParameters['BatchError']
    if not self.batchError.startswith('/'):
      self.batchError = os.path.join(self.sharedArea, self.batchError)
    self.infoArea = self.ceParameters['InfoArea']
    if not self.infoArea.startswith('/'):
      self.infoArea = os.path.join(self.sharedArea, self.infoArea)
    self.executableArea = self.ceParameters['ExecutableArea']
    if not self.executableArea.startswith('/'):
      self.executableArea = os.path.join(self.sharedArea, self.executableArea)
    self.workArea = self.ceParameters['WorkArea']
    if not self.workArea.startswith('/'):
      self.workArea = os.path.join(self.sharedArea, self.workArea)

    parallelLibraryName = self.ceParameters.get('ParallelLibrary')
    if parallelLibraryName:
      result = self.loadParallelLibrary(parallelLibraryName, self.executableArea)
      if not result['OK']:
        self.log.error('Failed to load the parallel library plugin %s', parallelLibraryName)
        return result

    result = self._prepareHost()
    if not result['OK']:
      self.log.error('Failed to initialize CE', self.ceName)
      return result

    self.removeOutput = True
    if 'RemoveOutput' in self.ceParameters:
      if self.ceParameters['RemoveOutput'].lower() in ['no', 'false', '0']:
        self.removeOutput = False

    self.submitOptions = self.ceParameters.get('SubmitOptions', '')
    self.numberOfProcessors = self.ceParameters.get('NumberOfProcessors', 1)
    self.wholeNode = self.ceParameters.get('WholeNode', False)
    # numberOfNodes is treated as a string as it can contain values such as "2-4"
    # where 2 would represent the minimum number of nodes to allocate, and 4 the maximum
    self.numberOfNodes = self.ceParameters.get('NumberOfNodes', '1')
    self.numberOfGPUs = self.ceParameters.get("NumberOfGPUs")

    return S_OK()

  def _addCEConfigDefaults(self):
    """Method to make sure all necessary Configuration Parameters are defined
    """
    # First assure that any global parameters are loaded
    ComputingElement._addCEConfigDefaults(self)
    # Now batch system specific ones
    if 'ExecQueue' not in self.ceParameters:
      self.ceParameters['ExecQueue'] = self.ceParameters.get('Queue', '')

    if 'SharedArea' not in self.ceParameters:
      defaultPath = os.environ.get('HOME', '.')
      self.ceParameters['SharedArea'] = gConfig.getValue('/LocalSite/InstancePath', defaultPath)

    if 'BatchOutput' not in self.ceParameters:
      self.ceParameters['BatchOutput'] = 'data'

    if 'BatchError' not in self.ceParameters:
      self.ceParameters['BatchError'] = 'data'

    if 'ExecutableArea' not in self.ceParameters:
      self.ceParameters['ExecutableArea'] = 'data'

    if 'InfoArea' not in self.ceParameters:
      self.ceParameters['InfoArea'] = 'info'

    if 'WorkArea' not in self.ceParameters:
      self.ceParameters['WorkArea'] = 'work'

  def _prepareHost(self):
    """ Prepare directories and copy control script
    """

    # Make remote directories
    dirTuple = uniqueElements([self.sharedArea,
                               self.executableArea,
                               self.infoArea,
                               self.batchOutput,
                               self.batchError,
                               self.workArea])
    cmdTuple = ['mkdir', '-p'] + dirTuple
    self.log.verbose('Creating working directories')
    result = systemCall(30, cmdTuple)
    if not result['OK']:
      self.log.error('Failed creating working directories', '(%s)' % result['Message'][1])
      return result
    status, output, error = result['Value']
    if status != 0:
      self.log.error('Failed to create directories', '(%s)' % error)
      return S_ERROR(errno.EACCES, 'Failed to create directories')

    return S_OK()

  def submitJob(self, executableFile, proxy=None, numberOfJobs=1):
    copyExecutable = os.path.join(self.executableArea, os.path.basename(executableFile))
    if self.parallelLibrary and executableFile != copyExecutable:
      # Because we use a parallel library, the executable will become a dependency of the parallel library script
      # Thus, it has to be defined in a specific area (executableArea) to be found and executed properly
      # For this reason, we copy the executable from its location to executableArea
      shutil.copy(executableFile, copyExecutable)
      executableFile = copyExecutable

    if not os.access(executableFile, 5):
      os.chmod(executableFile, stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)

    # if no proxy is supplied, the executable can be submitted directly
    # otherwise a wrapper script is needed to get the proxy to the execution node
    # The wrapper script makes debugging more complicated and thus it is
    # recommended to transfer a proxy inside the executable if possible.
    if self.proxy and not proxy:
      proxy = self.proxy
    if proxy:
      self.log.verbose('Setting up proxy for payload')
      wrapperContent = bundleProxy(executableFile, proxy)
      name = writeScript(wrapperContent, os.getcwd())
      submitFile = name
    else:  # no proxy
      submitFile = executableFile

    if self.parallelLibrary:
      # Wrap the executable to be executed multiple times in parallel via a parallel library
      submitFile = self.parallelLibrary.generateWrapper(submitFile)

    jobStamps = []
    for _i in range(numberOfJobs):
      jobStamps.append(makeGuid()[:8])

    batchDict = {
        "Executable": submitFile,
        "NJobs": numberOfJobs,
        "OutputDir": self.batchOutput,
        "ErrorDir": self.batchError,
        "SubmitOptions": self.submitOptions,
        "ExecutionContext": self.execution,
        "JobStamps": jobStamps,
        "Queue": self.queue,
        "WholeNode": self.wholeNode,
        "NumberOfProcessors": self.numberOfProcessors,
        "NumberOfNodes": self.numberOfNodes,
        "NumberOfGPUs": self.numberOfGPUs,
    }
    resultSubmit = self.batchSystem.submitJob(**batchDict)
    if proxy or self.parallelLibrary:
      os.remove(submitFile)

    if resultSubmit['Status'] == 0:
      self.submittedJobs += len(resultSubmit['Jobs'])
      # jobIDs = [ self.ceType.lower()+'://'+self.ceName+'/'+_id for _id in resultSubmit['Jobs'] ]
      # FIXME: It would be more proper to fix pilotCommands.__setFlavour where 'ssh' is hardcoded than
      # making this illogical fix, but there is no good way for pilotCommands to know its origin ceType.
      # So, the jobIDs here need to start with 'ssh', not ceType, to accomodate
      # them to those hardcoded in pilotCommands.__setFlavour
      batchSystemName = self.batchSystem.__class__.__name__.lower()
      jobIDs = ['ssh' + batchSystemName + '://' + self.ceName + '/' + _id for _id in resultSubmit['Jobs']]
      result = S_OK(jobIDs)
    else:
      result = S_ERROR(resultSubmit['Message'])

    return result

  def killJob(self, jobIDList):
    """ Kill a bunch of jobs
    """

    batchDict = {'JobIDList': jobIDList,
                 'Queue': self.queue}
    resultKill = self.batchSystem.killJob(**batchDict)
    if resultKill['Status'] == 0:
      return S_OK()
    return S_ERROR(resultKill['Message'])

  def getCEStatus(self):
    """ Method to return information on running and pending jobs.
    """
    result = S_OK()
    result['SubmittedJobs'] = self.submittedJobs
    result['RunningJobs'] = 0
    result['WaitingJobs'] = 0

    batchDict = {'User': self.userName,
                 'Queue': self.queue}
    resultGet = self.batchSystem.getCEStatus(**batchDict)
    if resultGet['Status'] == 0:
      result['RunningJobs'] = resultGet.get('Running', 0)
      result['WaitingJobs'] = resultGet.get('Waiting', 0)
    else:
      result = S_ERROR(resultGet['Message'])

    self.log.verbose('Waiting Jobs: ', result['WaitingJobs'])
    self.log.verbose('Running Jobs: ', result['RunningJobs'])

    return result

  def getJobStatus(self, jobIDList):
    """ Get the status information for the given list of jobs
    """
    resultDict = {}
    jobDict = {}

    # Extract the batch job ID from the full DIRAC job ID
    for job in jobIDList:
      stamp = os.path.basename(urlparse(job).path)
      jobDict[stamp] = job
    stampList = list(jobDict)

    # Get the status for a given batch job ID
    batchDict = {'JobIDList': stampList,
                 'User': self.userName,
                 'Queue': self.queue}
    resultGet = self.batchSystem.getJobStatus(**batchDict)

    if resultGet['Status'] != 0:
      return S_ERROR(resultGet['Message'])

    # Construct the dictionary to return: resultDict[dirac job ID] = status
    for stamp, status in resultGet['Jobs'].items():
      resultDict[jobDict[stamp]] = status

    return S_OK(resultDict)

  def getJobOutput(self, jobID, localDir=None):
    """ Get the specified job standard output and error files. If the localDir is provided,
        the output is returned as file in this directory. Otherwise, the output is returned
        as strings.
    """
    self.log.verbose('Getting output for jobID', jobID)
    result = self._getJobOutputFiles(jobID)
    if not result['OK']:
      return result

    jobStamp, _host, outputFile, errorFile = result['Value']
    if self.parallelLibrary:
      # outputFile and errorFile are directly modified by parallelLib
      self.parallelLibrary.processOutput(outputFile, errorFile)

    if not localDir:
      tempDir = tempfile.mkdtemp()
    else:
      tempDir = localDir

    try:
      localOut = os.path.join(tempDir, '%s.out' % jobStamp)
      localErr = os.path.join(tempDir, '%s.err' % jobStamp)
      if os.path.exists(outputFile):
        shutil.copy(outputFile, localOut)
      if os.path.exists(errorFile):
        shutil.copy(errorFile, localErr)
    except Exception as x:
      return S_ERROR('Failed to get output files: %s' % str(x))

    open(localOut, 'a').close()
    open(localErr, 'a').close()

    # The result is OK, we can remove the output
    if self.removeOutput and os.path.exists(outputFile):
      os.remove(outputFile)
    if self.removeOutput and os.path.exists(errorFile):
      os.remove(errorFile)

    if localDir:
      return S_OK((localOut, localErr))

    # Return the output as a string
    with open(localOut, 'r') as outputFile:
      output = outputFile.read()
    with open(localErr, 'r') as errorFile:
      error = errorFile.read()
    shutil.rmtree(tempDir)
    return S_OK((output, error))

  def _getJobOutputFiles(self, jobID):
    """ Get output file names for the specific CE
    """
    jobStamp = os.path.basename(urlparse(jobID).path)
    host = urlparse(jobID).hostname

    if hasattr(self.batchSystem, 'getJobOutputFiles'):
      batchDict = {'JobIDList': [jobStamp],
                   'OutputDir': self.batchOutput,
                   'ErrorDir': self.batchError}
      result = self.batchSystem.getJobOutputFiles(**batchDict)
      if result['Status'] != 0:
        return S_ERROR('Failed to get job output files: %s' % result['Message'])

      output = result['Jobs'][jobStamp]['Output']
      error = result['Jobs'][jobStamp]['Error']
    else:
      output = '%s/%s.out' % (self.batchOutput, jobStamp)
      error = '%s/%s.out' % (self.batchError, jobStamp)

    return S_OK((jobStamp, host, output, error))
