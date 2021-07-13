########################################################################
# File :   SSHComputingElement.py
# Author : Dumitru Laurentiu, A.T.
########################################################################

""" SSH (Virtual) Computing Element

For a given IP/host it will send jobs directly through ssh

**Configuration Parameters**

Configuration for the SSHComputingElement submission can be done via the configuration system.

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

SSHHost:
   SSH host name

SSHUser:
   SSH user login

SSHPassword:
   SSH password

SSHPort:
   Port number if not standard, e.g. for the gsissh access

SSHKey:
   Location of the ssh private key for no-password connection

SSHOptions:
   Any other SSH options to be used

SSHTunnel:
   String defining the use of intermediate SSH host. Example::

     ssh -i /private/key/location -l final_user final_host

SSHType:
   SSH (default) or gsissh

**Code Documentation**

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import six
import os
import json
import stat
import shutil
import errno
from six.moves.urllib.parse import urlparse
from six.moves.urllib.parse import quote as urlquote
from six.moves.urllib.parse import unquote as urlunquote
from six.moves import shlex_quote

import DIRAC
from DIRAC import S_OK, S_ERROR
from DIRAC import rootPath
from DIRAC import gLogger

from DIRAC.Resources.Computing.ComputingElement import ComputingElement
from DIRAC.Resources.Computing.PilotBundle import bundleProxy, writeScript
from DIRAC.Resources.Computing.BatchSystems.executeBatch import executeBatchContent
from DIRAC.Core.Utilities.List import uniqueElements
from DIRAC.Core.Utilities.File import makeGuid
from DIRAC.Core.Utilities.List import breakListIntoChunks


class SSH(object):
  """ SSH class encapsulates passing commands and files through an SSH tunnel
      to a remote host. It can use either ssh or gsissh access. The final host
      where the commands will be executed and where the files will copied/retrieved
      can be reached through an intermediate host if SSHTunnel parameters is defined.

      SSH constructor parameters are defined in a SSH accessible Computing Element
      in the Configuration System:

      - SSHHost: SSH host name
      - SSHUser: SSH user login
      - SSHPassword: SSH password
      - SSHPort: port number if not standard, e.g. for the gsissh access
      - SSHKey: location of the ssh private key for no-password connection
      - SSHOptions: any other SSH options to be used
      - SSHTunnel: string defining the use of intermediate SSH host. Example:
                   'ssh -i /private/key/location -l final_user final_host'
      - SSHType: ssh ( default ) or gsissh

      The class public interface includes two methods:

      sshCall( timeout, command_sequence )
      scpCall( timeout, local_file, remote_file, upload = False/True )
  """

  def __init__(self, host=None, parameters=None):

    self.host = host
    if parameters is None:
      parameters = {}
    if not host:
      self.host = parameters.get('SSHHost', '')

    self.user = parameters.get('SSHUser', '')
    self.password = parameters.get('SSHPassword', '')
    self.port = parameters.get('SSHPort', '')
    self.key = parameters.get('SSHKey', '')
    self.options = parameters.get('SSHOptions', '')
    self.sshTunnel = parameters.get('SSHTunnel', '')
    self.sshType = parameters.get('SSHType', 'ssh')

    if self.port:
      self.options += ' -p %s' % self.port
    if self.key:
      self.options += ' -i %s' % self.key
    self.options = self.options.strip()

    self.log = gLogger.getSubLogger('SSH')

  def __ssh_call(self, command, timeout):

    try:
      import pexpect
      expectFlag = True
    except BaseException:
      from DIRAC.Core.Utilities.Subprocess import shellCall
      expectFlag = False

    if not timeout:
      timeout = 999

    if expectFlag:
      ssh_newkey = 'Are you sure you want to continue connecting'
      try:
        child = pexpect.spawn(command, timeout=timeout)
        i = child.expect([pexpect.TIMEOUT, ssh_newkey, pexpect.EOF, 'assword: '])
        if i == 0:  # Timeout
          return S_OK((-1, child.before, 'SSH login failed'))
        elif i == 1:  # SSH does not have the public key. Just accept it.
          child.sendline('yes')
          child.expect('assword: ')
          i = child.expect([pexpect.TIMEOUT, 'assword: '])
          if i == 0:  # Timeout
            return S_OK((-1, str(child.before) + str(child.after), 'SSH login failed'))
          elif i == 1:
            child.sendline(self.password)
            child.expect(pexpect.EOF)
            return S_OK((0, child.before, ''))
        elif i == 2:
          # Passwordless login, get the output
          return S_OK((0, child.before, ''))

        if self.password:
          child.sendline(self.password)
          child.expect(pexpect.EOF)
          return S_OK((0, child.before, ''))
        return S_ERROR((-2, child.before, ''))
      except Exception as x:
        res = (-1, 'Encountered exception %s: %s' % (Exception, str(x)))
        return S_ERROR(res)
    else:
      # Try passwordless login
      result = shellCall(timeout, command)
#      print ( "!!! SSH command: %s returned %s\n" % (command, result) )
      if result['Value'][0] == 255:
        return S_ERROR((-1, 'Cannot connect to host %s' % self.host, ''))
      return result

  def sshCall(self, timeout, cmdSeq):
    """ Execute remote command via a ssh remote call

    :param int timeout: timeout of the command
    :param cmdSeq: list of command components
    :type cmdSeq: python:list
    """

    command = cmdSeq
    if isinstance(cmdSeq, list):
      command = ' '.join(cmdSeq)

    pattern = "__DIRAC__"

    if self.sshTunnel:
      command = command.replace("'", '\\\\\\\"')
      command = command.replace('$', '\\\\\\$')
      command = '/bin/sh -c \' %s -q %s -l %s %s "%s \\\"echo %s; %s\\\" " \' ' % (self.sshType, self.options,
                                                                                   self.user, self.host,
                                                                                   self.sshTunnel, pattern, command)
    else:
      # command = command.replace( '$', '\$' )
      command = '%s -q %s -l %s %s "echo %s; %s"' % (self.sshType, self.options, self.user, self.host,
                                                     pattern, command)
    self.log.debug("SSH command: %s" % command)
    result = self.__ssh_call(command, timeout)
    self.log.debug("SSH command result %s" % str(result))
    if not result['OK']:
      return result

    # Take the output only after the predefined pattern
    ind = result['Value'][1].find('__DIRAC__')
    if ind == -1:
      return result

    status, output, error = result['Value']
    output = output[ind + 9:]
    if output.startswith('\r'):
      output = output[1:]
    if output.startswith('\n'):
      output = output[1:]

    result['Value'] = (status, output, error)
    return result

  def scpCall(self, timeout, localFile, remoteFile, postUploadCommand='', upload=True):
    """ Perform file copy through an SSH magic.

    :param int timeout: timeout of the command
    :param str localFile: local file path, serves as source for uploading and destination for downloading.
                          Can take 'Memory' as value, in this case the downloaded contents is returned
                          as result['Value']
    :param str remoteFile: remote file full path
    :param str postUploadCommand: command executed on the remote side after file upload
    :param bool upload: upload if True, download otherwise
    """
    # shlex_quote aims to prevent any security issue or problems with filepath containing spaces
    # it returns a shell-escaped version of the filename
    localFile = shlex_quote(localFile)
    remoteFile = shlex_quote(remoteFile)
    if upload:
      if self.sshTunnel:
        remoteFile = remoteFile.replace('$', r'\\\\\$')
        postUploadCommand = postUploadCommand.replace('$', r'\\\\\$')
        command = "/bin/sh -c 'cat %s | %s -q %s %s@%s \"%s \\\"cat > %s; %s\\\"\"' " % (localFile,
                                                                                         self.sshType,
                                                                                         self.options,
                                                                                         self.user,
                                                                                         self.host,
                                                                                         self.sshTunnel,
                                                                                         remoteFile,
                                                                                         postUploadCommand)
      else:
        command = "/bin/sh -c \"cat %s | %s -q %s %s@%s 'cat > %s; %s'\" " % (localFile,
                                                                              self.sshType,
                                                                              self.options,
                                                                              self.user,
                                                                              self.host,
                                                                              remoteFile,
                                                                              postUploadCommand)
    else:
      finalCat = '| cat > %s' % localFile
      if localFile.lower() == 'memory':
        finalCat = ''
      if self.sshTunnel:
        remoteFile = remoteFile.replace('$', '\\\\\\$')
        command = "/bin/sh -c '%s -q %s -l %s %s \"%s \\\"cat %s\\\"\" %s'" % (self.sshType,
                                                                               self.options,
                                                                               self.user,
                                                                               self.host,
                                                                               self.sshTunnel,
                                                                               remoteFile,
                                                                               finalCat)
      else:
        remoteFile = remoteFile.replace('$', r'\$')
        command = "/bin/sh -c '%s -q %s -l %s %s \"cat %s\" %s'" % (self.sshType,
                                                                    self.options,
                                                                    self.user,
                                                                    self.host,
                                                                    remoteFile,
                                                                    finalCat)

    self.log.debug("SSH copy command: %s" % command)
    return self.__ssh_call(command, timeout)


class SSHComputingElement(ComputingElement):

  #############################################################################
  def __init__(self, ceUniqueID):
    """ Standard constructor.
    """
    super(SSHComputingElement, self).__init__(ceUniqueID)

    self.ceType = 'SSH'
    self.execution = "SSH"
    self.submittedJobs = 0
    self.outputTemplate = ''
    self.errorTemplate = ''

  ############################################################################
  def setProxy(self, proxy, valid=0):
    """
    Set and prepare proxy to use

    :param str proxy: proxy to use
    :param int valid: proxy validity period
    :return:  S_OK/S_ERROR
    """
    ComputingElement.setProxy(self, proxy, valid)
    if self.ceParameters.get('SSHType', 'ssh') == 'gsissh':
      result = self._prepareProxy()
      if not result['OK']:
        gLogger.error('SSHComputingElement: failed to set up proxy', result['Message'])
        return result
    return S_OK()

  #############################################################################
  def _addCEConfigDefaults(self):
    """Method to make sure all necessary Configuration Parameters are defined
    """
    # First assure that any global parameters are loaded
    ComputingElement._addCEConfigDefaults(self)
    # Now batch system specific ones
    if 'ExecQueue' not in self.ceParameters:
      self.ceParameters['ExecQueue'] = self.ceParameters.get('Queue', '')

    if 'SharedArea' not in self.ceParameters:
      # . isn't a good location, move to $HOME
      self.ceParameters['SharedArea'] = '$HOME'

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

  def _reset(self):
    """ Process CE parameters and make necessary adjustments
    """
    batchSystemName = self.ceParameters.get('BatchSystem', 'Host')
    if 'BatchSystem' not in self.ceParameters:
      self.ceParameters['BatchSystem'] = batchSystemName
    result = self.loadBatchSystem(batchSystemName)
    if not result['OK']:
      self.log.error('Failed to load the batch system plugin %s', batchSystemName)
      return result

    self.user = self.ceParameters['SSHUser']
    self.queue = self.ceParameters['Queue']
    self.submitOptions = self.ceParameters.get('SubmitOptions', '')
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
      result = self.loadParallelLibrary(parallelLibraryName)
      if not result['OK']:
        self.log.error('Failed to load the parallel library plugin %s', parallelLibraryName)
        return result

    self.submitOptions = self.ceParameters.get('SubmitOptions', '')
    self.removeOutput = True
    if 'RemoveOutput' in self.ceParameters:
      if self.ceParameters['RemoveOutput'].lower() in ['no', 'false', '0']:
        self.removeOutput = False
    self.preamble = self.ceParameters.get('Preamble', '')

    result = self._prepareRemoteHost()
    if not result['OK']:
      return result

    return S_OK()

  def _prepareRemoteHost(self, host=None):
    """ Prepare remote directories and upload control script
    """

    ssh = SSH(host=host, parameters=self.ceParameters)

    # Make remote directories
    dirTuple = tuple(uniqueElements([self.sharedArea,
                                     self.executableArea,
                                     self.infoArea,
                                     self.batchOutput,
                                     self.batchError,
                                     self.workArea]))
    nDirs = len(dirTuple)
    cmd = 'mkdir -p %s; ' * nDirs % dirTuple
    cmd = "bash -c '%s'" % cmd
    self.log.verbose('Creating working directories on %s' % self.ceParameters['SSHHost'])
    result = ssh.sshCall(30, cmd)
    if not result['OK']:
      self.log.error('Failed creating working directories', '(%s)' % result['Message'][1])
      return result
    status, output, _error = result['Value']
    if status == -1:
      self.log.error('Timeout while creating directories')
      return S_ERROR(errno.ETIME, 'Timeout while creating directories')
    if "cannot" in output:
      self.log.error('Failed to create directories', '(%s)' % output)
      return S_ERROR(errno.EACCES, 'Failed to create directories')

    # Upload the control script now
    result = self._generateControlScript()
    if not result['OK']:
      self.log.warn('Failed generating control script')
      return result
    localScript = result['Value']
    self.log.verbose('Uploading %s script to %s' % (self.batchSystem.__class__.__name__, self.ceParameters['SSHHost']))
    remoteScript = '%s/execute_batch' % self.sharedArea
    result = ssh.scpCall(30,
                         localScript,
                         remoteScript,
                         postUploadCommand='chmod +x %s' % remoteScript)
    if not result['OK']:
      self.log.warn('Failed uploading control script: %s' % result['Message'][1])
      return result
    status, output, _error = result['Value']
    if status != 0:
      if status == -1:
        self.log.warn('Timeout while uploading control script')
        return S_ERROR('Timeout while uploading control script')
      self.log.warn('Failed uploading control script: %s' % output)
      return S_ERROR('Failed uploading control script')

    # Delete the generated control script locally
    try:
      os.remove(localScript)
    except OSError:
      self.log.warn('Failed removing the generated control script locally')
      return S_ERROR('Failed removing the generated control script locally')

    # Chmod the control scripts
    # self.log.verbose( 'Chmod +x control script' )
    # result = ssh.sshCall( 10, "chmod +x %s/%s" % ( self.sharedArea, self.controlScript ) )
    # if not result['OK']:
    #  self.log.warn( 'Failed chmod control script: %s' % result['Message'][1] )
    #  return result
    # status, output, _error = result['Value']
    # if status != 0:
    #  if status == -1:
    #    self.log.warn( 'Timeout while chmod control script' )
    #    return S_ERROR( 'Timeout while chmod control script' )
    #  else:
    #    self.log.warn( 'Failed uploading chmod script: %s' % output )
    #    return S_ERROR( 'Failed uploading chmod script' )

    return S_OK()

  def _generateControlScript(self):
    """ Generates a control script from a BatchSystem and a script called executeBatch

        :return: a path containing the script generated
    """
    # Get the batch system module to use
    batchSystemDir = os.path.join(os.path.dirname(DIRAC.__file__), "Resources", "Computing", "BatchSystems")
    batchSystemScript = os.path.join(batchSystemDir, '%s.py' % self.batchSystem.__class__.__name__)

    # Get the executeBatch.py content: an str variable composed of code content that has to be extracted
    # The control script is generated from the batch system module and this variable
    controlScript = os.path.join(batchSystemDir, "control_script.py")

    try:
      shutil.copyfile(batchSystemScript, controlScript)
      with open(controlScript, "a") as cs:
        cs.write(executeBatchContent)
    except IOError:
      return S_ERROR('IO Error trying to generate control script')

    return S_OK('%s' % controlScript)

  def __executeHostCommand(self, command, options, ssh=None, host=None):

    if not ssh:
      ssh = SSH(host=host, parameters=self.ceParameters)

    options['BatchSystem'] = self.batchSystem.__class__.__name__
    options['Method'] = command
    options['SharedDir'] = self.sharedArea
    options['OutputDir'] = self.batchOutput
    options['ErrorDir'] = self.batchError
    options['WorkDir'] = self.workArea
    options['InfoDir'] = self.infoArea
    options['ExecutionContext'] = self.execution
    options['User'] = self.user
    options['Queue'] = self.queue

    options = json.dumps(options)
    options = urlquote(options)

    cmd = "bash --login -c 'python %s/execute_batch %s'" % (self.sharedArea, options)

    self.log.verbose('CE submission command: %s' % cmd)

    result = ssh.sshCall(120, cmd)
    if not result['OK']:
      self.log.error('%s CE job submission failed' % self.ceType, result['Message'])
      return result

    sshStatus = result['Value'][0]
    sshStdout = result['Value'][1]
    sshStderr = result['Value'][2]

    # Examine results of the job submission
    if sshStatus == 0:
      output = sshStdout.strip().replace('\r', '').strip()
      try:
        index = output.index('============= Start output ===============')
        output = output[index + 42:]
      except Exception:
        return S_ERROR("Invalid output from remote command: %s" % output)
      try:
        output = urlunquote(output)
        result = json.loads(output)
        if isinstance(result, six.string_types) and result.startswith('Exception:'):
          return S_ERROR(result)
        return S_OK(result)
      except Exception:
        return S_ERROR('Invalid return structure from job submission')
    else:
      return S_ERROR('\n'.join([sshStdout, sshStderr]))

  def submitJob(self, executableFile, proxy, numberOfJobs=1):

    #    self.log.verbose( "Executable file path: %s" % executableFile )
    if not os.access(executableFile, 5):
      os.chmod(executableFile, stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)

    # if no proxy is supplied, the executable can be submitted directly
    # otherwise a wrapper script is needed to get the proxy to the execution node
    # The wrapper script makes debugging more complicated and thus it is
    # recommended to transfer a proxy inside the executable if possible.
    if proxy:
      self.log.verbose('Setting up proxy for payload')
      wrapperContent = bundleProxy(executableFile, proxy)
      name = writeScript(wrapperContent, os.getcwd())
      submitFile = name
    else:  # no proxy
      submitFile = executableFile

    inputs = []
    if self.parallelLibrary:
      # In this case, the executable becomes an input of a parallel library script.
      # It needs to be submitted along with the submitFile, which is a parallel library wrapper.
      inputFile = os.path.join(self.executableArea, os.path.basename(submitFile))
      inputs.append(inputFile)
      submitFile = self.parallelLibrary.generateWrapper(inputFile)

    result = self._submitJobToHost(submitFile, numberOfJobs, inputs=inputs)
    if proxy or self.parallelLibrary:
      os.remove(submitFile)

    return result

  def _submitJobToHost(self, executableFile, numberOfJobs, host=None, inputs=None):
    """  Submit prepared executable to the given host
    """
    ssh = SSH(host=host, parameters=self.ceParameters)
    # Copy the executable
    submitFile = os.path.join(self.executableArea, os.path.basename(executableFile))
    result = ssh.scpCall(30, executableFile, submitFile, postUploadCommand='chmod +x %s' % submitFile)
    if not result['OK']:
      return result

    # Copy the executable dependencies if any
    if inputs:
      for localInput in inputs:
        remoteInput = os.path.join(self.executableArea, os.path.basename(localInput))
        result = ssh.scpCall(30, localInput, remoteInput, postUploadCommand='chmod +x %s' % remoteInput)
        if not result['OK']:
          return result

    jobStamps = []
    for _i in range(numberOfJobs):
      jobStamps.append(makeGuid()[:8])

    numberOfProcessors = self.ceParameters.get('NumberOfProcessors', 1)
    wholeNode = self.ceParameters.get('WholeNode', False)
    # numberOfNodes is treated as a string as it can contain values such as "2-4"
    # where 2 would represent the minimum number of nodes to allocate, and 4 the maximum
    numberOfNodes = self.ceParameters.get('NumberOfNodes', '1')
    self.numberOfGPUs = self.ceParameters.get("NumberOfGPUs")

    # Collect command options
    commandOptions = {
        "Executable": submitFile,
        "NJobs": numberOfJobs,
        "SubmitOptions": self.submitOptions,
        "JobStamps": jobStamps,
        "WholeNode": wholeNode,
        "NumberOfProcessors": numberOfProcessors,
        "NumberOfNodes": numberOfNodes,
        "Preamble": self.preamble,
        "NumberOfGPUs": self.numberOfGPUs,
    }
    resultCommand = self.__executeHostCommand('submitJob', commandOptions, ssh=ssh, host=host)
    if not resultCommand['OK']:
      return resultCommand

    result = resultCommand['Value']
    if result['Status'] != 0:
      return S_ERROR('Failed job submission: %s' % result['Message'])
    else:
      batchIDs = result['Jobs']
      if batchIDs:
        ceHost = host
        if host is None:
          ceHost = self.ceName
        batchSystemName = self.batchSystem.__class__.__name__.lower()
        jobIDs = ['%s%s://%s/%s' % (self.ceType.lower(), batchSystemName, ceHost, _id) for _id in batchIDs]
      else:
        return S_ERROR('No jobs IDs returned')

    result = S_OK(jobIDs)
    self.submittedJobs += len(batchIDs)

    return result

  def killJob(self, jobIDList):
    """ Kill a bunch of jobs
    """
    if isinstance(jobIDList, six.string_types):
      jobIDList = [jobIDList]
    return self._killJobOnHost(jobIDList)

  def _killJobOnHost(self, jobIDList, host=None):
    """ Kill the jobs for the given list of job IDs
    """
    jobDict = {}
    for job in jobIDList:
      stamp = os.path.basename(urlparse(job).path)
      jobDict[stamp] = job
    stampList = list(jobDict)

    commandOptions = {'JobIDList': stampList, 'User': self.user}
    resultCommand = self.__executeHostCommand('killJob', commandOptions, host=host)
    if not resultCommand['OK']:
      return resultCommand

    result = resultCommand['Value']
    if result['Status'] != 0:
      return S_ERROR('Failed job kill: %s' % result['Message'])

    if result['Failed']:
      return S_ERROR('%d jobs failed killing' % len(result['Failed']))

    return S_OK(len(result['Successful']))

  def _getHostStatus(self, host=None):
    """ Get jobs running at a given host
    """
    resultCommand = self.__executeHostCommand('getCEStatus', {}, host=host)
    if not resultCommand['OK']:
      return resultCommand

    result = resultCommand['Value']
    if result['Status'] != 0:
      return S_ERROR('Failed to get CE status: %s' % result['Message'])

    return S_OK(result)

  def getCEStatus(self):
    """ Method to return information on running and pending jobs.
    """
    result = S_OK()
    result['SubmittedJobs'] = self.submittedJobs
    result['RunningJobs'] = 0
    result['WaitingJobs'] = 0

    resultHost = self._getHostStatus()
    if not resultHost['OK']:
      return resultHost

    result['RunningJobs'] = resultHost['Value'].get('Running', 0)
    result['WaitingJobs'] = resultHost['Value'].get('Waiting', 0)
    if "AvailableCores" in resultHost['Value']:
      result['AvailableCores'] = resultHost['Value']['AvailableCores']
    self.log.verbose('Waiting Jobs: ', result['WaitingJobs'])
    self.log.verbose('Running Jobs: ', result['RunningJobs'])

    return result

  def getJobStatus(self, jobIDList):
    """ Get the status information for the given list of jobs
    """
    return self._getJobStatusOnHost(jobIDList)

  def _getJobStatusOnHost(self, jobIDList, host=None):
    """ Get the status information for the given list of jobs
    """

    resultDict = {}
    jobDict = {}
    for job in jobIDList:
      stamp = os.path.basename(urlparse(job).path)
      jobDict[stamp] = job
    stampList = list(jobDict)

    for jobList in breakListIntoChunks(stampList, 100):
      resultCommand = self.__executeHostCommand('getJobStatus', {'JobIDList': jobList}, host=host)
      if not resultCommand['OK']:
        return resultCommand

      result = resultCommand['Value']
      if result['Status'] != 0:
        return S_ERROR('Failed to get job status: %s' % result['Message'])

      for stamp in result['Jobs']:
        resultDict[jobDict[stamp]] = result['Jobs'][stamp]

    return S_OK(resultDict)

  def _getJobOutputFiles(self, jobID, host=None):
    """ Get output file names for the specific CE
    """
    jobStamp = os.path.basename(urlparse(jobID).path)
    host = urlparse(jobID).hostname

    if 'OutputTemplate' in self.ceParameters:
      self.outputTemplate = self.ceParameters['OutputTemplate']
      self.errorTemplate = self.ceParameters['ErrorTemplate']

    if self.outputTemplate:
      output = self.outputTemplate % jobStamp
      error = self.errorTemplate % jobStamp
    elif 'OutputTemplate' in self.ceParameters:
      self.outputTemplate = self.ceParameters['OutputTemplate']
      self.errorTemplate = self.ceParameters['ErrorTemplate']
      output = self.outputTemplate % jobStamp
      error = self.errorTemplate % jobStamp
    elif hasattr(self.batchSystem, 'getJobOutputFiles'):
      commandOptions = {'JobIDList': [jobStamp],
                        'OutputDir': self.batchOutput,
                        'ErrorDir': self.batchError}
      resultCommand = self.__executeHostCommand('getJobOutputFiles', commandOptions, host=host)
      if not resultCommand['OK']:
        return resultCommand

      result = resultCommand['Value']
      if result['Status'] != 0:
        return S_ERROR('Failed to get job output files: %s' % result['Message'])

      if 'OutputTemplate' in result:
        self.outputTemplate = result['OutputTemplate']
        self.errorTemplate = result['ErrorTemplate']

      output = result['Jobs'][jobStamp]['Output']
      error = result['Jobs'][jobStamp]['Error']
    else:
      output = '%s/%s.out' % (self.batchOutput, jobStamp)
      error = '%s/%s.err' % (self.batchError, jobStamp)

    return S_OK((jobStamp, host, output, error))

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

    if localDir:
      localOutputFile = '%s/%s.out' % (localDir, jobStamp)
      localErrorFile = '%s/%s.err' % (localDir, jobStamp)
    else:
      localOutputFile = 'Memory'
      localErrorFile = 'Memory'

    ssh = SSH(parameters=self.ceParameters)
    result = ssh.scpCall(30, localOutputFile, outputFile, upload=False)
    if not result['OK']:
      return result

    result = ssh.scpCall(30, localErrorFile, errorFile, upload=False)
    if not result['OK']:
      return result

    if localDir:
      output = localOutputFile
      error = localErrorFile
    else:
      output = result['Value'][1]
      error = result['Value'][1]

    if self.parallelLibrary:
      output, error = self.parallelLibrary.processOutput(output, error, isFile=bool(localDir))

    return S_OK((output, error))
