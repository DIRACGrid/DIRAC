""" SingularityCE is a type of "inner" CEs
    (meaning it's used by a jobAgent inside a pilot).
    A computing element class using singularity containers,
    where Singularity is supposed to be found on the WN.

    The goal of this CE is to start the job in the container set by
    the "ContainerRoot" config option.

    DIRAC can be re-installed within the container, extra flags can
    be given to the dirac-install command with the "ContainerExtraOpts"
    option.

    See the Configuration/Resources/Computing documention for details on
    where to set the option parameters.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import io
import shutil
import tempfile
import json
import six

import DIRAC
from DIRAC import S_OK, S_ERROR, gConfig, gLogger
from DIRAC.Core.Utilities.Subprocess import systemCall
from DIRAC.ConfigurationSystem.Client.Helpers import CSGlobals
from DIRAC.ConfigurationSystem.Client.Helpers import Operations
from DIRAC.Core.Utilities.ThreadScheduler import gThreadScheduler
from DIRAC.Resources.Computing.ComputingElement import ComputingElement
from DIRAC.WorkloadManagementSystem.Utilities.Utils import createRelocatedJobWrapper

__RCSID__ = "$Id$"


DIRAC_INSTALL = os.path.join(DIRAC.rootPath, 'DIRAC', 'Core', 'scripts', 'dirac-install.py')
# Default container to use if it isn't specified in the CE options
CONTAINER_DEFROOT = "/cvmfs/cernvm-prod.cern.ch/cvm3"
CONTAINER_WORKDIR = "DIRAC_containers"
CONTAINER_INNERDIR = "/tmp"


# What is executed inside the container (2 options given)

CONTAINER_WRAPPER_INSTALL = """#!/bin/bash

echo "Starting inner container wrapper scripts (uses dirac-install) at `date`."
set -x
cd /tmp
# Avoid using the host's DIRAC(OS) installation
unset DIRAC
unset DIRACOS
# Install DIRAC
./dirac-install.py %(install_args)s
source bashrc
dirac-configure -F %(config_args)s -I
# Add compatibility with pilot3 where config is in pilot.cfg
ln -s etc/dirac.cfg pilot.cfg
# Run next wrapper (to start actual job)
bash %(next_wrapper)s
# Write the payload errorcode to a file for the outer scripts
echo $? > retcode
chmod 644 retcode
echo "Finishing inner container wrapper scripts at `date`."

"""
# Path to a directory on CVMFS to use as a fallback if no
# other version found: Only used if node has user namespaces
FALLBACK_SINGULARITY = "/cvmfs/oasis.opensciencegrid.org/mis/singularity/current/bin"

CONTAINER_WRAPPER_NO_INSTALL = """#!/bin/bash

echo "Starting inner container wrapper scripts (no install) at `date`."
set -x
cd /tmp
# In any case we need to find a bashrc, and a pilot.cfg, both created by the pilot
source bashrc
# Run next wrapper (to start actual job)
bash %(next_wrapper)s
# Write the payload errorcode to a file for the outer scripts
echo $? > retcode
chmod 644 retcode
echo "Finishing inner container wrapper scripts at `date`."

"""


class SingularityComputingElement(ComputingElement):
  """ A Computing Element for running a job within a Singularity container.
  """

  def __init__(self, ceUniqueID):
    """ Standard constructor.
    """
    super(SingularityComputingElement, self).__init__(ceUniqueID)
    self.__submittedJobs = 0
    self.__runningJobs = 0
    self.__root = CONTAINER_DEFROOT
    if 'ContainerRoot' in self.ceParameters:
      self.__root = self.ceParameters['ContainerRoot']
    self.__workdir = CONTAINER_WORKDIR
    self.__innerdir = CONTAINER_INNERDIR
    self.__singularityBin = 'singularity'
    self.__installDIRACInContainer = self.ceParameters.get('InstallDIRACInContainer', True)
    if isinstance(self.__installDIRACInContainer, six.string_types) and \
       self.__installDIRACInContainer.lower() in ('false', 'no'):
      self.__installDIRACInContainer = False

    self.processors = int(self.ceParameters.get('NumberOfProcessors', 1))

  def __hasUserNS(self):
    """ Detect if this node has user namespaces enabled.
        Returns True if they are enabled, False otherwise.
    """
    try:
      with open("/proc/sys/user/max_user_namespaces", "r") as proc_fd:
        maxns = int(proc_fd.readline().strip())
        # Any "reasonable number" of namespaces is sufficient
        return (maxns > 100)
    except Exception:
      # Any failure, missing file, doesn't contain a number, etc. and we
      # assume they are disabled.
      return False

  def __hasSingularity(self):
    """ Search the current PATH for an exectuable named singularity.
        Returns True if it is found, False otherwise.
    """
    if self.ceParameters.get('ContainerBin'):
      binPath = self.ceParameters['ContainerBin']
      if os.path.isfile(binPath) and os.access(binPath, os.X_OK):
        self.__singularityBin = binPath
        self.log.debug('Use singularity from "%s"' % self.__singularityBin)
        return True
    if "PATH" not in os.environ:
      return False  # Hmm, PATH not set? How unusual...
    searchPaths = os.environ["PATH"].split(os.pathsep)
    # We can use CVMFS as a last resort if userNS is enabled
    if self.__hasUserNS():
      searchPaths.append(FALLBACK_SINGULARITY)
    for searchPath in searchPaths:
      binPath = os.path.join(searchPath, 'singularity')
      if os.path.isfile(binPath):
        # File found, check it's executable to be certain:
        if os.access(binPath, os.X_OK):
          self.log.debug('Found singularity at "%s"' % binPath)
          self.__singularityBin = binPath
          return True
    # No suitable binaries found
    return False

  @staticmethod
  def __findInstallBaseDir():
    """Find the path to root of the current DIRAC installation"""
    candidate = os.path.join(DIRAC.rootPath, "bashrc")
    return os.path.dirname(os.path.realpath(candidate))

  def __getInstallFlags(self, infoDict=None):
    """ Get the flags to pass to dirac-install.py inside the container.
        Returns a string containing the command line flags.
    """
    if not infoDict:
      infoDict = {}

    instOpts = []

    setup = infoDict.get('DefaultSetup')
    if not setup:
      setup = list(infoDict.get('Setups'))[0]
    if not setup:
      setup = gConfig.getValue("/DIRAC/Setup", "unknown")
    setup = str(setup)

    installationName = str(infoDict.get('Installation'))
    if not installationName or installationName == 'None':
      installationName = Operations.Operations(setup=setup).getValue("Pilot/Installation", "")
    if installationName:
      instOpts.append('-V %s' % installationName)

    diracVersions = str(infoDict['Setups'][setup].get('Version')).split(',')
    if not diracVersions:
      diracVersions = str(infoDict['Setups']['Defaults'].get('Version')).split(',')
    if not diracVersions:
      diracVersions = Operations.Operations(setup=setup).getValue("Pilot/Version", [])
    instOpts.append("-r '%s'" % diracVersions[0].strip())

    pilotExtensionsList = str(infoDict['Setups'][setup].get('CommandExtensions')).split(',')
    if not pilotExtensionsList:
      pilotExtensionsList = str(infoDict['Setups']['Defaults'].get('CommandExtensions')).split(',')
    if not pilotExtensionsList:
      pilotExtensionsList = Operations.Operations(setup=setup).getValue("Pilot/Extensions", [])
    extensionsList = []
    if pilotExtensionsList:
      if pilotExtensionsList[0] != 'None':
        extensionsList = pilotExtensionsList
    else:
      extensionsList = CSGlobals.getCSExtensions()
    if extensionsList:
      instOpts.append("-e '%s'" % ','.join([ext for ext in extensionsList if 'Web' not in ext]))
    if 'ContainerExtraOpts' in self.ceParameters:
      instOpts.append(self.ceParameters['ContainerExtraOpts'])
    return ' '.join(instOpts)

  @staticmethod
  def __getConfigFlags(infoDict=None):
    """ Get the flags for dirac-configure inside the container.
        Returns a string containing the command line flags.
    """
    if not infoDict:
      infoDict = {}

    cfgOpts = []

    setup = infoDict.get('DefaultSetup')
    if not setup:
      setup = gConfig.getValue("/DIRAC/Setup", "unknown")
    cfgOpts.append("-S '%s'" % setup)

    csServers = infoDict.get('ConfigurationServers')
    if not csServers:
      csServers = gConfig.getValue("/DIRAC/Configuration/Servers", [])
    cfgOpts.append("-C '%s'" % ','.join([str(ce) for ce in csServers]))
    cfgOpts.append("-n '%s'" % DIRAC.siteName())
    return ' '.join(cfgOpts)

  def __createWorkArea(self, jobDesc=None, log=None, logLevel='INFO', proxy=None):
    """ Creates a directory for the container and populates it with the
        template directories, scripts & proxy.
    """
    if not jobDesc:
      jobDesc = {}
    if not log:
      log = gLogger

    # Create the directory for our container area
    try:
      os.mkdir(self.__workdir)
    except OSError:
      if not os.path.isdir(self.__workdir):
        result = S_ERROR("Failed to create container base directory '%s'" % self.__workdir)
        result['ReschedulePayload'] = True
        return result
      # Otherwise, directory probably just already exists...
    baseDir = None
    try:
      baseDir = tempfile.mkdtemp(prefix="job%s_" % jobDesc.get('jobID', 0), dir=self.__workdir)
    except OSError:
      result = S_ERROR("Failed to create container work directory in '%s'" % self.__workdir)
      result['ReschedulePayload'] = True
      return result

    self.log.debug('Use singularity workarea: %s' % baseDir)
    for subdir in ["home", "tmp", "var_tmp"]:
      os.mkdir(os.path.join(baseDir, subdir))
    tmpDir = os.path.join(baseDir, "tmp")

    # Now we have a directory, we can stage in the proxy and scripts
    # Proxy
    if proxy:
      proxyLoc = os.path.join(tmpDir, "proxy")
      rawfd = os.open(proxyLoc, os.O_WRONLY | os.O_CREAT, 0o600)
      fd = os.fdopen(rawfd, "wb")
      fd.write(proxy)
      fd.close()
    else:
      self.log.warn("No user proxy")

    # dirac-install.py
    install_loc = os.path.join(tmpDir, "dirac-install.py")
    shutil.copyfile(DIRAC_INSTALL, install_loc)
    os.chmod(install_loc, 0o755)

    # Job Wrapper (Standard-ish DIRAC wrapper)
    result = createRelocatedJobWrapper(wrapperPath=tmpDir,
                                       rootLocation=self.__innerdir,
                                       jobID=jobDesc.get('jobID', 0),
                                       jobParams=jobDesc.get('jobParams', {}),
                                       resourceParams=jobDesc.get('resourceParams', {}),
                                       optimizerParams=jobDesc.get('optimizerParams', {}),
                                       log=log,
                                       logLevel=logLevel)
    if not result['OK']:
      result['ReschedulePayload'] = True
      return result
    wrapperPath = result['Value']

    if self.__installDIRACInContainer:
      # dirac-install.py
      install_loc = os.path.join(tmpDir, "dirac-install.py")
      shutil.copyfile(DIRAC_INSTALL, install_loc)
      os.chmod(install_loc, 0o755)

      infoDict = None
      if os.path.isfile('pilot.json'):  # if this is a pilot 3 this file should be found
        with io.open('pilot.json') as pj:
          infoDict = json.load(pj)

      # Extra Wrapper (Container DIRAC installer)
      wrapSubs = {'next_wrapper': wrapperPath,
                  'install_args': self.__getInstallFlags(infoDict),
                  'config_args': self.__getConfigFlags(infoDict),
                  }
      CONTAINER_WRAPPER = CONTAINER_WRAPPER_INSTALL

    else:  # In case we don't (re)install DIRAC
      shutil.copyfile(
          os.path.join(self.__findInstallBaseDir(), 'bashrc'),
          os.path.join(tmpDir, 'bashrc'),
      )
      shutil.copyfile('pilot.cfg', os.path.join(tmpDir, 'pilot.cfg'))
      wrapSubs = {'next_wrapper': wrapperPath}
      CONTAINER_WRAPPER = CONTAINER_WRAPPER_NO_INSTALL

    wrapLoc = os.path.join(tmpDir, "dirac_container.sh")
    rawfd = os.open(wrapLoc, os.O_WRONLY | os.O_CREAT, 0o700)
    fd = os.fdopen(rawfd, "w")
    fd.write(CONTAINER_WRAPPER % wrapSubs)
    fd.close()

    ret = S_OK()
    ret['baseDir'] = baseDir
    ret['tmpDir'] = tmpDir
    if proxy:
      ret['proxyLocation'] = proxyLoc
    return ret

  def __deleteWorkArea(self, baseDir):
    """ Deletes the container work area (baseDir path) unless 'KeepWorkArea'
        option is set. Returns None.
    """
    if self.ceParameters.get('KeepWorkArea', False):
      return
    # We can't really do anything about errors: The tree should be fully owned
    # by the pilot user, so we don't expect any permissions problems.
    shutil.rmtree(baseDir, ignore_errors=True)

  def __getEnv(self):
    """ Gets the environment for use within the container.
        We blank almost everything to prevent contamination from the host system.
    """
    payloadEnv = {}
    if 'TERM' in os.environ:
      payloadEnv['TERM'] = os.environ['TERM']
    payloadEnv['TMP'] = '/tmp'
    payloadEnv['TMPDIR'] = '/tmp'
    payloadEnv['X509_USER_PROXY'] = os.path.join(self.__innerdir, "proxy")
    return payloadEnv

  @staticmethod
  def __checkResult(tmpDir):
    """ Gets the result of the payload command and returns it. """
    # The wrapper writes the inner job return code to "retcode"
    # in the working directory.
    try:
      fd = open(os.path.join(tmpDir, "retcode"), "r")
      retCode = int(fd.read())
      fd.close()
    except (IOError, ValueError):
      # Something failed while trying to get the return code
      result = S_ERROR("Failed to get return code from inner wrapper")
      result['ReschedulePayload'] = True
      return result

    result = S_OK()
    if retCode:
      # This is the one case where we don't reschedule:
      # An actual failure of the inner payload for some reason
      result = S_ERROR("Command failed with exit code %d" % retCode)
    return result

  def submitJob(self, executableFile, proxy=None, **kwargs):
    """ Start a container for a job.
        executableFile is ignored. A new wrapper suitable for running in a
        container is created from jobDesc.
    """
    rootImage = self.__root

    # Check that singularity is available
    if not self.__hasSingularity():
      self.log.error('Singularity is not installed on PATH.')
      result = S_ERROR("Failed to find singularity ")
      result['ReschedulePayload'] = True
      return result

    self.log.info('Creating singularity container')

    # Start by making the directory for the container
    ret = self.__createWorkArea(kwargs.get('jobDesc'),
                                kwargs.get('log'),
                                kwargs.get('logLevel', 'INFO'),
                                proxy)
    if not ret['OK']:
      return ret
    baseDir = ret['baseDir']
    tmpDir = ret['tmpDir']

    if proxy:
      payloadProxyLoc = ret['proxyLocation']

      # Now we have to set-up payload proxy renewal for the container
      # This is fairly easy as it remains visible on the host filesystem
      result = gThreadScheduler.addPeriodicTask(self.proxyCheckPeriod, self._monitorProxy,
                                                taskArgs=(payloadProxyLoc,),
                                                executions=0, elapsedTime=0)
      if result['OK']:
        renewTask = result['Value']
      else:
        self.log.warn('Failed to start proxy renewal task')
        renewTask = None

    # Very simple accounting
    self.__submittedJobs += 1
    self.__runningJobs += 1

    # Now prepare start singularity
    # Mount /cvmfs in if it exists on the host
    withCVMFS = os.path.isdir("/cvmfs")
    innerCmd = os.path.join(self.__innerdir, "dirac_container.sh")
    cmd = [self.__singularityBin, "exec"]
    cmd.extend(["--contain"])  # use minimal /dev and empty other directories (e.g. /tmp and $HOME)
    cmd.extend(["--ipc", "--pid"])  # run container in new IPC and PID namespaces
    cmd.extend(["--workdir", baseDir])  # working directory to be used for /tmp, /var/tmp and $HOME
    if self.__hasUserNS():
      cmd.append("--userns")
    if withCVMFS:
      cmd.extend(["--bind", "/cvmfs"])
    if not self.__installDIRACInContainer:
      cmd.extend(["--bind", "{0}:{0}:ro".format(self.__findInstallBaseDir())])
    if 'ContainerBind' in self.ceParameters:
      bindPaths = self.ceParameters['ContainerBind'].split(',')
      for bindPath in bindPaths:
        if len(bindPath.split(':::')) == 1:
          cmd.extend(["--bind", bindPath.strip()])
        elif len(bindPath.split(':::')) in [2, 3]:
          cmd.extend(["--bind", ":".join([bp.strip() for bp in bindPath.split(':::')])])
    if 'ContainerOptions' in self.ceParameters:
      containerOpts = self.ceParameters['ContainerOptions'].split(',')
      for opt in containerOpts:
        cmd.extend([opt.strip()])
    if os.path.isdir(rootImage) or os.path.isfile(rootImage):
      cmd.extend([rootImage, innerCmd])
    else:
      # if we are here is because there's no image, or it is not accessible (e.g. not on CVMFS)
      self.log.error('Singularity image to exec not found: ', rootImage)
      result = S_ERROR("Failed to find singularity image to exec")
      result['ReschedulePayload'] = True
      return result

    self.log.debug('Execute singularity command: %s' % cmd)
    self.log.debug('Execute singularity env: %s' % self.__getEnv())
    result = systemCall(0, cmd, callbackFunction=self.sendOutput, env=self.__getEnv())

    self.__runningJobs -= 1

    if not result["OK"]:
      self.log.error('Fail to run Singularity', result['Message'])
      if proxy and renewTask:
        gThreadScheduler.removeTask(renewTask)
      self.__deleteWorkArea(baseDir)
      result = S_ERROR("Error running singularity command")
      result['ReschedulePayload'] = True
      return result

    result = self.__checkResult(tmpDir)
    if proxy and renewTask:
      gThreadScheduler.removeTask(renewTask)
    self.__deleteWorkArea(baseDir)
    return result

  def getCEStatus(self):
    """ Method to return information on running and pending jobs.
    """
    result = S_OK()
    result['SubmittedJobs'] = self.__submittedJobs
    result['RunningJobs'] = self.__runningJobs
    result['WaitingJobs'] = 0
    # processors
    result['AvailableProcessors'] = self.processors
    return result
