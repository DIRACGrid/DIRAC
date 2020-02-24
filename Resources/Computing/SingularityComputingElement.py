""" A computing element class using singularity containers.

    This computing element will start the job in the container set by
    the "ContainerRoot" config option.

    DIRAC will the re-installed within the container, extra flags can
    be given to the dirac-install command with the "ContainerExtraOpts"
    option.

    See the Configuration/Resources/Computing documention for details on
    where to set the option parameters.
"""

import os
import sys
import shutil
import tempfile

import DIRAC
from DIRAC import S_OK, S_ERROR, gConfig, gLogger
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
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
CONTAINER_WORKDIR = "containers"
CONTAINER_INNERDIR = "/tmp"
CONTAINER_WRAPPER = """#!/bin/bash

echo "Starting inner container wrapper scripts at `date`."
set -x
cd /tmp
# Install DIRAC
./dirac-install.py %(install_args)s
source bashrc
dirac-configure -F %(config_args)s -I
# Run next wrapper (to start actual job)
bash %(next_wrapper)s
# Write the payload errorcode to a file for the outer scripts
echo $? > retcode
chmod 644 retcode
echo "Finishing inner continer wrapper scripts at `date`."

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

    self.log = gLogger.getSubLogger('Singularity')

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
    for searchPath in os.environ["PATH"].split(os.pathsep):
      binPath = os.path.join(searchPath, 'singularity')
      if os.path.isfile(binPath):
        # File found, check it's exectuable to be certain:
        if os.access(binPath, os.X_OK):
          self.log.debug('Find singularity from PATH "%s"' % binPath)
          return True
    # No suitablable binaries found
    return False

  def __getInstallFlags(self):
    """ Get the flags to pass to dirac-install.py inside the container.
        Returns a string containing the command line flags.
    """
    instOpts = []
    setup = gConfig.getValue("/DIRAC/Setup", "unknown")
    opsHelper = Operations.Operations(setup=setup)

    installationName = opsHelper.getValue("Pilot/Installation", "")
    if installationName:
      instOpts.append('-V %s' % installationName)

    diracVersions = opsHelper.getValue("Pilot/Version", [])
    instOpts.append("-r '%s'" % diracVersions[0])

    pyVer = "%u%u" % (sys.version_info.major, sys.version_info.minor)
    instOpts.append("-i %s" % pyVer)
    pilotExtensionsList = opsHelper.getValue("Pilot/Extensions", [])
    extensionsList = []
    if pilotExtensionsList:
      if pilotExtensionsList[0] != 'None':
        extensionsList = pilotExtensionsList
    else:
      extensionsList = CSGlobals.getCSExtensions()
    if extensionsList:
      instOpts.append("-e '%s'" % ','.join([ext for ext in extensionsList if 'Web' not in ext]))
    lcgVer = opsHelper.getValue("Pilot/LCGBundleVersion", None)
    if lcgVer:
      instOpts.append("-g %s" % lcgVer)
    if 'ContainerExtraOpts' in self.ceParameters:
      instOpts.append(self.ceParameters['ContainerExtraOpts'])
    return ' '.join(instOpts)

  @staticmethod
  def __getConfigFlags():
    """ Get the flags for dirac-configure inside the container.
        Returns a string containing the command line flags.
    """
    cfgOpts = []
    setup = gConfig.getValue("/DIRAC/Setup", "unknown")
    if setup:
      cfgOpts.append("-S '%s'" % setup)
    csServers = gConfig.getValue("/DIRAC/Configuration/Servers", [])
    cfgOpts.append("-C '%s'" % ','.join(csServers))
    cfgOpts.append("-n '%s'" % DIRAC.siteName())
    return ' '.join(cfgOpts)

  def __createWorkArea(self, proxy, jobDesc, log, logLevel):
    """ Creates a directory for the container and populates it with the
        template directories, scripts & proxy.
    """

    # Create the directory for our continer area
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
      baseDir = tempfile.mkdtemp(prefix="job%s_" % jobDesc["jobID"], dir=self.__workdir)
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
    proxyLoc = os.path.join(tmpDir, "proxy")
    rawfd = os.open(proxyLoc, os.O_WRONLY | os.O_CREAT, 0o600)
    fd = os.fdopen(rawfd, "w")
    fd.write(proxy)
    fd.close()

    # dirac-install.py
    install_loc = os.path.join(tmpDir, "dirac-install.py")
    shutil.copyfile(DIRAC_INSTALL, install_loc)
    os.chmod(install_loc, 0o755)

    # Job Wrapper (Standard DIRAC wrapper)
    result = createRelocatedJobWrapper(tmpDir, self.__innerdir,
                                       log=log, logLevel=logLevel, **jobDesc)
    if not result['OK']:
      result['ReschedulePayload'] = True
      return result
    wrapperPath = result['Value']

    # Extra Wrapper (Container DIRAC installer)
    wrapSubs = {'next_wrapper': wrapperPath,
                'install_args': self.__getInstallFlags(),
                'config_args': self.__getConfigFlags(),
                }
    wrapLoc = os.path.join(tmpDir, "dirac_container.sh")
    rawfd = os.open(wrapLoc, os.O_WRONLY | os.O_CREAT, 0o700)
    fd = os.fdopen(rawfd, "w")
    fd.write(CONTAINER_WRAPPER % wrapSubs)
    fd.close()

    ret = S_OK()
    ret['baseDir'] = baseDir
    ret['tmpDir'] = tmpDir
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

  # pylint: disable=unused-argument,arguments-differ
  def submitJob(self, executableFile, proxy, jobDesc, log, logLevel, **kwargs):
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
    ret = self.__createWorkArea(proxy, jobDesc, log, logLevel)
    if not ret['OK']:
      return ret
    baseDir = ret['baseDir']
    tmpDir = ret['tmpDir']
    proxyLoc = ret['proxyLocation']

    # Now we have to set-up proxy renewal for the container
    # This is fairly easy as it remains visible on the host filesystem
    ret = getProxyInfo()
    if not ret['OK']:
      pilotProxy = None
    else:
      pilotProxy = ret['Value']['path']
    result = gThreadScheduler.addPeriodicTask(self.proxyCheckPeriod, self._monitorProxy,
                                              taskArgs=(pilotProxy, proxyLoc),
                                              executions=0, elapsedTime=0)
    renewTask = None
    if result['OK']:
      renewTask = result['Value']
    else:
      self.log.warn('Failed to start proxy renewal task')

    # Very simple accounting
    self.__submittedJobs += 1
    self.__runningJobs += 1

    # Now prepare start singularity
    # Mount /cvmfs in if it exists on the host
    withCVMFS = os.path.isdir("/cvmfs")
    innerCmd = os.path.join(self.__innerdir, "dirac_container.sh")
    cmd = [self.__singularityBin, "exec"]
    cmd.extend(["-c", "-i", "-p"])
    cmd.extend(["-W", baseDir])
    if withCVMFS:
      cmd.extend(["-B", "/cvmfs"])
    if 'ContainerBind' in self.ceParameters:
      bindPaths = self.ceParameters['ContainerBind'].split(',')
      for bindPath in bindPaths:
        cmd.extend(["-B", bindPath.strip()])
    if 'ContainerOptions' in self.ceParameters:
      containerOpts = self.ceParameters['ContainerOptions'].split(',')
      for opt in containerOpts:
        cmd.extend([opt.strip()])
    cmd.extend([rootImage, innerCmd])

    self.log.debug('Execute singularity command: %s' % cmd)
    self.log.debug('Execute singularity env: %s' % self.__getEnv())
    result = systemCall(0, cmd, callbackFunction=self.sendOutput, env=self.__getEnv())

    self.__runningJobs -= 1

    if not result["OK"]:
      if renewTask:
        gThreadScheduler.removeTask(renewTask)
      self.__deleteWorkArea(baseDir)
      result = S_ERROR("Error running singularity command")
      result['ReschedulePayload'] = True
      return result

    result = self.__checkResult(tmpDir)
    if renewTask:
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
    return result
