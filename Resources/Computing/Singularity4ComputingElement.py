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

__RCSID__ = "$Id$"

import os

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.Core.Utilities.ThreadScheduler import gThreadScheduler
from DIRAC.Resources.Computing.ComputingElement import ComputingElement


CONTAINER_DEFROOT = "/cvmfs/cernvm-prod.cern.ch/cvm4"
CONTAINER_INNERDIR = "/tmp"


class Singularity4ComputingElement(ComputingElement):
  """ A Computing Element for running a job within a Singularity container.
  """

  def __init__(self, ceUniqueID):
    """ Standard constructor.
    """
    super(Singularity4ComputingElement, self).__init__(ceUniqueID)
    self.__submittedJobs = 0
    self.__runningJobs = 0
    self.__root = CONTAINER_DEFROOT
    if 'ContainerRoot' in self.ceParameters:
      self.__root = self.ceParameters['ContainerRoot']
    # self.__workdir = CONTAINER_WORKDIR
    # self.__innerdir = CONTAINER_INNERDIR
    self.__singularityBin = 'singularity'

    self.log.debug("CE parameters", self.ceParameters)

  def submitJob(self, executableFile, proxy=None, **kwargs):
    """ Method to submit job (overriding base method).

    :param str executableFile: file to execute via systemCall.
                               Normally the JobWrapperTemplate when invoked by the JobAgent.
    :param str proxy: the proxy used for running the job (the payload). It will be dumped to a file.
    """

    # Check that singularity is available
    if not self.__hasSingularity():
      self.log.error('Singularity is not installed on PATH.')
      result = S_ERROR("Failed to find singularity ")
      result['ReschedulePayload'] = True
      return result

    self.log.info('Creating singularity container')

    # Now we have to set-up pilot proxy renewal for the container
    # This is fairly easy as it remains visible on the host filesystem
    ret = getProxyInfo()
    if not ret['OK']:
      pilotProxy = None
    else:
      pilotProxy = ret['Value']['path']

    self.log.notice('Pilot Proxy:', pilotProxy)

    payloadEnv = dict(os.environ)
    payloadProxyLoc = ''
    if proxy:
      self.log.verbose('Setting up proxy for payload')
      result = self.writeProxyToFile(proxy)
      if not result['OK']:
        return result

      payloadProxyLoc = result['Value']  # proxy file location
      payloadEnv['X509_USER_PROXY'] = payloadProxyLoc

    result = gThreadScheduler.addPeriodicTask(self.proxyCheckPeriod, self._monitorProxy,
                                              taskArgs=(pilotProxy, payloadProxyLoc),
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
    innerCmd = os.path.join(self.__innerdir, "dirac_container.sh")
    cmd = [self.__singularityBin, "exec"]
    cmd.extend(["-c", "-i", "-p"])
    cmd.extend(["-W", baseDir])

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
    # No suitable binaries found
    return False
