"""
  The Job Sanity executor screens jobs for the following problems:

     - Output data already exists
     - Problematic JDL
     - Jobs with too much input data e.g. > 100 files
     - Jobs with input data incorrectly specified e.g. castor:/
     - Input sandbox not correctly uploaded.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import re

from DIRAC import S_OK, S_ERROR

from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.WorkloadManagementSystem.Client.SandboxStoreClient import SandboxStoreClient
from DIRAC.WorkloadManagementSystem.Executor.Base.OptimizerExecutor import OptimizerExecutor
from DIRAC.WorkloadManagementSystem.Client import JobMinorStatus


class JobSanity(OptimizerExecutor):
  """
      The specific Optimizer must provide the following methods:
        - optimizeJob() - the main method called for each job
      and it can provide:
        - initializeOptimizer() before each execution cycle
  """

  @classmethod
  def initializeOptimizer(cls):
    """Initialize specific parameters for JobSanityAgent.
    """
    cls.sandboxClient = SandboxStoreClient(useCertificates=True, smdb=True)
    return S_OK()

  def optimizeJob(self, jid, jobState):
    """ This method controls the order and presence of
        each sanity check for submitted jobs. This should
        be easily extended in the future to accommodate
        any other potential checks.
    """
    # Job JDL check
    result = jobState.getAttribute('JobType')
    if not result['OK']:
      self.jobLog.error("Failed to get job type attribute", result['Message'])
      return result
    jobType = result['Value'].lower()

    result = jobState.getManifest()
    if not result['OK']:
      self.jobLog.error("Failed to get job manifest", result['Message'])
      return result
    manifest = result['Value']

    # Input data check
    if self.ex_getOption('InputDataCheck', True):
      voName = manifest.getOption("VirtualOrganization", "")
      if not voName:
        return S_ERROR("No VirtualOrganization defined in manifest")
      result = self.checkInputData(jobState, jobType, voName)
      if not result['OK']:
        self.jobLog.error("Failed to check input data", result['Message'])
        return result
      self.jobLog.info("Found LFNs", result['Value'])

    # Input Sandbox uploaded check
    if self.ex_getOption('InputSandboxCheck', True):
      result = self.checkInputSandbox(jobState, manifest)
      if not result['OK']:
        self.jobLog.error("Failed to check input sandbox", result['Message'])
        return result
      self.jobLog.info("Assigned ISBs", result['Value'])

    return self.setNextOptimizer(jobState)

  def checkInputData(self, jobState, jobType, voName):
    """This method checks both the amount of input
       datasets for the job and whether the LFN conventions
       are correct.
    """

    result = jobState.getInputData()
    if not result['OK']:
      self.jobLog.warn('Failed to get input data from JobDB', result['Message'])
      return S_ERROR("Input Data Specification")

    data = result['Value']  # seems to be [''] when null, which isn't an empty list ;)
    data = [lfn.strip() for lfn in data if lfn.strip()]
    if not data:
      return S_OK(0)

    self.jobLog.debug('Input data requirement will be checked')
    self.jobLog.debug('Data is:\n\t%s' % "\n\t".join(data))

    voRE = re.compile("^(LFN:)?/%s/" % voName)

    for lfn in data:
      if not voRE.match(lfn):
        return S_ERROR(JobMinorStatus.INPUT_INCORRECT)
      if lfn.find("//") > -1:
        return S_ERROR(JobMinorStatus.INPUT_CONTAINS_SLASHES)

    # only check limit for user jobs
    if jobType == 'user':
      maxLFNs = self.ex_getOption('MaxInputDataPerJob', 100)
      if len(data) > maxLFNs:
        return S_ERROR("Exceeded Maximum Dataset Limit (%s)" % maxLFNs)
    return S_OK(len(data))

  def checkInputSandbox(self, jobState, manifest):
    """The number of input sandbox files, as specified in the job
       JDL are checked in the JobDB.
    """
    result = jobState.getAttributes(['Owner', 'OwnerDN', 'OwnerGroup', 'DIRACSetup'])
    if not result['OK']:
      self.jobLog.error("Failed to get job attributes", result['Message'])
      return result
    attDict = result['Value']
    ownerName = attDict['Owner']
    if not ownerName:
      ownerDN = attDict['OwnerDN']
      if not ownerDN:
        return S_ERROR("Missing OwnerDN")
      result = Registry.getUsernameForDN(ownerDN)
      if not result['OK']:
        self.jobLog.error("Failed to get user name from DN", result['Message'])
        return result
      ownerName = result['Value']
    ownerGroup = attDict['OwnerGroup']
    if not ownerGroup:
      return S_ERROR("Missing OwnerGroup")
    jobSetup = attDict['DIRACSetup']
    if not jobSetup:
      return S_ERROR("Missing DIRACSetup")

    isbList = manifest.getOption('InputSandbox', [])
    sbsToAssign = []
    for isb in isbList:
      if isb.find("SB:") == 0:
        self.jobLog.info("Found a sandbox", isb)
        sbsToAssign.append((isb, "Input"))
    numSBsToAssign = len(sbsToAssign)
    if not numSBsToAssign:
      return S_OK(0)
    self.jobLog.info("Assigning sandboxes",
                     "(%s on behalf of %s@%s)" % (numSBsToAssign, ownerName, ownerGroup))
    result = self.sandboxClient.assignSandboxesToJob(jobState.jid, sbsToAssign, ownerName, ownerGroup, jobSetup)
    if not result['OK']:
      self.jobLog.error("Could not assign sandboxes in the SandboxStore")
      return S_ERROR("Cannot assign sandbox to job")
    assigned = result['Value']
    if assigned != numSBsToAssign:
      self.jobLog.error("Could not assign all sandboxes",
                        "(%s). Only assigned %s" % (numSBsToAssign, assigned))
    return S_OK(numSBsToAssign)
