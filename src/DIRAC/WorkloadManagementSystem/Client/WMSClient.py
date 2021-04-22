""" DIRAC Workload Management System Client class encapsulates all the
    methods necessary to communicate with the Workload Management System
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
from six import StringIO
import time

from DIRAC import S_OK, S_ERROR, gLogger

from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd
from DIRAC.Core.Utilities import File
from DIRAC.WorkloadManagementSystem.Client.SandboxStoreClient import SandboxStoreClient
from DIRAC.WorkloadManagementSystem.Utilities.ParametricJob import getParameterVectorLength
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Utilities.DErrno import EWMSJDL, EWMSSUBM

__RCSID__ = "$Id$"


class WMSClient(object):
  """ Class exposing the following jobs methods:

      submit
      kill
      delete
      remove
      reschedule
      reset
  """

  def __init__(self, jobManagerClient=None, sbRPCClient=None, sbTransferClient=None,
               useCertificates=False, timeout=600, delegatedDN=None, delegatedGroup=None):
    """ WMS Client constructor

        Here we also initialize the needed clients and connections
    """

    self.useCertificates = useCertificates
    self.delegatedDN = delegatedDN
    self.delegatedGroup = delegatedGroup
    self.timeout = timeout
    self._jobManager = jobManagerClient
    self.operationsHelper = Operations()
    self.sandboxClient = None
    if sbRPCClient and sbTransferClient:
      self.sandboxClient = SandboxStoreClient(rpcClient=sbRPCClient,
                                              transferClient=sbTransferClient,
                                              useCertificates=useCertificates)

  @property
  def jobManager(self):
    if not self._jobManager:
      self._jobManager = RPCClient('WorkloadManagement/JobManager',
                                   useCertificates=self.useCertificates,
                                   delegatedDN=self.delegatedDN,
                                   delegatedGroup=self.delegatedGroup,
                                   timeout=self.timeout)

    return self._jobManager

  def __getInputSandboxEntries(self, classAdJob):
    if classAdJob.lookupAttribute("InputSandbox"):
      inputSandbox = classAdJob.get_expression("InputSandbox")
      inputSandbox = inputSandbox.replace('","', "\n")
      inputSandbox = inputSandbox.replace('{', "")
      inputSandbox = inputSandbox.replace('}', "")
      inputSandbox = inputSandbox.replace('"', "")
      inputSandbox = inputSandbox.replace(',', "")
      inputSandbox = inputSandbox.split()
    else:
      inputSandbox = []

    return inputSandbox

  def __uploadInputSandbox(self, classAdJob, jobDescriptionObject=None):
    """Checks the validity of the job Input Sandbox.
       The function returns the list of Input Sandbox files.
       The total volume of the input sandbox is evaluated
    """
    inputSandbox = self.__getInputSandboxEntries(classAdJob)

    realFiles = []
    badFiles = []
    diskFiles = []

    for isFile in inputSandbox:
      if not isFile.startswith(('lfn:', 'LFN:', 'SB:', '%s', '%(')):
        realFiles.append(isFile)

    stringIOFiles = []
    stringIOFilesSize = 0
    if jobDescriptionObject is not None:
      if isinstance(jobDescriptionObject, StringIO):
        stringIOFiles = [jobDescriptionObject]
        stringIOFilesSize = len(jobDescriptionObject.getvalue())
        gLogger.debug("Size of the stringIOFiles: " + str(stringIOFilesSize))
      else:
        return S_ERROR(EWMSJDL, "jobDescriptionObject is not a StringIO object")

    # Check real files
    for isFile in realFiles:
      if not os.path.exists(isFile):  # we are passing in real files, we expect them to be on disk
        badFiles.append(isFile)
        gLogger.warn("inputSandbox file/directory " + isFile + " not found. Keep looking for the others")
        continue
      diskFiles.append(isFile)

    diskFilesSize = File.getGlobbedTotalSize(diskFiles)
    gLogger.debug("Size of the diskFiles: " + str(diskFilesSize))
    totalSize = diskFilesSize + stringIOFilesSize
    gLogger.verbose("Total size of the inputSandbox: " + str(totalSize))

    okFiles = stringIOFiles + diskFiles
    if badFiles:
      result = S_ERROR(EWMSJDL, 'Input Sandbox is not valid')
      result['BadFile'] = badFiles
      result['TotalSize'] = totalSize
      return result

    if okFiles:
      if not self.sandboxClient:
        self.sandboxClient = SandboxStoreClient(useCertificates=self.useCertificates,
                                                delegatedDN=self.delegatedDN,
                                                delegatedGroup=self.delegatedGroup)
      result = self.sandboxClient.uploadFilesAsSandbox(okFiles)
      if not result['OK']:
        return result
      inputSandbox.append(result['Value'])
      classAdJob.insertAttributeVectorString("InputSandbox", inputSandbox)

    return S_OK()

  def submitJob(self, jdl, jobDescriptionObject=None):
    """ Submit one job specified by its JDL to WMS.

        The JDL may actually be the desciption of a parametric job,
        resulting in multiple DIRAC jobs submitted to the DIRAC WMS
    """

    if os.path.exists(jdl):
      with open(jdl, "r") as fic:
        jdlString = fic.read()
    else:
      # If file JDL does not exist, assume that the JDL is passed as a string
      jdlString = jdl

    jdlString = jdlString.strip()

    gLogger.debug("Submitting JDL", jdlString)
    # Strip of comments in the jdl string
    newJdlList = []
    for line in jdlString.split('\n'):
      if not line.strip().startswith('#'):
        newJdlList.append(line)
    jdlString = '\n'.join(newJdlList)

    # Check the validity of the input JDL
    if jdlString.find("[") != 0:
      jdlString = "[%s]" % jdlString
    classAdJob = ClassAd(jdlString)
    if not classAdJob.isOK():
      return S_ERROR(EWMSJDL, 'Invalid job JDL')

    # Check the size and the contents of the input sandbox
    result = self.__uploadInputSandbox(classAdJob, jobDescriptionObject)
    if not result['OK']:
      return result

    # Submit the job now and get the new job ID
    result = getParameterVectorLength(classAdJob)
    if not result['OK']:
      return result
    nJobs = result['Value']
    result = self.jobManager.submitJob(classAdJob.asJDL())

    if nJobs:
      gLogger.debug('Applying transactional job submission')
      # The server applies transactional bulk submission, we should confirm the jobs
      if result['OK']:
        jobIDList = result['Value']
        if len(jobIDList) == nJobs:
          # Confirm the submitted jobs
          confirmed = False
          for _attempt in range(3):
            result = self.jobManager.confirmBulkSubmission(jobIDList)
            if result['OK']:
              confirmed = True
              break
            time.sleep(1)
          if not confirmed:
            # The bulk submission failed, try to delete the created jobs
            resultDelete = self.jobManager.deleteJob(jobIDList)
            error = "Job submission failed to confirm bulk transaction"
            if not resultDelete['OK']:
              error += "; removal of created jobs failed"
            return S_ERROR(EWMSSUBM, error)
        else:
          return S_ERROR(EWMSSUBM, "The number of submitted jobs does not match job description")

    if result.get('requireProxyUpload'):
      gLogger.warn("Need to upload the proxy")

    return result

  def killJob(self, jobID):
    """ Kill running job.
        jobID can be an integer representing a single DIRAC job ID or a list of IDs
    """
    return self.jobManager.killJob(jobID)

  def deleteJob(self, jobID):
    """ Delete job(s) (set their status to DELETED) from the WMS Job database.
        jobID can be an integer representing a single DIRAC job ID or a list of IDs
    """
    return self.jobManager.deleteJob(jobID)

  def removeJob(self, jobID):
    """ Fully remove job(s) from the WMS Job database.
        jobID can be an integer representing a single DIRAC job ID or a list of IDs
    """
    return self.jobManager.removeJob(jobID)

  def rescheduleJob(self, jobID):
    """ Reschedule job(s) in WMS Job database.
        jobID can be an integer representing a single DIRAC job ID or a list of IDs
    """
    return self.jobManager.rescheduleJob(jobID)

  def resetJob(self, jobID):
    """ Reset job(s) in WMS Job database.
        jobID can be an integer representing a single DIRAC job ID or a list of IDs
    """
    return self.jobManager.resetJob(jobID)
