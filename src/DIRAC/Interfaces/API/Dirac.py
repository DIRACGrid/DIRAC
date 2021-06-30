"""
   DIRAC API Class

   All DIRAC functionality is exposed through the DIRAC API and this
   serves as a source of documentation for the project via EpyDoc.

   The DIRAC API provides the following functionality:
    - A transparent and secure way for users
      to submit jobs to the Grid, monitor them and
      retrieve outputs
    - Interaction with Grid storage and file catalogues
      via the DataManagement public interfaces (more to be added)
    - Local execution of workflows for testing purposes.

"""

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

__RCSID__ = "$Id$"

import glob
import io
import os
import re
import shlex
import shutil
import sys
import tarfile
import tempfile
import time
import six
from six import StringIO
from six.moves.urllib.parse import unquote as urlunquote


import DIRAC
from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.API import API
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities import Time
from DIRAC.Core.Utilities.File import mkDir
from DIRAC.Core.Utilities.List import breakListIntoChunks
from DIRAC.Core.Utilities.SiteSEMapping import getSEsForSite
from DIRAC.Core.Utilities.PrettyPrint import printTable, printDict
from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd
from DIRAC.Core.Utilities.Subprocess import systemCall
from DIRAC.Core.Utilities.ModuleFactory import ModuleFactory
from DIRAC.Core.Utilities.Decorators import deprecated
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.ConfigurationSystem.Client.PathFinder import getSystemSection, getServiceURL
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOForGroup
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Interfaces.API.JobRepository import JobRepository
from DIRAC.DataManagementSystem.Client.DataManager import DataManager
from DIRAC.Resources.Storage.StorageElement import StorageElement
from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
from DIRAC.WorkloadManagementSystem.Client.WMSClient import WMSClient
from DIRAC.WorkloadManagementSystem.Client.SandboxStoreClient import SandboxStoreClient
from DIRAC.WorkloadManagementSystem.Client.JobMonitoringClient import JobMonitoringClient

COMPONENT_NAME = 'DiracAPI'

try:
  # Python 2: "file" is built-in
  file_types = file, io.IOBase
except NameError:
  # Python 3: "file" fully replaced with IOBase
  file_types = (io.IOBase,)


def parseArguments(args):
  argList = []
  for arg in args:
    argList += [a.strip() for a in arg.split(',') if a.strip()]
  return argList


class Dirac(API):
  """
   DIRAC API Class
  """

  #############################################################################
  def __init__(self, withRepo=False, repoLocation='', useCertificates=False, vo=None):
    """Internal initialization of the DIRAC API.
    """
    super(Dirac, self).__init__()

    self.section = '/LocalSite/'

    self.jobRepo = False
    if withRepo:
      self.jobRepo = JobRepository(repoLocation)
      if not self.jobRepo.isOK():
        gLogger.error("Unable to write to supplied repository location")
        self.jobRepo = False

    self.useCertificates = useCertificates

    # Determine the default file catalog
    self.defaultFileCatalog = gConfig.getValue(self.section + '/FileCatalog', None)
    self.vo = vo

  def _checkFileArgument(self, fnList, prefix=None, single=False):
    if prefix is None:
      prefix = 'LFN'
    if isinstance(fnList, six.string_types):
      otherPrefix = 'LFN:' if prefix == 'PFN' else 'PFN:'
      if otherPrefix in fnList:
        return self._errorReport('Expected %s string, not %s') % (prefix, otherPrefix)
      return S_OK(fnList.replace('%s:' % prefix, ''))
    elif isinstance(fnList, list):
      if single:
        return self._errorReport('Expected single %s string' % prefix)
      try:
        return S_OK([fn.replace('%s:' % prefix, '') for fn in fnList])
      except Exception as x:
        return self._errorReport(str(x), 'Expected strings in list of %ss' % prefix)
    else:
      return self._errorReport('Expected single string or list of strings for %s(s)' % prefix)

  def _checkJobArgument(self, jobID, multiple=False):
    try:
      if isinstance(jobID, (str, six.integer_types)):
        jobID = int(jobID)
        if multiple:
          jobID = [jobID]
      elif isinstance(jobID, (list, dict)):
        if multiple:
          jobID = [int(job) for job in jobID]
        else:
          return self._errorReport('Expected int or string, not list')
      return S_OK(jobID)
    except Exception as x:
      return self._errorReport(str(x), 'Expected %s integer or string for existing jobID' %
                               '(list of) ' if multiple else '')

  #############################################################################
  # Repository specific methods
  #############################################################################
  def getRepositoryJobs(self, printOutput=False):
    """ Retrieve all the jobs in the repository

       Example Usage:

       >>> print dirac.getRepositoryJobs()
       {'OK': True, 'Value': [1,2,3,4]}

       :return: S_OK,S_ERROR
    """
    if not self.jobRepo:
      gLogger.warn("No repository is initialised")
      return S_OK()
    jobIDs = list(self.jobRepo.readRepository()['Value'])
    if printOutput:
      print(self.pPrint.pformat(jobIDs))
    return S_OK(jobIDs)

  def monitorRepository(self, printOutput=False):
    """Monitor the jobs present in the repository

       Example Usage:

       >>> print dirac.monitorRepository()
       {'OK': True, 'Value': ''}

       :returns: S_OK,S_ERROR
    """
    if not self.jobRepo:
      gLogger.warn("No repository is initialised")
      return S_OK()
    jobs = self.jobRepo.readRepository()['Value']
    jobIDs = list(jobs)
    res = self.getJobStatus(jobIDs)
    if not res['OK']:
      return self._errorReport(res['Message'], 'Failed to get status of jobs from WMS')

    statusDict = {}
    for jobDict in jobs.values():
      state = jobDict.get('State', 'Unknown')
      statusDict[state] = statusDict.setdefault(state, 0) + 1
    if printOutput:
      print(self.pPrint.pformat(statusDict))
    return S_OK(statusDict)

  def retrieveRepositorySandboxes(self, requestedStates=None, destinationDirectory=''):
    """ Obtain the output sandbox for the jobs in requested states in the repository

       Example Usage:

       >>> print dirac.retrieveRepositorySandboxes(requestedStates=['Done','Failed'],destinationDirectory='sandboxes')
       {'OK': True, 'Value': ''}

       :param requestedStates: List of jobs states to be considered
       :type requestedStates: list of strings
       :param destinationDirectory: The target directory
                                    to place sandboxes (each jobID will have a directory created beneath this)
       :type destinationDirectory: string
       :returns: S_OK,S_ERROR
    """
    if not self.jobRepo:
      gLogger.warn("No repository is initialised")
      return S_OK()
    if requestedStates is None:
      requestedStates = ['Done', 'Failed', 'Completed']  # because users dont care about completed
    jobs = self.jobRepo.readRepository()['Value']
    for jobID in sorted(jobs):
      jobDict = jobs[jobID]
      if jobDict.get('State') in requestedStates:
        # # Value of 'Retrieved' is a string, e.g. '0' when read from file
        if not int(jobDict.get('Retrieved')):
          res = self.getOutputSandbox(jobID, destinationDirectory)
          if not res['OK']:
            return res
    return S_OK()

  def retrieveRepositoryData(self, requestedStates=None, destinationDirectory=''):
    """ Obtain the output data for the jobs in requested states in the repository

       Example Usage:

       >>> print dirac.retrieveRepositoryData(requestedStates=['Done'],destinationDirectory='outputData')
       {'OK': True, 'Value': ''}

       :param requestedStates: List of jobs states to be considered
       :type requestedStates: list of strings
       :param destinationDirectory: The target directory to place sandboxes (a directory is created for each JobID)
       :type destinationDirectory: string
       :returns: S_OK,S_ERROR
    """
    if not self.jobRepo:
      gLogger.warn("No repository is initialised")
      return S_OK()
    if requestedStates is None:
      requestedStates = ['Done']
    jobs = self.jobRepo.readRepository()['Value']
    for jobID in sorted(jobs):
      jobDict = jobs[jobID]
      if jobDict.get('State') in requestedStates:
        # # Value of 'OutputData' is a string, e.g. '0' when read from file
        if not int(jobDict.get('OutputData')):
          destDir = jobID
          if destinationDirectory:
            destDir = "%s/%s" % (destinationDirectory, jobID)
          self.getJobOutputData(jobID, destinationDir=destDir)
    return S_OK()

  def removeRepository(self):
    """ Removes the job repository and all sandboxes and output data retrieved

       Example Usage:

       >>> print dirac.removeRepository()
       {'OK': True, 'Value': ''}

       :returns: S_OK,S_ERROR
    """
    if not self.jobRepo:
      gLogger.warn("No repository is initialised")
      return S_OK()
    jobs = self.jobRepo.readRepository()['Value']
    for jobID in sorted(jobs):
      jobDict = jobs[jobID]
      if os.path.exists(jobDict.get('Sandbox', '')):
        shutil.rmtree(jobDict['Sandbox'], ignore_errors=True)
      if 'OutputFiles' in jobDict:
        for fileName in eval(jobDict['OutputFiles']):
          if os.path.exists(fileName):
            os.remove(fileName)
    self.deleteJob(sorted(jobs))
    os.remove(self.jobRepo.getLocation()['Value'])
    self.jobRepo = False
    return S_OK()

  def resetRepository(self, jobIDs=None):
    """ Reset all the status of the (optionally supplied) jobs in the repository

       Example Usage:

       >>> print dirac.resetRepository(jobIDs = [1111,2222,'3333'])
       {'OK': True, 'Value': ''}

       :returns: S_OK,S_ERROR
    """
    if not self.jobRepo:
      gLogger.warn("No repository is initialised")
      return S_OK()
    if jobIDs is None:
      jobIDs = []
    if not isinstance(jobIDs, list):
      return self._errorReport('The jobIDs must be a list of (strings or ints).')
    self.jobRepo.resetRepository(jobIDs=jobIDs)
    return S_OK()

  #############################################################################

  def submitJob(self, job, mode='wms'):
    """Submit jobs to DIRAC (by default to the Workload Management System).
       These can be either:

        - Instances of the Job Class
           - VO Application Jobs
           - Inline scripts
           - Scripts as executables
           - Scripts inside an application environment

        - JDL File
        - JDL String

       Example usage:

       >>> print dirac.submitJob(job)
       {'OK': True, 'Value': '12345'}

       :param job: Instance of Job class or JDL string
       :type job: ~DIRAC.Interfaces.API.Job.Job or str
       :param mode: Submit job to WMS with mode = 'wms' (default),
                    'local' to run the workflow locally
       :type mode: str
       :returns: S_OK,S_ERROR
    """
    self.__printInfo()

    if isinstance(job, six.string_types):
      if os.path.exists(job):
        self.log.verbose('Found job JDL file %s' % (job))
        with open(job, 'r') as fd:
          jdlAsString = fd.read()
      else:
        self.log.verbose('Job is a JDL string')
        jdlAsString = job
      jobDescriptionObject = None
    else:  # we assume it is of type "DIRAC.Interfaces.API.Job.Job"
      try:
        formulationErrors = job.errorDict
      except AttributeError as x:
        self.log.verbose('Could not obtain job errors:%s' % (x))
        formulationErrors = {}

      if formulationErrors:
        for method, errorList in formulationErrors.items():  # can be an iterator
          self.log.error('>>>> Error in %s() <<<<\n%s' % (method, '\n'.join(errorList)))
        return S_ERROR(formulationErrors)

      # Run any VO specific checks if desired prior to submission, this may or may not be overridden
      # in a derived class for example
      try:
        result = self.preSubmissionChecks(job, mode)
        if not result['OK']:
          self.log.error('Pre-submission checks failed for job with message: "%s"' % (result['Message']))
          return result
      except BaseException as x:
        msg = 'Error in VO specific function preSubmissionChecks: "%s"' % (x)
        self.log.error(msg)
        return S_ERROR(msg)

      jobDescriptionObject = StringIO(job._toXML())  # pylint: disable=protected-access
      jdlAsString = job._toJDL(jobDescriptionObject=jobDescriptionObject)  # pylint: disable=protected-access

    if mode.lower() == 'local':
      result = self.runLocal(job)

    elif mode.lower() == 'wms':
      self.log.verbose('Will submit job to WMS')  # this will happen by default anyway
      result = WMSClient(useCertificates=self.useCertificates).submitJob(jdlAsString, jobDescriptionObject)
      if not result['OK']:
        self.log.error('Job submission failure', result['Message'])
      elif self.jobRepo:
        jobIDList = result['Value']
        if not isinstance(jobIDList, list):
          jobIDList = [jobIDList]
        for jobID in jobIDList:
          result = self.jobRepo.addJob(jobID, 'Submitted')

    return result

  #############################################################################
  def __cleanTmp(self, cleanPath):
    """Remove tmp file or directory
    """
    if not cleanPath:
      return
    if os.path.isfile(cleanPath):
      os.unlink(cleanPath)
      return
    if os.path.isdir(cleanPath):
      shutil.rmtree(cleanPath, ignore_errors=True)
      return
    self.__printOutput(sys.stdout, 'Could not remove %s' % str(cleanPath))
    return

  #############################################################################
  def preSubmissionChecks(self, job, mode):
    """Internal function.  The pre-submission checks method allows VOs to
       make their own checks before job submission. To make use of this the
       method should be overridden in a derived VO-specific Dirac class.
    """
    return S_OK('Nothing to do')

  #############################################################################
  @staticmethod
  @deprecated("It will be removed after v7r2. Use Operations sections only!")
  def __getVOPolicyModule(module):
    """ Utility to get the VO Policy module name
    """

    moduleName = ''
    setup = gConfig.getValue('/DIRAC/Setup', '')
    vo = None
    ret = getProxyInfo(disableVOMS=True)
    if ret['OK'] and 'group' in ret['Value']:
      vo = getVOForGroup(ret['Value']['group'])
    if setup and vo:
      moduleName = gConfig.getValue('DIRAC/VOPolicy/%s/%s/%s' % (vo, setup, module), '')
      if not moduleName:
        moduleName = gConfig.getValue('DIRAC/VOPolicy/%s' % module, '')

    return moduleName

  #############################################################################
  def getInputDataCatalog(self, lfns, siteName='', fileName='pool_xml_catalog.xml', ignoreMissing=False):
    """This utility will create a pool xml catalogue slice for the specified LFNs using
       the full input data resolution policy plugins for the VO.

       If not specified the site is assumed to be the DIRAC.siteName() from the local
       configuration.  The fileName can be a full path.

       Example usage:

       >>> print print d.getInputDataCatalog('/lhcb/a/b/c/00001680_00000490_5.dst',None,'myCat.xml')
       {'Successful': {'<LFN>': {'pfntype': 'ROOT_All', 'protocol': 'SRM2',
        'pfn': '<PFN>', 'turl': '<TURL>', 'guid': '3E3E097D-0AC0-DB11-9C0A-00188B770645',
        'se': 'CERN-disk'}}, 'Failed': [], 'OK': True, 'Value': ''}

       :param lfns: Logical File Name(s) to query
       :type lfns: LFN str or python:list []
       :param siteName: DIRAC site name
       :type siteName: string
       :param fileName: Catalogue name (can include path)
       :type fileName: string
       :returns: S_OK,S_ERROR

    """
    ret = self._checkFileArgument(lfns, 'LFN')
    if not ret['OK']:
      return ret
    lfns = ret['Value']

    if not siteName:
      siteName = DIRAC.siteName()

    if ignoreMissing:
      self.log.verbose('Ignore missing flag is enabled')

    localSEList = getSEsForSite(siteName)
    if not localSEList['OK']:
      return localSEList

    self.log.verbose(localSEList)

    inputDataPolicy = Operations().getValue('InputDataPolicy/InputDataModule')
    if not inputDataPolicy:
      return self._errorReport('Could not retrieve /DIRAC/Operations/InputDataPolicy/InputDataModule for VO')

    self.log.info('Attempting to resolve data for %s' % siteName)
    self.log.verbose('%s' % ('\n'.join(lfns)))
    replicaDict = self.getReplicasForJobs(lfns)
    if not replicaDict['OK']:
      return replicaDict
    catalogFailed = replicaDict['Value'].get('Failed', {})

    guidDict = self.getLfnMetadata(lfns)
    if not guidDict['OK']:
      return guidDict
    for lfn, reps in replicaDict['Value']['Successful'].items():  # can be an iterator
      guidDict['Value']['Successful'][lfn].update(reps)
    resolvedData = guidDict
    diskSE = gConfig.getValue(self.section + '/DiskSE', ['-disk', '-DST', '-USER', '-FREEZER'])
    tapeSE = gConfig.getValue(self.section + '/TapeSE', ['-tape', '-RDST', '-RAW'])
    # Add catalog path / name here as well as site name to override the standard policy of resolving automatically
    configDict = {'JobID': None,
                  'LocalSEList': localSEList['Value'],
                  'DiskSEList': diskSE,
                  'TapeSEList': tapeSE,
                  'SiteName': siteName,
                  'CatalogName': fileName}

    self.log.verbose(configDict)
    argumentsDict = {'FileCatalog': resolvedData, 'Configuration': configDict, 'InputData': lfns}
    if ignoreMissing:
      argumentsDict['IgnoreMissing'] = True
    self.log.verbose(argumentsDict)
    moduleFactory = ModuleFactory()
    self.log.verbose('Input Data Policy Module: %s' % inputDataPolicy)
    moduleInstance = moduleFactory.getModule(inputDataPolicy, argumentsDict)
    if not moduleInstance['OK']:
      self.log.warn('Could not create InputDataModule')
      return moduleInstance

    module = moduleInstance['Value']
    result = module.execute()
    self.log.debug(result)
    if not result['OK']:
      if 'Failed' in result:
        self.log.error('Input data resolution failed for the following files:\n', '\n'.join(result['Failed']))

    if catalogFailed:
      self.log.error('Replicas not found for the following files:')
      for key, value in catalogFailed.items():  # can be an iterator
        self.log.error('%s %s' % (key, value))
      if 'Failed' in result:
        result['Failed'] = list(catalogFailed)

    return result

  #############################################################################

  def runLocal(self, job):
    """ Internal function.  This method is called by DIRAC API function submitJob(job,mode='Local').
        All output files are written to the local directory.

        This is a method for running local tests. It skips the creation of a JobWrapper,
        but preparing an environment that mimics it.

    :param job: a job object
    :type job: ~DIRAC.Interfaces.API.Job.Job
    """
    self.log.notice('Executing workflow locally')
    curDir = os.getcwd()
    self.log.info('Executing from %s' % curDir)

    jobDir = tempfile.mkdtemp(suffix='_JobDir', prefix='Local_', dir=curDir)
    os.chdir(jobDir)
    self.log.info('Executing job at temp directory %s' % jobDir)

    tmpdir = tempfile.mkdtemp(prefix='DIRAC_')
    self.log.verbose('Created temporary directory for submission %s' % (tmpdir))
    jobXMLFile = tmpdir + '/jobDescription.xml'
    self.log.verbose('Job XML file description is: %s' % jobXMLFile)
    with open(jobXMLFile, 'w+') as fd:
      fd.write(job._toXML())  # pylint: disable=protected-access

    shutil.copy(jobXMLFile, '%s/%s' % (os.getcwd(), os.path.basename(jobXMLFile)))

    res = self.__getJDLParameters(job)
    if not res['OK']:
      self.log.error("Could not extract job parameters from job")
      return res
    parameters = res['Value']
    self.log.debug("Extracted job parameters from JDL", parameters)

    arguments = parameters.get('Arguments', '')

    # Replace argument placeholders for parametric jobs
    # if we have Parameters then we have a parametric job
    if 'Parameters' in parameters:
      for par, value in parameters.items():  # can be an iterator
        if par.startswith('Parameters.'):
          # we just use the first entry in all lists to run one job
          parameters[par[len('Parameters.'):]] = value[0]
      arguments = arguments % parameters

    self.log.verbose("Job parameters: %s" % printDict(parameters))
    inputDataRes = self._getLocalInputData(parameters)
    if not inputDataRes['OK']:
      return inputDataRes
    inputData = inputDataRes['Value']

    if inputData:
      self.log.verbose("Job has input data: %s" % inputData)
      localSEList = gConfig.getValue('/LocalSite/LocalSE', '')
      if not localSEList:
        return self._errorReport('LocalSite/LocalSE should be defined in your config file')
      localSEList = localSEList.replace(' ', '').split(',')
      self.log.debug("List of local SEs: %s" % localSEList)
      inputDataPolicy = Operations().getValue('InputDataPolicy/InputDataModule')
      if not inputDataPolicy:
        return self._errorReport('Could not retrieve DIRAC/Operations/InputDataPolicy/InputDataModule for VO')

      self.log.info('Job has input data requirement, will attempt to resolve data for %s' % DIRAC.siteName())
      self.log.verbose('\n'.join(inputData if isinstance(inputData, (list, tuple)) else [inputData]))
      replicaDict = self.getReplicasForJobs(inputData)
      if not replicaDict['OK']:
        return replicaDict
      guidDict = self.getLfnMetadata(inputData)
      if not guidDict['OK']:
        return guidDict
      for lfn, reps in replicaDict['Value']['Successful'].items():  # can be an iterator
        guidDict['Value']['Successful'][lfn].update(reps)
      resolvedData = guidDict
      diskSE = gConfig.getValue(self.section + '/DiskSE', ['-disk', '-DST', '-USER', '-FREEZER'])
      tapeSE = gConfig.getValue(self.section + '/TapeSE', ['-tape', '-RDST', '-RAW'])
      configDict = {'JobID': None,
                    'LocalSEList': localSEList,
                    'DiskSEList': diskSE,
                    'TapeSEList': tapeSE}
      self.log.verbose(configDict)
      argumentsDict = {'FileCatalog': resolvedData,
                       'Configuration': configDict,
                       'InputData': inputData,
                       'Job': parameters}
      self.log.verbose(argumentsDict)
      moduleFactory = ModuleFactory()
      moduleInstance = moduleFactory.getModule(inputDataPolicy, argumentsDict)
      if not moduleInstance['OK']:
        self.log.warn('Could not create InputDataModule')
        return moduleInstance

      module = moduleInstance['Value']
      result = module.execute()
      if not result['OK']:
        self.log.warn('Input data resolution failed')
        return result

    softwarePolicy = Operations().getValue('SoftwareDistModule')
    if softwarePolicy:
      moduleFactory = ModuleFactory()
      moduleInstance = moduleFactory.getModule(softwarePolicy, {'Job': parameters})
      if not moduleInstance['OK']:
        self.log.warn('Could not create SoftwareDistModule')
        return moduleInstance

      module = moduleInstance['Value']
      result = module.execute()
      if not result['OK']:
        self.log.warn('Software installation failed with result:\n%s' % (result))
        return result
    else:
      self.log.verbose('Could not retrieve SoftwareDistModule for VO')

    self.log.debug("Looking for resolving the input sandbox, if it is present")
    sandbox = parameters.get('InputSandbox')
    if sandbox:
      self.log.verbose("Input Sandbox is %s" % sandbox)
      if isinstance(sandbox, six.string_types):
        sandbox = [isFile.strip() for isFile in sandbox.split(',')]
      for isFile in sandbox:
        self.log.debug("Resolving Input Sandbox %s" % isFile)
        if isFile.lower().startswith("lfn:"):  # isFile is an LFN
          isFile = isFile[4:]
        # Attempt to copy into job working directory, unless it is already there
        if os.path.exists(os.path.join(os.getcwd(), os.path.basename(isFile))):
          self.log.debug("Input Sandbox %s found in the job directory, no need to copy it" % isFile)
        else:
          if os.path.isabs(isFile) and os.path.exists(isFile):
            self.log.debug("Input Sandbox %s is a file with absolute path, copying it" % isFile)
            shutil.copy(isFile, os.getcwd())
          elif os.path.isdir(isFile):
            self.log.debug("Input Sandbox %s is a directory, found in the user working directory, copying it" % isFile)
            shutil.copytree(isFile, os.path.basename(isFile), symlinks=True)
          elif os.path.exists(os.path.join(curDir, os.path.basename(isFile))):
            self.log.debug("Input Sandbox %s found in the submission directory, copying it" % isFile)
            shutil.copy(os.path.join(curDir, os.path.basename(isFile)), os.getcwd())
          elif os.path.exists(os.path.join(tmpdir, isFile)):  # if it is in the tmp dir
            self.log.debug("Input Sandbox %s is a file, found in the tmp directory, copying it" % isFile)
            shutil.copy(os.path.join(tmpdir, isFile), os.getcwd())
          else:
            self.log.verbose("perhaps the file %s is in an LFN, so we attempt to download it." % isFile)
            getFile = self.getFile(isFile)
            if not getFile['OK']:
              self.log.warn('Failed to download %s with error: %s' % (isFile, getFile['Message']))
              return S_ERROR('Can not copy InputSandbox file %s' % isFile)

        isFileInCWD = os.getcwd() + os.path.sep + isFile

        basefname = os.path.basename(isFileInCWD)
        if tarfile.is_tarfile(basefname):
          try:
            with tarfile.open(basefname, 'r') as tf:
              for member in tf.getmembers():
                tf.extract(member, os.getcwd())
          except (tarfile.ReadError, tarfile.CompressionError, tarfile.ExtractError) as x:
            return S_ERROR('Could not untar or extract %s with exception %s' % (basefname, repr(x)))

    self.log.info('Attempting to submit job to local site: %s' % DIRAC.siteName())

    # DIRACROOT is used for finding dirac-jobexec in python2 installations
    # (it is normally set by the JobWrapper)
    # We don't use DIRAC.rootPath as we assume that a DIRAC installation is already done at this point
    # DIRAC env variable is only set for python2 installations
    if 'DIRAC' in os.environ:
      os.environ['DIRACROOT'] = os.environ['DIRAC']
      self.log.verbose('DIRACROOT = %s' % (os.environ['DIRACROOT']))

    if 'Executable' in parameters:
      executable = os.path.expandvars(parameters['Executable'])
    else:
      return self._errorReport('Missing job "Executable"')

    if '-o LogLevel' in arguments:
      dArguments = arguments.split()
      logLev = dArguments.index('-o') + 1
      dArguments[logLev] = 'LogLevel=DEBUG'
      arguments = ' '.join(dArguments)
    else:
      arguments += ' -o LogLevel=DEBUG'
    command = '%s %s' % (executable, arguments)

    self.log.info('Executing: %s' % command)
    executionEnv = dict(os.environ)
    variableList = parameters.get('ExecutionEnvironment')
    if variableList:
      self.log.verbose('Adding variables to execution environment')
      if isinstance(variableList, six.string_types):
        variableList = [variableList]
      for var in variableList:
        nameEnv = var.split('=')[0]
        valEnv = urlunquote(var.split('=')[1])  # this is needed to make the value contain strange things
        executionEnv[nameEnv] = valEnv
        self.log.verbose('%s = %s' % (nameEnv, valEnv))

    result = systemCall(0, cmdSeq=shlex.split(command), env=executionEnv, callbackFunction=self.__printOutput)
    if not result['OK']:
      return result

    status = result['Value'][0]
    self.log.verbose('Status after execution is %s' % (status))

    # FIXME: if there is an callbackFunction, StdOutput and StdError will be empty soon
    outputFileName = parameters.get('StdOutput')
    errorFileName = parameters.get('StdError')

    if outputFileName:
      stdout = result['Value'][1]
      if os.path.exists(outputFileName):
        os.remove(outputFileName)
      self.log.info('Standard output written to %s' % (outputFileName))
      with open(outputFileName, 'w') as outputFile:
        print(stdout, file=outputFile)
    else:
      self.log.warn('Job JDL has no StdOutput file parameter defined')

    if errorFileName:
      stderr = result['Value'][2]
      if os.path.exists(errorFileName):
        os.remove(errorFileName)
      self.log.verbose('Standard error written to %s' % (errorFileName))
      with open(errorFileName, 'w') as errorFile:
        print(stderr, file=errorFile)
      sandbox = None
    else:
      self.log.warn('Job JDL has no StdError file parameter defined')
      sandbox = parameters.get('OutputSandbox')

    if sandbox:
      if isinstance(sandbox, six.string_types):
        sandbox = [osFile.strip() for osFile in sandbox.split(',')]
      for i in sandbox:
        globList = glob.glob(i)
        for osFile in globList:
          if os.path.isabs(osFile):
            # if a relative path, it is relative to the user working directory
            osFile = os.path.basename(osFile)
          # Attempt to copy back from job working directory
          if os.path.isdir(osFile):
            shutil.copytree(osFile, curDir, symlinks=True)
          elif os.path.exists(osFile):
            shutil.copy(osFile, curDir)
          else:
            return S_ERROR('Can not copy OutputSandbox file %s' % osFile)

    os.chdir(curDir)

    if status:  # if it fails, copy content of execution dir in current directory
      destDir = os.path.join(curDir, os.path.basename(os.path.dirname(tmpdir)))
      self.log.verbose("Copying outputs from %s to %s" % (tmpdir, destDir))
      if os.path.exists(destDir):
        shutil.rmtree(destDir)
      shutil.copytree(tmpdir, destDir)

    self.log.verbose('Cleaning up %s...' % tmpdir)
    self.__cleanTmp(tmpdir)

    if status:
      return S_ERROR('Execution completed with non-zero status %s' % (status))

    return S_OK('Execution completed successfully')

  @staticmethod
  def _getLocalInputData(parameters):
    """ Resolve input data for locally run jobs.
        Here for reason of extensibility
    """
    inputData = parameters.get('InputData')
    if inputData:
      if isinstance(inputData, six.string_types):
        inputData = [inputData]
    return S_OK(inputData)

  #############################################################################
  @staticmethod
  def __printOutput(fd=None, message=''):
    """Internal callback function to return standard output when running locally.
    """
    if fd:
      if isinstance(fd, six.integer_types):
        if fd == 0:
          print(message, file=sys.stdout)
        elif fd == 1:
          print(message, file=sys.stderr)
        else:
          print(message)
      elif isinstance(fd, file_types):
        print(message, file=fd)
    else:
      print(message)

  #############################################################################
  def listCatalogDirectory(self, directoryLFN, printOutput=False):
    """lists the contents of a directory in the DFC

       Example usage:

       >>> res = dirac.listCatalogDir("/lz/data/test", printOutput=True)
       Listing content of: /lz/data/test
       Subdirectories:
       /lz/data/test/reconstructed
       /lz/data/test/BACCARAT_release-2.1.1_geant4.9.5.p02
       /lz/data/test/BACCARAT_release-2.1.0_geant4.9.5.p02
       Files:
       /lz/data/test/sites.log
       /lz/data/test/sites2.log

       >>> print(res)
       {'OK': True, 'Value': {'Successful': {'/lz/data/test': {'Files': {'/lz/data/test/sites.log':
       {'MetaData': {'Status': 'AprioriGood', 'GUID': 'AD81AD07-3BC0-A9FE-1D82-786C4DC9D380',
        'ChecksumType': 'Adler32', 'Checksum': '8b994dd5', 'Size': 1100L, 'UID': 2,
        'OwnerGroup': 'lz_production', 'Owner': 'daniela.bauer', 'GID': 24, 'Mode': 509,
        'ModificationDate': datetime.datetime(2021, 6, 11, 14, 23, 51),
        'CreationDate': datetime.datetime(2021, 6, 11, 14, 23, 51), 'Type': 'File', 'FileID': 27519475L}},
        '/lz/data/test/sites2.log': {'MetaData': {'Status': 'AprioriGood',
        'GUID': 'AD81AD07-3BC0-A9FE-1D82-786C4DC9D380', 'ChecksumType': 'Adler32', 'Checksum': '8b994dd5',
        'Size': 1100L, 'UID': 2, 'OwnerGroup': 'lz_production', 'Owner': 'daniela.bauer', 'GID': 24,
        'Mode': 509, 'ModificationDate': datetime.datetime(2021, 6, 16, 15, 26, 21),
        'CreationDate': datetime.datetime(2021, 6, 16, 15, 26, 21), 'Type': 'File', 'FileID': 27601076L}}},
        'Datasets': {}, 'SubDirs': {'/lz/data/test/reconstructed': True,
        '/lz/data/test/BACCARAT_release-2.1.1_geant4.9.5.p02': True,
        '/lz/data/test/BACCARAT_release-2.1.0_geant4.9.5.p02': True}, 'Links': {}}}, 'Failed': {}}}

       :param directoryLFN: LFN of the directory to be listed
       :type directoryLFN: string or list in LFN format
       :param printOutput: prints output in a more human readable form
       :type printOutput: bool
       :returns: S_OK,S_ERROR. S_OK returns a dictionary. Please see the example for its structure.
    """
    ret = self._checkFileArgument(directoryLFN, 'LFN')
    if not ret['OK']:
      return ret
    res = FileCatalog().listDirectory(directoryLFN)
    if not res['OK']:
      self.log.warn(res['Message'])
      return res
    if not res['Value']['Successful']:
      self.log.warn("listCatalogDir failed for all LFNs (%s)." % directoryLFN)
      return res
    # now deal with the case where *some* of the LFNs are OK
    if res['Value']['Failed']:
      self.log.warn("listCatalogDir failed for: %s" % (res['Value']['Failed']))
      # do not return, we still want to process the good ones
    if printOutput:
      # treat a string as array with a single entry
      if isinstance(directoryLFN, str):
        directoryLFN = [directoryLFN]
      for directory in directoryLFN:
        if directory in res['Value']['Successful']:
          print("Listing content of: %s" % directory)
          subdirs = res['Value']['Successful'][directory]['SubDirs']
          files = res['Value']['Successful'][directory]['Files']
          print("Subdirectories:")
          print('\n'.join(subdirs))
          print("Files:")
          print('\n'.join(files))

    return res

  #############################################################################
  # def listCatalog( self, directory, printOutput = False ):
  #   """ Under development.
  #       Obtain listing of the specified directory.
  #   """
  #   rm = ReplicaManager()
  #   listing = rm.listCatalogDirectory( directory )
  #   if re.search( '\/$', directory ):
  #     directory = directory[:-1]
  #
  #   if printOutput:
  #     for fileKey, metaDict in listing['Value']['Successful'][directory]['Files'].items():  # can be an iterator
  #       print '#' * len( fileKey )
  #       print fileKey
  #       print '#' * len( fileKey )
  #       print self.pPrint.pformat( metaDict )

  #############################################################################

  def getReplicas(self, lfns, active=True, preferDisk=False, diskOnly=False, printOutput=False):
    """Obtain replica information from file catalogue client. Input LFN(s) can be string or list.

       Example usage:

       >>> print dirac.getReplicas('/lhcb/data/CCRC08/RDST/00000106/0000/00000106_00006321_1.rdst')
       {'OK': True, 'Value': {'Successful': {'/lhcb/data/CCRC08/RDST/00000106/0000/00000106_00006321_1.rdst':
       {'CERN-RDST':
       'srm://srm-lhcb.cern.ch/castor/cern.ch/grid/lhcb/data/CCRC08/RDST/00000106/0000/00000106_00006321_1.rdst'}},
       'Failed': {}}}

       :param lfns: Logical File Name(s) to query
       :type lfns: LFN str or python:list []
       :param active: restrict to only replicas at SEs that are not banned
       :type active: boolean
       :param preferDisk: give preference to disk replicas if True
       :type preferDisk: boolean
       :param diskOnly: restrict to only disk replicas if True
       :type diskOnly: boolean
       :param printOutput: Optional flag to print result
       :type printOutput: boolean
       :returns: S_OK,S_ERROR
    """
    ret = self._checkFileArgument(lfns, 'LFN')
    if not ret['OK']:
      return ret
    lfns = ret['Value']

    start = time.time()
    dm = DataManager()
    repsResult = dm.getReplicas(lfns, active=active, preferDisk=preferDisk, diskOnly=diskOnly)
    timing = time.time() - start
    self.log.info('Replica Lookup Time: %.2f seconds ' % (timing))
    self.log.debug(repsResult)
    if not repsResult['OK']:
      self.log.warn(repsResult['Message'])
      return repsResult

    if printOutput:
      fields = ['LFN', 'StorageElement', 'URL']
      records = []
      for lfn in repsResult['Value']['Successful']:
        lfnPrint = lfn
        for se, url in repsResult['Value']['Successful'][lfn].items():  # can be an iterator
          records.append((lfnPrint, se, url))
          lfnPrint = ''
      for lfn in repsResult['Value']['Failed']:
        records.append((lfn, 'Unknown', str(repsResult['Value']['Failed'][lfn])))

      printTable(fields, records, numbering=False)

    return repsResult

  def getReplicasForJobs(self, lfns, diskOnly=False, printOutput=False):
    """Obtain replica information from file catalogue client. Input LFN(s) can be string or list.

       Example usage:

       >>> print dirac.getReplicasForJobs('/lhcb/data/CCRC08/RDST/00000106/0000/00000106_00006321_1.rdst')
       {'OK': True, 'Value': {'Successful': {'/lhcb/data/CCRC08/RDST/00000106/0000/00000106_00006321_1.rdst':
       {'CERN-RDST':
       'srm://srm-lhcb.cern.ch/castor/cern.ch/grid/lhcb/data/CCRC08/RDST/00000106/0000/00000106_00006321_1.rdst'}},
       'Failed': {}}}

       :param lfns: Logical File Name(s) to query
       :type lfns: LFN str or python:list []
       :param diskOnly: restrict to only disk replicas if True
       :type diskOnly: boolean
       :param printOutput: Optional flag to print result
       :type printOutput: boolean
       :returns: S_OK,S_ERROR
    """
    ret = self._checkFileArgument(lfns, 'LFN')
    if not ret['OK']:
      return ret
    lfns = ret['Value']

    start = time.time()
    dm = DataManager()
    repsResult = dm.getReplicasForJobs(lfns, diskOnly=diskOnly)
    timing = time.time() - start
    self.log.info('Replica Lookup Time: %.2f seconds ' % (timing))
    self.log.debug(repsResult)
    if not repsResult['OK']:
      self.log.warn(repsResult['Message'])
      return repsResult

    if printOutput:
      fields = ['LFN', 'StorageElement', 'URL']
      records = []
      for lfn in repsResult['Value']['Successful']:
        lfnPrint = lfn
        for se, url in repsResult['Value']['Successful'][lfn].items():  # can be an iterator
          records.append((lfnPrint, se, url))
          lfnPrint = ''
      for lfn in repsResult['Value']['Failed']:
        records.append((lfn, 'Unknown', str(repsResult['Value']['Failed'][lfn])))

      printTable(fields, records, numbering=False)

    return repsResult

  #############################################################################
  def getAllReplicas(self, lfns, printOutput=False):
    """Only differs from getReplicas method in the sense that replicas on banned SEs
       will be included in the result.

       Obtain replica information from file catalogue client. Input LFN(s) can be string or list.

       Example usage:

       >>> print dirac.getAllReplicas('/lhcb/data/CCRC08/RDST/00000106/0000/00000106_00006321_1.rdst')
       {'OK': True, 'Value': {'Successful': {'/lhcb/data/CCRC08/RDST/00000106/0000/00000106_00006321_1.rdst':
       {'CERN-RDST':
       'srm://srm-lhcb.cern.ch/castor/cern.ch/grid/lhcb/data/CCRC08/RDST/00000106/0000/00000106_00006321_1.rdst'}},
       'Failed': {}}}

       :param lfns: Logical File Name(s) to query
       :type lfns: LFN str or python:list
       :param printOutput: Optional flag to print result
       :type printOutput: bool
       :returns: S_OK,S_ERROR
    """
    ret = self._checkFileArgument(lfns, 'LFN')
    if not ret['OK']:
      return ret
    lfns = ret['Value']

#     rm = ReplicaManager()
#     start = time.time()
#     repsResult = rm.getCatalogReplicas( lfns )
    # RF_NOTE : this method will return different values that api.getReplicas
    fc = FileCatalog()
    start = time.time()
    repsResult = fc.getReplicas(lfns)

    timing = time.time() - start
    self.log.info('Replica Lookup Time: %.2f seconds ' % (timing))
    self.log.verbose(repsResult)
    if not repsResult['OK']:
      self.log.warn(repsResult['Message'])
      return repsResult

    if printOutput:
      print(self.pPrint.pformat(repsResult['Value']))

    return repsResult

  def checkSEAccess(self, se, access='Write'):
    """ returns the value of a certain SE status flag (access or other)

      :param se: Storage Element name
      :type se: string
      :param access: type of access
      :type access: string in ('Read', 'Write', 'Remove', 'Check')
      :returns: True or False
    """
    return StorageElement(se, vo=self.vo).status().get(access, False)

  #############################################################################
  def splitInputData(self, lfns, maxFilesPerJob=20, printOutput=False):
    """Split the supplied lfn list by the replicas present at the possible
       destination sites.  An S_OK object will be returned containing a list of
       lists in order to create the jobs.

       Example usage:

       >>> d.splitInputData(lfns,10)
       {'OK': True, 'Value': [['<LFN>'], ['<LFN>']]}


       :param lfns: Logical File Name(s) to split
       :type lfns: python:list
       :param maxFilesPerJob: Number of files per bunch
       :type maxFilesPerJob: integer
       :param printOutput: Optional flag to print result
       :type printOutput: boolean
       :returns: S_OK,S_ERROR
    """
    from DIRAC.Core.Utilities.SiteSEMapping import getSitesForSE
    sitesForSE = {}
    ret = self._checkFileArgument(lfns, 'LFN')
    if not ret['OK']:
      return ret
    lfns = ret['Value']

    if not isinstance(maxFilesPerJob, six.integer_types):
      try:
        maxFilesPerJob = int(maxFilesPerJob)
      except Exception as x:
        return self._errorReport(str(x), 'Expected integer for maxFilesPerJob')

    replicaDict = self.getReplicasForJobs(lfns)
    if not replicaDict['OK']:
      return replicaDict
    if not replicaDict['Value']['Successful']:
      return self._errorReport(list(replicaDict['Value']['Failed'].items())[0],
                               'Failed to get replica information')
    siteLfns = {}
    for lfn, reps in replicaDict['Value']['Successful'].items():  # can be an iterator
      possibleSites = set(site for se in reps for site in (
          sitesForSE[se] if se in sitesForSE else sitesForSE.setdefault(se, getSitesForSE(se).get('Value', []))))
      siteLfns.setdefault(','.join(sorted(possibleSites)), []).append(lfn)

    if '' in siteLfns:
      # Some files don't have active replicas
      return self._errorReport('No active replica found for', str(siteLfns['']))
    lfnGroups = []
    for files in siteLfns.values():
      lists = breakListIntoChunks(files, maxFilesPerJob)
      lfnGroups += lists

    if printOutput:
      print(self.pPrint.pformat(lfnGroups))
    return S_OK(lfnGroups)

  #############################################################################

  def getLfnMetadata(self, lfns, printOutput=False):
    """Obtain replica metadata from file catalogue client.
       Input LFN(s) can be string or list. LFN(s) can be either files or directories

       Example usage:

       >>> print dirac.getLfnMetadata('/lhcb/data/CCRC08/RDST/00000106/0000/00000106_00006321_1.rdst')
       {'OK': True, 'Value': {'Successful': {'/lhcb/data/CCRC08/RDST/00000106/0000/00000106_00006321_1.rdst':
       {'Status': '-', 'Size': 619475828L, 'GUID': 'E871FBA6-71EA-DC11-8F0C-000E0C4DEB4B', 'ChecksumType': 'AD',
       'CheckSumValue': ''}}, 'Failed': {}}}

       :param lfns: Logical File Name(s) to query
       :type lfns: LFN str or python:list []
       :param printOutput: Optional flag to print result
       :type printOutput: boolean
       :returns: S_OK,S_ERROR
    """
    ret = self._checkFileArgument(lfns, 'LFN')
    if not ret['OK']:
      return ret
    lfns = ret['Value']

    fc = FileCatalog()
    start = time.time()
    fileResult = fc.getFileMetadata(lfns)
    timing = time.time() - start
    self.log.info('Metadata Lookup Time: %.2f seconds ' % (timing))
    self.log.verbose(fileResult)
    if not fileResult['OK']:
      self.log.warn('Failed to retrieve file metadata from the catalogue', fileResult['Message'])
      return fileResult

    repsResult = fileResult['Value']
    if repsResult['Failed']:
      # Some entries can be directories
      dirs = list(repsResult['Failed'])
      dirResult = fc.getDirectoryMetadata(dirs)
      if not dirResult['OK']:
        self.log.warn('Failed to retrieve directory metadata from the catalogue')
        self.log.warn(dirResult['Message'])
        return dirResult
      for directory in dirResult['Value']['Successful']:
        repsResult['Successful'][directory] = dirResult['Value']['Successful'][directory]
        repsResult['Failed'].pop(directory)

    if printOutput:
      print(self.pPrint.pformat(repsResult))

    return S_OK(repsResult)

  #############################################################################
  def addFile(self, lfn, fullPath, diracSE, fileGuid=None, printOutput=False):
    """Add a single file to Grid storage. lfn is the desired logical file name
       for the file, fullPath is the local path to the file and diracSE is the
       Storage Element name for the upload.  The fileGuid is optional, if not
       specified a GUID will be generated on the fly.  If subsequent access
       depends on the file GUID the correct one should

       Example Usage:

       >>> print dirac.addFile('/lhcb/user/p/paterson/myFile.tar.gz','myFile.tar.gz','CERN-USER')
       {'OK': True, 'Value':{'Failed': {},
        'Successful': {'/lhcb/user/p/paterson/test/myFile.tar.gz': {'put': 64.246301889419556,
                                                                    'register': 1.1102778911590576}}}}

       :param lfn: Logical File Name (LFN)
       :type lfn: string
       :param diracSE: DIRAC SE name e.g. CERN-USER
       :type diracSE: string
       :param printOutput: Optional flag to print result
       :type printOutput: boolean
       :returns: S_OK,S_ERROR
    """
    ret = self._checkFileArgument(lfn, 'LFN', single=True)
    if not ret['OK']:
      return ret
    lfn = ret['Value']

    if not os.path.exists(fullPath):
      return self._errorReport('Local file %s does not exist' % (fullPath))

    if not os.path.isfile(fullPath):
      return self._errorReport('Expected path to file not %s' % (fullPath))

    dm = DataManager(catalogs=self.defaultFileCatalog)
    result = dm.putAndRegister(lfn, fullPath, diracSE, guid=fileGuid)
    if not result['OK']:
      return self._errorReport('Problem during putAndRegister call', result['Message'])
    if printOutput:
      print(self.pPrint.pformat(result['Value']))
    return result

  #############################################################################
  def getFile(self, lfn, destDir='', printOutput=False):
    """Retrieve a single file or list of files from Grid storage to the current directory. lfn is the
       desired logical file name for the file, fullPath is the local path to the file and diracSE is the
       Storage Element name for the upload.  The fileGuid is optional, if not specified a GUID will be
       generated on the fly.

       Example Usage:

       >>> print dirac.getFile('/lhcb/user/p/paterson/myFile.tar.gz')
       {'OK': True, 'Value':{'Failed': {},
        'Successful': {'/lhcb/user/p/paterson/test/myFile.tar.gz': '/afs/cern.ch/user/p/paterson/myFile.tar.gz'}}}

       :param lfn: Logical File Name (LFN)
       :type lfn: string
       :param printOutput: Optional flag to print result
       :type printOutput: boolean
       :returns: S_OK,S_ERROR
    """
    ret = self._checkFileArgument(lfn, 'LFN')
    if not ret['OK']:
      return ret
    lfn = ret['Value']

    dm = DataManager()
    result = dm.getFile(lfn, destinationDir=destDir)
    if not result['OK']:
      return self._errorReport('Problem during getFile call', result['Message'])

    if result['Value']['Failed']:
      self.log.error('Failures occurred during rm.getFile')
      if printOutput:
        print(self.pPrint.pformat(result['Value']))
      return S_ERROR(result['Value'])

    if printOutput:
      print(self.pPrint.pformat(result['Value']))
    return result

  #############################################################################
  def replicateFile(self, lfn, destinationSE, sourceSE='', localCache='', printOutput=False):
    """Replicate an existing file to another Grid SE. lfn is the desired logical file name
       for the file to be replicated, destinationSE is the DIRAC Storage Element to create a
       replica of the file at.  Optionally the source storage element and local cache for storing
       the retrieved file for the new upload can be specified.

       Example Usage:

       >>> print dirac.replicateFile('/lhcb/user/p/paterson/myFile.tar.gz','CNAF-USER')
       {'OK': True, 'Value':{'Failed': {},
       'Successful': {'/lhcb/user/p/paterson/test/myFile.tar.gz': {'register': 0.44766902923583984,
                                                                  'replicate': 56.42345404624939}}}}

       :param lfn: Logical File Name (LFN)
       :type lfn: string
       :param destinationSE: Destination DIRAC SE name e.g. CERN-USER
       :type destinationSE: string
       :param sourceSE: Optional source SE
       :type sourceSE: string
       :param localCache: Optional path to local cache
       :type localCache: string
       :param printOutput: Optional flag to print result
       :type printOutput: boolean
       :returns: S_OK,S_ERROR
    """
    ret = self._checkFileArgument(lfn, 'LFN', single=True)
    if not ret['OK']:
      return ret
    lfn = ret['Value']

    if not sourceSE:
      sourceSE = ''
    if not localCache:
      localCache = ''
    if not isinstance(sourceSE, six.string_types):
      return self._errorReport('Expected string for source SE name')
    if not isinstance(localCache, six.string_types):
      return self._errorReport('Expected string for path to local cache')

    localFile = os.path.join(localCache, os.path.basename(lfn))
    if os.path.exists(localFile):
      return self._errorReport('A local file "%s" with the same name as the remote file exists. '
                               'Cannot proceed with replication:\n'
                               '   Go to a different working directory\n'
                               '   Move it different directory or use a different localCache\n'
                               '   Delete the file yourself'
                               '' % localFile)

    dm = DataManager()
    result = dm.replicateAndRegister(lfn, destinationSE, sourceSE, '', localCache)
    if not result['OK']:
      return self._errorReport('Problem during replicateFile call', result['Message'])
    if printOutput:
      print(self.pPrint.pformat(result['Value']))
    return result

  def replicate(self, lfn, destinationSE, sourceSE='', printOutput=False):
    """Replicate an existing file to another Grid SE. lfn is the desired logical file name
       for the file to be replicated, destinationSE is the DIRAC Storage Element to create a
       replica of the file at.  Optionally the source storage element and local cache for storing
       the retrieved file for the new upload can be specified.

       Example Usage:

       >>> print dirac.replicate('/lhcb/user/p/paterson/myFile.tar.gz','CNAF-USER')
       {'OK': True, 'Value':{'Failed': {},
       'Successful': {'/lhcb/user/p/paterson/test/myFile.tar.gz': {'register': 0.44766902923583984}}}}

       :param lfn: Logical File Name (LFN)
       :type lfn: string
       :param destinationSE: Destination DIRAC SE name e.g. CERN-USER
       :type destinationSE: string
       :param sourceSE: Optional source SE
       :type sourceSE: string
       :param printOutput: Optional flag to print result
       :type printOutput: boolean
       :returns: S_OK,S_ERROR
    """
    ret = self._checkFileArgument(lfn, 'LFN', single=True)
    if not ret['OK']:
      return ret
    lfn = ret['Value']

    if not sourceSE:
      sourceSE = ''

    if not isinstance(sourceSE, six.string_types):
      return self._errorReport('Expected string for source SE name')

    dm = DataManager()
    result = dm.replicate(lfn, destinationSE, sourceSE, '')
    if not result['OK']:
      return self._errorReport('Problem during replicate call', result['Message'])
    if printOutput:
      print(self.pPrint.pformat(result['Value']))
    return result

  #############################################################################
  def getAccessURL(self, lfn, storageElement, printOutput=False, protocol=False):
    """Allows to retrieve an access URL for an LFN replica given a valid DIRAC SE
       name.  Contacts the file catalog and contacts the site endpoint behind the scenes.

       Example Usage:

       >>> print dirac.getAccessURL('/lhcb/data/CCRC08/DST/00000151/0000/00000151_00004848_2.dst','CERN-RAW')
       {'OK': True, 'Value': {'Successful': {'srm://...': {'SRM2': 'rfio://...'}}, 'Failed': {}}}

       :param lfn: Logical File Name (LFN)
       :type lfn: str or python:list
       :param storageElement: DIRAC SE name e.g. CERN-RAW
       :type storageElement: string
       :param printOutput: Optional flag to print result
       :type printOutput: boolean
       :param protocol: protocol requested
       :type protocol: str or python:list
       :returns: S_OK,S_ERROR
    """
    ret = self._checkFileArgument(lfn, 'LFN')
    if not ret['OK']:
      return ret
    lfn = ret['Value']

    dm = DataManager()
    result = dm.getReplicaAccessUrl(lfn, storageElement, protocol=protocol)
    if not result['OK']:
      return self._errorReport('Problem during getAccessURL call', result['Message'])
    if printOutput:
      print(self.pPrint.pformat(result['Value']))
    return result

  #############################################################################
  def getPhysicalFileAccessURL(self, pfn, storageElement, printOutput=False):
    """Allows to retrieve an access URL for an PFN  given a valid DIRAC SE
       name.  The SE is contacted directly for this information.

       Example Usage:

       >>> print dirac.getPhysicalFileAccessURL('srm://srm-lhcb.cern.ch/castor/cern.ch/grid/lhcb/data/CCRC08/DST/00000151/0000/00000151_00004848_2.dst','CERN_M-DST')
       {'OK': True, 'Value':{'Failed': {},
       'Successful': {'srm://srm-lhcb.cern.ch/castor/cern.ch/grid/lhcb/data/CCRC08/DST/00000151/0000/00000151_00004848_2.dst': {'RFIO': 'castor://...'}}}}

       :param pfn: Physical File Name (PFN)
       :type pfn: str or python:list
       :param storageElement: DIRAC SE name e.g. CERN-RAW
       :type storageElement: string
       :param printOutput: Optional flag to print result
       :type printOutput: boolean
       :returns: S_OK,S_ERROR
    """
    ret = self._checkFileArgument(pfn, 'PFN')
    if not ret['OK']:
      return ret
    pfn = ret['Value']

    result = StorageElement(storageElement).getURL([pfn])
    if not result['OK']:
      return self._errorReport('Problem during getAccessURL call', result['Message'])
    if printOutput:
      print(self.pPrint.pformat(result['Value']))
    return result

  #############################################################################
  def getPhysicalFileMetadata(self, pfn, storageElement, printOutput=False):
    """Allows to retrieve metadata for physical file(s) on a supplied storage
       element.  Contacts the site endpoint and performs a gfal_ls behind
       the scenes.

       Example Usage:

       >>> print dirac.getPhysicalFileMetadata('srm://srm.grid.sara.nl/pnfs/grid.sara.nl/data
       /lhcb/data/CCRC08/RAW/LHCb/CCRC/23341/023341_0000039571.raw','NIKHEF-RAW')
       {'OK': True, 'Value': {'Successful': {'srm://...': {'SRM2': 'rfio://...'}}, 'Failed': {}}}

       :param pfn: Physical File Name (PFN)
       :type pfn: str or python:list
       :param storageElement: DIRAC SE name e.g. CERN-RAW
       :type storageElement: string
       :param printOutput: Optional flag to print result
       :type printOutput: boolean
       :returns: S_OK,S_ERROR
    """
    ret = self._checkFileArgument(pfn, 'PFN')
    if not ret['OK']:
      return ret
    pfn = ret['Value']

    result = StorageElement(storageElement).getFileMetadata(pfn)
    if not result['OK']:
      return self._errorReport('Problem during getStorageFileMetadata call', result['Message'])
    if printOutput:
      print(self.pPrint.pformat(result['Value']))
    return result

  #############################################################################
  def removeFile(self, lfn, printOutput=False):
    """Remove LFN and *all* associated replicas from Grid Storage Elements and
       file catalogues.

       Example Usage:

       >>> print dirac.removeFile('LFN:/lhcb/data/CCRC08/RAW/LHCb/CCRC/22808/022808_0000018443.raw')
       {'OK': True, 'Value':...}

       :param lfn: Logical File Name (LFN)
       :type lfn: string
       :param printOutput: Flag to print to stdOut
       :type printOutput: Boolean
       :returns: S_OK,S_ERROR

    """
    ret = self._checkFileArgument(lfn, 'LFN')
    if not ret['OK']:
      return ret
    lfn = ret['Value']

    dm = DataManager()
    result = dm.removeFile(lfn)
    if printOutput and result['OK']:
      print(self.pPrint.pformat(result['Value']))
    return result

  #############################################################################
  def removeReplica(self, lfn, storageElement, printOutput=False):
    """Remove replica of LFN from specified Grid Storage Element and
       file catalogues.

       Example Usage:

       >>> print dirac.removeReplica('LFN:/lhcb/user/p/paterson/myDST.dst','CERN-USER')
       {'OK': True, 'Value':...}

       :param lfn: Logical File Name (LFN)
       :type lfn: string
       :param storageElement: DIRAC SE Name
       :type storageElement: string
       :returns: S_OK,S_ERROR
    """
    ret = self._checkFileArgument(lfn, 'LFN')
    if not ret['OK']:
      return ret
    lfn = ret['Value']

    dm = DataManager()
    result = dm.removeReplica(storageElement, lfn)
    if printOutput and result['OK']:
      print(self.pPrint.pformat(result['Value']))
    return result

  #############################################################################
  def getInputSandbox(self, jobID, outputDir=None):
    """Retrieve input sandbox for existing JobID.

       This method allows the retrieval of an existing job input sandbox for
       debugging purposes.  By default the sandbox is downloaded to the current
       directory but this can be overridden via the outputDir parameter. All files
       are extracted into a InputSandbox<JOBID> directory that is automatically created.

       Example Usage:

       >>> print dirac.getInputSandbox(12345)
       {'OK': True, 'Value': ['Job__Sandbox__.tar.bz2']}

       :param jobID: JobID
       :type jobID: integer or string
       :param outputDir: Optional directory for files
       :type outputDir: string
       :returns: S_OK,S_ERROR
    """
    ret = self._checkJobArgument(jobID, multiple=False)
    if not ret['OK']:
      return ret
    jobID = ret['Value']

    # TODO: Do not check if dir already exists
    dirPath = ''
    if outputDir:
      dirPath = '%s/InputSandbox%s' % (outputDir, jobID)
      if os.path.exists(dirPath):
        return self._errorReport('Job input sandbox directory %s already exists' % (dirPath))
    else:
      dirPath = '%s/InputSandbox%s' % (os.getcwd(), jobID)
      if os.path.exists(dirPath):
        return self._errorReport('Job input sandbox directory %s already exists' % (dirPath))

    try:
      os.mkdir(dirPath)
    except Exception as x:
      return self._errorReport(repr(x), 'Could not create directory in %s' % (dirPath))

    result = SandboxStoreClient(useCertificates=self.useCertificates).downloadSandboxForJob(jobID,
                                                                                            'Input',
                                                                                            dirPath)
    if not result['OK']:
      self.log.warn(result['Message'])
    else:
      self.log.info('Files retrieved and extracted in %s' % (dirPath))
    return result

  #############################################################################
  def getOutputSandbox(self, jobID, outputDir=None, oversized=True, noJobDir=False, unpack=True):
    """Retrieve output sandbox for existing JobID.

       This method allows the retrieval of an existing job output sandbox.
       By default the sandbox is downloaded to the current directory but
       this can be overridden via the outputDir parameter. All files are
       extracted into a <JOBID> directory that is automatically created.

       Example Usage:

       >>> print dirac.getOutputSandbox(12345)
       {'OK': True, 'Value': ['Job__Sandbox__.tar.bz2']}

       :param jobID: JobID
       :type jobID: integer or string
       :param outputDir: Optional directory path
       :type outputDir: string
       :param oversized: Optionally disable oversized sandbox download
       :type oversized: boolean
       :returns: S_OK,S_ERROR
    """
    ret = self._checkJobArgument(jobID, multiple=False)
    if not ret['OK']:
      return ret
    jobID = ret['Value']

    dirPath = ''
    if outputDir:
      dirPath = outputDir
      if not noJobDir:
        dirPath = '%s/%s' % (outputDir, jobID)
    else:
      dirPath = '%s/%s' % (os.getcwd(), jobID)
      if os.path.exists(dirPath):
        return self._errorReport('Job output directory %s already exists' % (dirPath))
    mkDir(dirPath)

    # New download
    result = SandboxStoreClient(useCertificates=self.useCertificates).downloadSandboxForJob(jobID,
                                                                                            'Output',
                                                                                            dirPath,
                                                                                            inMemory=False,
                                                                                            unpack=unpack)
    if result['OK']:
      self.log.info('Files retrieved and extracted in %s' % (dirPath))
      if self.jobRepo:
        self.jobRepo.updateJob(jobID, {'Retrieved': 1, 'Sandbox': os.path.realpath(dirPath)})
      return result
    self.log.warn(result['Message'])

    if not oversized:
      if self.jobRepo:
        self.jobRepo.updateJob(jobID, {'Retrieved': 1, 'Sandbox': os.path.realpath(dirPath)})
      return result

    params = self.getJobParameters(int(jobID))
    if not params['OK']:
      self.log.verbose('Could not retrieve job parameters to check for oversized sandbox')
      return params

    if not params['Value'].get('OutputSandboxLFN'):
      self.log.verbose('No oversized output sandbox for job %s:\n%s' % (jobID, params))
      return result

    oversizedSandbox = params['Value']['OutputSandboxLFN']
    if not oversizedSandbox:
      self.log.verbose('Null OutputSandboxLFN for job %s' % jobID)
      return result

    self.log.info('Attempting to retrieve %s' % oversizedSandbox)
    start = os.getcwd()
    os.chdir(dirPath)
    getFile = self.getFile(oversizedSandbox)
    if not getFile['OK']:
      self.log.warn('Failed to download %s with error:%s' % (oversizedSandbox, getFile['Message']))
      os.chdir(start)
      return getFile

    fileName = os.path.basename(oversizedSandbox)
    result = S_OK(oversizedSandbox)
    if tarfile.is_tarfile(fileName):
      try:
        with tarfile.open(fileName, 'r') as tf:
          for member in tf.getmembers():
            tf.extract(member, dirPath)
      except Exception as x:
        os.chdir(start)
        result = S_ERROR(str(x))

    if os.path.exists(fileName):
      os.unlink(fileName)

    os.chdir(start)
    if result['OK']:
      if self.jobRepo:
        self.jobRepo.updateJob(jobID, {'Retrieved': 1, 'Sandbox': os.path.realpath(dirPath)})
    return result

  #############################################################################

  def deleteJob(self, jobID):
    """Delete job or list of jobs from the WMS, if running these jobs will
       also be killed.

       Example Usage:

       >>> print dirac.deleteJob(12345)
       {'OK': True, 'Value': [12345]}

       :param jobID: JobID
       :type jobID: int, str or python:list
       :returns: S_OK,S_ERROR

    """
    ret = self._checkJobArgument(jobID, multiple=True)
    if not ret['OK']:
      return ret
    jobID = ret['Value']

    result = WMSClient(useCertificates=self.useCertificates).deleteJob(jobID)
    if result['OK']:
      if self.jobRepo:
        for jID in result['Value']:
          self.jobRepo.removeJob(jID)
    return result

  #############################################################################

  def rescheduleJob(self, jobID):
    """Reschedule a job or list of jobs in the WMS.  This operation is the same
       as resubmitting the same job as new.  The rescheduling operation may be
       performed to a configurable maximum number of times but the owner of a job
       can also reset this counter and reschedule jobs again by hand.

       Example Usage:

       >>> print dirac.rescheduleJob(12345)
       {'OK': True, 'Value': [12345]}

       :param jobID: JobID
       :type jobID: int, str or python:list
       :returns: S_OK,S_ERROR

    """
    ret = self._checkJobArgument(jobID, multiple=True)
    if not ret['OK']:
      return ret
    jobID = ret['Value']

    result = WMSClient(useCertificates=self.useCertificates).rescheduleJob(jobID)
    if result['OK']:
      if self.jobRepo:
        repoDict = {}
        for jID in result['Value']:
          repoDict[jID] = {'State': 'Submitted'}
        self.jobRepo.updateJobs(repoDict)
    return result

  def killJob(self, jobID):
    """Issue a kill signal to a running job.  If a job has already completed this
       action is harmless but otherwise the process will be killed on the compute
       resource by the Watchdog.

       Example Usage:

        >>> print dirac.killJob(12345)
        {'OK': True, 'Value': [12345]}

       :param jobID: JobID
       :type jobID: int, str or python:list
       :returns: S_OK,S_ERROR

    """
    ret = self._checkJobArgument(jobID, multiple=True)
    if not ret['OK']:
      return ret
    jobID = ret['Value']

    result = WMSClient(useCertificates=self.useCertificates).killJob(jobID)
    if result['OK']:
      if self.jobRepo:
        for jID in result['Value']:
          self.jobRepo.removeJob(jID)
    return result

  #############################################################################

  def getJobStatus(self, jobID):
    """Monitor the status of DIRAC Jobs.

       Example Usage:

       >>> print dirac.getJobStatus(79241)
       {79241: {'status': 'Done', 'site': 'LCG.CERN.ch'}}

       :param jobID: JobID
       :type jobID: int, str or python:list
       :returns: S_OK,S_ERROR
    """
    ret = self._checkJobArgument(jobID, multiple=True)
    if not ret['OK']:
      return ret
    jobID = ret['Value']

    monitoring = JobMonitoringClient()
    statusDict = monitoring.getJobsStatus(jobID)
    minorStatusDict = monitoring.getJobsMinorStatus(jobID)
    siteDict = monitoring.getJobsSites(jobID)

    if not statusDict['OK']:
      self.log.warn('Could not obtain job status information')
      return statusDict
    if not siteDict['OK']:
      self.log.warn('Could not obtain job site information')
      return siteDict
    if not minorStatusDict['OK']:
      self.log.warn('Could not obtain job minor status information')
      return minorStatusDict

    result = {}
    repoDict = {}
    for job, vals in statusDict['Value'].items():  # can be an iterator
      result[job] = vals
      if self.jobRepo:
        repoDict[job] = {'State': vals['Status']}
    if self.jobRepo:
      self.jobRepo.updateJobs(repoDict)
    for job, vals in siteDict['Value'].items():  # can be an iterator
      result[job].update(vals)
    for job, vals in minorStatusDict['Value'].items():  # can be an iterator
      result[job].update(vals)
    for job in result:
      result[job].pop('JobID', None)

    return S_OK(result)

  #############################################################################
  def getJobInputData(self, jobID):
    """Retrieve the input data requirement of any job existing in the workload management
       system.

       Example Usage:

       >>> dirac.getJobInputData(1405)
       {'OK': True, 'Value': {1405:
        ['LFN:/lhcb/production/DC06/phys-v2-lumi5/00001680/DST/0000/00001680_00000490_5.dst']}}

       :param jobID: JobID
       :type jobID: int, str or python:list
       :returns: S_OK,S_ERROR
    """
    ret = self._checkJobArgument(jobID, multiple=True)
    if not ret['OK']:
      return ret
    jobID = ret['Value']

    summary = {}
    monitoring = JobMonitoringClient()
    for job in jobID:
      result = monitoring.getInputData(job)
      if result['OK']:
        summary[job] = result['Value']
      else:
        self.log.warn('Getting input data for job %s failed with message:\n%s' % (job, result['Message']))
        summary[job] = []

    return S_OK(summary)

  #############################################################################
  def getJobOutputLFNs(self, jobID):
    """ Retrieve the output data LFNs of a given job locally.

       This does not download the output files but simply returns the LFN list
       that a given job has produced.

       Example Usage:

       >>> dirac.getJobOutputLFNs(1405)
       {'OK':True,'Value':[<LFN>]}

       :param jobID: JobID
       :type jobID: int or string
       :returns: S_OK,S_ERROR
    """
    try:
      jobID = int(jobID)
    except ValueError as x:
      return self._errorReport(str(x), 'Expected integer or string for existing jobID')

    result = self.getJobParameters(jobID)
    if not result['OK']:
      return result
    if not result['Value'].get('UploadedOutputData'):
      self.log.info('Parameters for job %s do not contain uploaded output data:\n%s' % (jobID, result))
      return S_ERROR('No output data found for job %s' % jobID)

    outputData = result['Value']['UploadedOutputData']
    outputData = outputData.replace(' ', '').split(',')
    if not outputData:
      return S_ERROR('No output data files found')

    self.log.verbose('Found the following output data LFNs:\n', '\n'.join(outputData))
    return S_OK(outputData)

  #############################################################################
  def getJobOutputData(self, jobID, outputFiles='', destinationDir=''):
    """ Retrieve the output data files of a given job locally.

       Optionally restrict the download of output data to a given file name or
       list of files using the outputFiles option, by default all job outputs
       will be downloaded.

       Example Usage:

       >>> dirac.getJobOutputData(1405)
       {'OK':True,'Value':[<LFN>]}

       :param jobID: JobID
       :type jobID: int or string
       :param outputFiles: Optional files to download
       :type outputFiles: str or python:list
       :returns: S_OK,S_ERROR
    """
    try:
      jobID = int(jobID)
    except ValueError as x:
      return self._errorReport(str(x), 'Expected integer or string for existing jobID')

    result = self.getJobParameters(jobID)
    if not result['OK']:
      return result
    if not result['Value'].get('UploadedOutputData'):
      self.log.info('Parameters for job %s do not contain uploaded output data:\n%s' % (jobID, result))
      return S_ERROR('No output data found for job %s' % jobID)

    outputData = result['Value']['UploadedOutputData']
    outputData = outputData.replace(' ', '').split(',')
    if not outputData:
      return S_ERROR('No output data files found to download')

    if outputFiles:
      if isinstance(outputFiles, six.string_types):
        outputFiles = [os.path.basename(outputFiles)]
      elif isinstance(outputFiles, list):
        try:
          outputFiles = [os.path.basename(fname) for fname in outputFiles]
        except AttributeError as x:
          return self._errorReport(str(x), 'Expected strings for output file names')
      else:
        return self._errorReport('Expected strings for output file names')
      self.log.info('Found specific outputFiles to download:', ', '.join(outputFiles))
      newOutputData = []
      for outputFile in outputData:
        if os.path.basename(outputFile) in outputFiles:
          newOutputData.append(outputFile)
          self.log.verbose('%s will be downloaded' % outputFile)
        else:
          self.log.verbose('%s will be ignored' % outputFile)
      outputData = newOutputData

    obtainedFiles = []
    for outputFile in outputData:
      self.log.info('Attempting to retrieve %s' % outputFile)
      result = self.getFile(outputFile, destDir=destinationDir)
      if not result['OK']:
        self.log.error('Failed to download %s' % outputFile)
        return result
      else:
        localPath = "%s/%s" % (destinationDir, os.path.basename(outputFile))
        obtainedFiles.append(os.path.realpath(localPath))

    if self.jobRepo:
      self.jobRepo.updateJob(jobID, {'OutputData': 1, 'OutputFiles': obtainedFiles})
    return S_OK(outputData)

  #############################################################################
  def selectJobs(self, status=None, minorStatus=None, applicationStatus=None,
                 site=None, owner=None, ownerGroup=None, jobGroup=None, date=None,
                 printErrors=True):
    """Options correspond to the web-page table columns. Returns the list of JobIDs for
       the specified conditions.  A few notes on the formatting:

         - date must be specified as yyyy-mm-dd.  By default, the date is today.
         - jobGroup corresponds to the name associated to a group of jobs, e.g. productionID / job names.
         - site is the DIRAC site name, e.g. LCG.CERN.ch
         - owner is the immutable nickname, e.g. paterson

       Example Usage:

         >>> dirac.selectJobs( status='Failed', owner='paterson', site='LCG.CERN.ch')
         {'OK': True, 'Value': ['25020', '25023', '25026', '25027', '25040']}

       :param status: Job status
       :type status: string
       :param minorStatus: Job minor status
       :type minorStatus: string
       :param applicationStatus: Job application status
       :type applicationStatus: string
       :param site: Job execution site
       :type site: string
       :param owner: Job owner
       :type owner: string
       :param jobGroup: Job group
       :type jobGroup: string
       :param date: Selection date
       :type date: string
       :returns: S_OK,S_ERROR
    """
    options = {'Status': status, 'MinorStatus': minorStatus, 'ApplicationStatus': applicationStatus, 'Owner': owner,
               'Site': site, 'JobGroup': jobGroup, 'OwnerGroup': ownerGroup}
    conditions = dict((key, str(value)) for key, value in options.items() if value)

    if date:
      try:
        date = str(date)
      except Exception as x:
        return self._errorReport(str(x), 'Expected yyyy-mm-dd string for date')
    else:
      date = '%s' % Time.date()
      self.log.verbose('Setting date to %s' % (date))

    self.log.verbose('Will select jobs with last update %s and following conditions' % date)
    self.log.verbose(self.pPrint.pformat(conditions))
    monitoring = JobMonitoringClient()
    result = monitoring.getJobs(conditions, date)
    if not result['OK']:
      if printErrors:
        self.log.warn(result['Message'])
    jobIDs = result['Value']
    self.log.verbose('%s job(s) selected' % (len(jobIDs)))
    if not printErrors:
      return result

    if not jobIDs:
      self.log.error("No jobs selected", "with date '%s' for conditions: %s" % (str(date), conditions))
      return S_ERROR("No jobs selected")
    return result

  #############################################################################
  def getJobSummary(self, jobID, outputFile=None, printOutput=False):
    """Output similar to the web page can be printed to the screen
       or stored as a file or just returned as a dictionary for further usage.

       Jobs can be specified individually or as a list.

       Example Usage:

       >>> dirac.getJobSummary(959209)
       {'OK': True, 'Value': {959209: {'Status': 'Staging', 'LastUpdateTime': '2008-12-08 16:43:18',
       'MinorStatus': '28 / 30', 'Site': 'Unknown', 'HeartBeatTime': 'None', 'ApplicationStatus': 'unknown',
       'JobGroup': '00003403', 'Owner': 'joel', 'SubmissionTime': '2008-12-08 16:41:38'}}}

       :param jobID: JobID
       :type jobID: int or string
       :param outputFile: Optional output file
       :type outputFile: string
       :param printOutput: Flag to print to stdOut
       :type printOutput: Boolean
       :returns: S_OK,S_ERROR
    """
    ret = self._checkJobArgument(jobID, multiple=True)
    if not ret['OK']:
      return ret
    jobID = ret['Value']

    headers = ['Status', 'MinorStatus', 'ApplicationStatus', 'Site', 'JobGroup', 'LastUpdateTime',
               'HeartBeatTime', 'SubmissionTime', 'Owner']

    monitoring = JobMonitoringClient()
    result = monitoring.getJobsSummary(jobID)
    if not result['OK']:
      self.log.warn(result['Message'])
      return result
    try:
      jobSummary = eval(result['Value'])
      # self.log.info(self.pPrint.pformat(jobSummary))
    except BaseException as x:
      self.log.warn('Problem interpreting result from job monitoring service')
      return S_ERROR('Problem while converting result from job monitoring')

    summary = {}
    for job in jobID:
      summary[job] = {}
      for key in headers:
        if job not in jobSummary:
          self.log.warn('No records for JobID %s' % job)
        value = jobSummary.get(job, {}).get(key, 'None')
        summary[job][key] = value

    if outputFile:
      if os.path.exists(outputFile):
        return self._errorReport('Output file %s already exists' % (outputFile))
      dirPath = os.path.basename(outputFile)
      if re.search('/', dirPath) and not os.path.exists(dirPath):
        try:
          os.mkdir(dirPath)
        except Exception as x:
          return self._errorReport(str(x), 'Could not create directory %s' % (dirPath))

      with open(outputFile, 'w') as fopen:
        line = 'JobID'.ljust(12)
        for i in headers:
          line += i.ljust(35)
        fopen.write(line + '\n')
        for jobID, params in summary.items():  # can be an iterator
          line = str(jobID).ljust(12)
          for header in headers:
            for key, value in params.items():  # can be an iterator
              if header == key:
                line += value.ljust(35)
          fopen.write(line + '\n')
      self.log.verbose('Output written to %s' % outputFile)

    if printOutput:
      print(self.pPrint.pformat(summary))

    return S_OK(summary)

  #############################################################################
  def getJobDebugOutput(self, jobID):
    """Developer function. Try to retrieve all possible outputs including
       logging information, job parameters, sandbox outputs, pilot outputs,
       last heartbeat standard output, JDL and CPU profile.

       Example Usage:

       >>> dirac.getJobDebugOutput(959209)
       {'OK': True, 'Value': '/afs/cern.ch/user/p/paterson/DEBUG_959209'}

       :param jobID: JobID
       :type jobID: int or string
       :returns: S_OK,S_ERROR
    """
    try:
      jobID = int(jobID)
    except ValueError as x:
      return self._errorReport(str(x), 'Expected integer or string for existing jobID')

    result = self.getJobStatus(jobID)
    if not result['OK']:
      self.log.info('Could not obtain status information for jobID %s, please check this is valid.' % jobID)
      return S_ERROR('JobID %s not found in WMS' % jobID)
    else:
      self.log.info('Job %s' % result['Value'])

    debugDir = '%s/DEBUG_%s' % (os.getcwd(), jobID)
    try:
      os.mkdir(debugDir)
    except OSError as x:
      return self._errorReport(str(x), 'Could not create directory in %s' % (debugDir))

    try:
      result = self.getOutputSandbox(jobID, '%s' % (debugDir))
      msg = []
      if not result['OK']:
        msg.append('Output Sandbox: Retrieval Failed')
      else:
        msg.append('Output Sandbox: Retrieved')
    except BaseException as x:
      msg.append('Output Sandbox: Not Available')

    try:
      result = self.getInputSandbox(jobID, '%s' % (debugDir))
      if not result['OK']:
        msg.append('Input Sandbox: Retrieval Failed')
      else:
        msg.append('Input Sandbox: Retrieved')
    except BaseException as x:
      msg.append('Input Sandbox: Not Available')

    try:
      result = self.getJobParameters(jobID)
      if not result['OK']:
        msg.append('Job Parameters: Retrieval Failed')
      else:
        self.__writeFile(result['Value'], '%s/JobParameters' % (debugDir))
        msg.append('Job Parameters: Retrieved')
    except BaseException as x:
      msg.append('Job Parameters: Not Available')

    try:
      result = self.peekJob(jobID)
      if not result['OK']:
        msg.append('Last Heartbeat StdOut: Retrieval Failed')
      else:
        self.__writeFile(result['Value'], '%s/LastHeartBeat' % (debugDir))
        msg.append('Last Heartbeat StdOut: Retrieved')
    except Exception as x:
      msg.append('Last Heartbeat StdOut: Not Available')

    try:
      result = self.getJobLoggingInfo(jobID)
      if not result['OK']:
        msg.append('Logging Info: Retrieval Failed')
      else:
        self.__writeFile(result['Value'], '%s/LoggingInfo' % (debugDir))
        msg.append('Logging Info: Retrieved')
    except Exception as x:
      msg.append('Logging Info: Not Available')

    try:
      result = self.getJobJDL(jobID)
      if not result['OK']:
        msg.append('Job JDL: Retrieval Failed')
      else:
        self.__writeFile(result['Value'], '%s/Job%s.jdl' % (debugDir, jobID))
        msg.append('Job JDL: Retrieved')
    except Exception as x:
      msg.append('Job JDL: Not Available')

    try:
      result = self.getJobCPUTime(jobID)
      if not result['OK']:
        msg.append('CPU Profile: Retrieval Failed')
      else:
        self.__writeFile(result['Value'], '%s/JobCPUProfile' % (debugDir))
        msg.append('CPU Profile: Retrieved')
    except Exception as x:
      msg.append('CPU Profile: Not Available')

    self.log.info('Summary of debugging outputs for job %s retrieved in directory:\n%s\n' % (jobID, debugDir),
                  '\n'.join(msg))
    return S_OK(debugDir)

  #############################################################################
  def __writeFile(self, pObject, fileName):
    """Internal function.  Writes a python object to a specified file path.
    """
    with open(fileName, 'w') as fopen:
      if not isinstance(pObject, six.string_types):
        fopen.write('%s\n' % self.pPrint.pformat(pObject))
      else:
        fopen.write(pObject)

  #############################################################################
  def getJobCPUTime(self, jobID, printOutput=False):
    """Retrieve job CPU consumed heartbeat data from job monitoring
       service.  Jobs can be specified individually or as a list.

       The time stamps and raw CPU consumed (s) are returned (if available).

       Example Usage:

       >>> d.getJobCPUTime(959209)
       {'OK': True, 'Value': {959209: {}}}

       :param jobID: JobID
       :type jobID: int or string
       :param printOutput: Flag to print to stdOut
       :type printOutput: Boolean
       :returns: S_OK,S_ERROR
    """
    ret = self._checkJobArgument(jobID, multiple=True)
    if not ret['OK']:
      return ret
    jobID = ret['Value']

    summary = {}
    for job in jobID:
      monitoring = JobMonitoringClient()
      result = monitoring.getJobHeartBeatData(job)
      summary[job] = {}
      if not result['OK']:
        return self._errorReport(result['Message'], 'Could not get heartbeat data for job %s' % job)
      if result['Value']:
        tupleList = result['Value']
        for tup in tupleList:
          if tup[0] == 'CPUConsumed':
            summary[job][tup[2]] = tup[1]
      else:
        self.log.warn('No heartbeat data for job %s' % job)

    if printOutput:
      print(self.pPrint.pformat(summary))

    return S_OK(summary)

  #############################################################################

  def getJobAttributes(self, jobID, printOutput=False):
    """Return DIRAC attributes associated with the given job.

       Each job will have certain attributes that affect the journey through the
       workload management system, see example below. Attributes are optionally
       printed to the screen.

       Example Usage:

       >>> print dirac.getJobAttributes(79241)
       {'AccountedFlag': 'False','ApplicationNumStatus': '0',
       'ApplicationStatus': 'Job Finished Successfully',
       'CPUTime': '0.0','DIRACSetup': 'LHCb-Production'}

       :param jobID: JobID
       :type jobID: int, str or python:list
       :param printOutput: Flag to print to stdOut
       :type printOutput: Boolean
       :returns: S_OK,S_ERROR
    """
    ret = self._checkJobArgument(jobID, multiple=False)
    if not ret['OK']:
      return ret
    jobID = ret['Value']

    monitoring = JobMonitoringClient()
    result = monitoring.getJobAttributes(jobID)
    if not result['OK']:
      return result

    if printOutput:
      print('=================\n', jobID)
      print(self.pPrint.pformat(result['Value']))

    return result

  #############################################################################

  def getJobParameters(self, jobID, printOutput=False):
    """Return DIRAC parameters associated with the given job.

       DIRAC keeps track of several job parameters which are kept in the job monitoring
       service, see example below. Selected parameters also printed to screen.

       Example Usage:

       >>> print dirac.getJobParameters(79241)
       {'OK': True, 'Value': {'JobPath': 'JobPath,JobSanity,JobPolicy,InputData,JobScheduling,TaskQueue',
       'JobSanityCheck': 'Job: 768 JDL: OK, InputData: 2 LFNs OK, '}

       :param jobID: JobID
       :type jobID: int or string
       :param printOutput: Flag to print to stdOut
       :type printOutput: Boolean
       :returns: S_OK,S_ERROR
    """
    ret = self._checkJobArgument(jobID, multiple=False)
    if not ret['OK']:
      return ret
    jobID = ret['Value']

    monitoring = JobMonitoringClient()
    result = monitoring.getJobParameters(jobID)
    if not result['OK']:
      return result

    result['Value'].get(jobID, {}).pop('StandardOutput', None)

    if printOutput:
      print(self.pPrint.pformat(result['Value']))

    if jobID in result['Value']:
      return S_OK(result['Value'][jobID])
    else:
      return S_ERROR('Failed to get job parameters for %s' % jobID)

  #############################################################################

  def getJobLoggingInfo(self, jobID, printOutput=False):
    """DIRAC keeps track of job transitions which are kept in the job monitoring
       service, see example below.  Logging summary also printed to screen at the
       INFO level.

       Example Usage:

       >>> print dirac.getJobLoggingInfo(79241)
       {'OK': True, 'Value': [('Received', 'JobPath', 'Unknown', '2008-01-29 15:37:09', 'JobPathAgent'),
       ('Checking', 'JobSanity', 'Unknown', '2008-01-29 15:37:14', 'JobSanityAgent')]}

       :param jobID: JobID
       :type jobID: int or string
       :param printOutput: Flag to print to stdOut
       :type printOutput: Boolean
       :returns: S_OK,S_ERROR
     """
    ret = self._checkJobArgument(jobID, multiple=False)
    if not ret['OK']:
      return ret
    jobID = ret['Value']

    monitoring = JobMonitoringClient()
    result = monitoring.getJobLoggingInfo(jobID)
    if not result['OK']:
      self.log.warn('Could not retrieve logging information for job %s' % jobID)
      self.log.warn(result)
      return result

    if printOutput:
      loggingTupleList = result['Value']

      fields = ['Source', 'Status', 'MinorStatus', 'ApplicationStatus', 'DateTime']
      records = []
      for l in loggingTupleList:
        records.append([l[i] for i in (4, 0, 1, 2, 3)])
      printTable(fields, records, numbering=False, columnSeparator='  ')

    return result

  #############################################################################

  def peekJob(self, jobID, printOutput=False):
    """The peek function will attempt to return standard output from the WMS for
       a given job if this is available.  The standard output is periodically
       updated from the compute resource via the application Watchdog. Available
       standard output is  printed to screen at the INFO level.

       Example Usage:

       >>> print dirac.peekJob(1484)
       {'OK': True, 'Value': 'Job peek result'}

       :param jobID: JobID
       :type jobID: int or string
       :returns: S_OK,S_ERROR
    """
    ret = self._checkJobArgument(jobID, multiple=False)
    if not ret['OK']:
      return ret
    jobID = ret['Value']

    monitoring = JobMonitoringClient()
    result = monitoring.getJobParameter(jobID, 'StandardOutput')
    if not result['OK']:
      return self._errorReport(result, 'Could not retrieve job attributes')

    stdout = result['Value'].get('StandardOutput')
    if stdout:
      if printOutput:
        self.log.notice(stdout)
      else:
        self.log.verbose(stdout)
    else:
      stdout = 'Not available yet.'
      self.log.info('No standard output available to print.')

    return S_OK(stdout)

  #############################################################################

  def pingService(self, system, service, printOutput=False, url=None):
    """The ping function will attempt to return standard information from a system
       service if this is available.  If the ping() command is unsuccessful it could
       indicate a period of service unavailability.

       Example Usage:

       >>> print dirac.pingService('WorkloadManagement','JobManager')
       {'OK': True, 'Value': 'Job ping result'}

       :param system: system
       :type system: string
       :param service: service name
       :type service: string
       :param printOutput: Flag to print to stdOut
       :type printOutput: Boolean
       :param url: url to ping (instad of system & service)
       :type url: string
       :returns: S_OK,S_ERROR
    """

    if not isinstance(system, six.string_types) \
        and isinstance(service, six.string_types) and \
            not isinstance(url, six.string_types):
      return self._errorReport('Expected string for system and service or a url to ping()')
    result = S_ERROR()
    try:
      if not url:
        systemSection = getSystemSection(system + '/')
        self.log.verbose('System section is: %s' % (systemSection))
        section = '%s/%s' % (systemSection, service)
        self.log.verbose('Requested service should have CS path: %s' % (section))
        serviceURL = getServiceURL('%s/%s' % (system, service))
        self.log.verbose('Service URL is: %s' % (serviceURL))
        client = RPCClient('%s/%s' % (system, service))
      else:
        serviceURL = url
        client = RPCClient(url)
      result = client.ping()
      if result['OK']:
        result['Value']['service url'] = serviceURL
    except Exception as x:
      self.log.warn('ping for %s/%s failed with exception:\n%s' % (system, service, str(x)))
      result['Message'] = str(x)

    if printOutput:
      print(self.pPrint.pformat(result))
    return result

  #############################################################################
  def getJobJDL(self, jobID, original=False, printOutput=False):
    """Simple function to retrieve the current JDL of an existing job in the
       workload management system.  The job JDL is converted to a dictionary
       and returned in the result structure.

       Example Usage:

       >>> print dirac.getJobJDL(12345)
       {'Arguments': 'jobDescription.xml',...}

       :param jobID: JobID
       :type jobID: int or string
       :returns: S_OK,S_ERROR

    """
    ret = self._checkJobArgument(jobID, multiple=False)
    if not ret['OK']:
      return ret
    jobID = ret['Value']

    monitoring = JobMonitoringClient()
    result = monitoring.getJobJDL(jobID, original)
    if not result['OK']:
      return result

    result = self.__getJDLParameters(result['Value'])
    if printOutput:
      print(self.pPrint.pformat(result['Value']))

    return result

  #############################################################################
  def __getJDLParameters(self, jdl):
    """ Internal function. Returns a dictionary of JDL parameters.

    :param jdl: a JDL
    :type jdl: ~DIRAC.Interfaces.API.Job.Job or str or file
    """
    self.log.debug("in __getJDLParameters")
    if hasattr(jdl, '_toJDL'):
      self.log.debug("jdl has a _toJDL method")
      jdl = jdl._toJDL()
    elif os.path.exists(jdl):
      self.log.debug("jdl %s is a file" % jdl)
      with open(jdl, 'r') as jdlFile:
        jdl = jdlFile.read()

    if not isinstance(jdl, six.string_types):
      return S_ERROR("Can't read JDL")

    try:
      parameters = {}
      if '[' not in jdl:
        jdl = '[' + jdl + ']'
      classAdJob = ClassAd(jdl)
      paramsDict = classAdJob.contents
      for param, value in paramsDict.items():  # can be an iterator
        if re.search('{', value):
          self.log.debug('Found list type parameter %s' % (param))
          rawValues = value.replace('{', '').replace('}', '').replace('"', '').replace('LFN:', '').split()
          valueList = []
          for val in rawValues:
            if re.search(',$', val):
              valueList.append(val[:-1])
            else:
              valueList.append(val)
          parameters[param] = valueList
        else:
          self.log.debug('Found standard parameter %s' % (param))
          parameters[param] = value.replace('"', '')
      return S_OK(parameters)
    except BaseException as x:
      self.log.exception(lException=x)
      return S_ERROR('Exception while extracting JDL parameters for job')

  #############################################################################
  def __printInfo(self):
    """Internal function to print the DIRAC API version and related information.
    """
    self.log.info('<=====%s=====>' % (self.diracInfo))
    self.log.verbose('DIRAC is running at %s in setup %s' % (DIRAC.siteName(), self.setup))

  def getConfigurationValue(self, option, default):
    """ Export the configuration client getValue() function
    """

    return gConfig.getValue(option, default)

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
