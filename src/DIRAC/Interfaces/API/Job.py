"""
   Job Base Class

   This class provides generic job definition functionality suitable for any VO.

   Helper functions are documented with example usage for the DIRAC API.  An example
   script (for a simple executable) would be::

     from DIRAC.Interfaces.API.Dirac import Dirac
     from DIRAC.Interfaces.API.Job import Job

     j = Job()
     j.setCPUTime(500)
     j.setExecutable('/bin/echo hello')
     j.setExecutable('yourPythonScript.py')
     j.setExecutable('/bin/echo hello again')
     j.setName('MyJobName')

     dirac = Dirac()
     jobID = dirac.submitJob(j)
     print 'Submission Result: ',jobID

   Note that several executables can be provided and wil be executed sequentially.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import re
import os
import shlex

import six
from six import StringIO
from six.moves.urllib.parse import quote as urlquote

from DIRAC import S_OK, gLogger
from DIRAC.Core.Base.API import API
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.Core.Workflow.Parameter import Parameter
from DIRAC.Core.Workflow.Workflow import Workflow
from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd
from DIRAC.Core.Utilities.Subprocess import systemCall
from DIRAC.Core.Utilities.List import uniqueElements
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOForGroup
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getCESiteMapping
from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC.Workflow.Utilities.Utils import getStepDefinition, addStepToWorkflow


COMPONENT_NAME = '/Interfaces/API/Job'


class Job(API):
  """ DIRAC jobs
  """

  #############################################################################

  def __init__(self, script=None, stdout='std.out', stderr='std.err'):
    """Instantiates the Workflow object and some default parameters.
    """

    super(Job, self).__init__()

    self.stepCount = 0
    self.owner = 'NotSpecified'
    self.name = 'Name'
    self.type = 'User'
    self.priority = 1
    vo = ''
    ret = getProxyInfo(disableVOMS=True)
    if ret['OK'] and 'group' in ret['Value']:
      vo = getVOForGroup(ret['Value']['group'])
    self.group = vo
    self.site = None
    self.stdout = stdout
    self.stderr = stderr
    self.logLevel = 'INFO'
    self.executable = 'dirac-jobexec'  # to be clarified
    self.addToInputSandbox = []
    self.addToOutputSandbox = []
    self.addToInputData = []
    # #Add member to handle Parametric jobs
    self.numberOfParameters = 0
    self.parameterSeqs = {}
    self.wfArguments = {}
    self.parametricWFArguments = {}

    # loading the function that will be used to determine the platform (it can be VO specific)
    res = ObjectLoader().loadObject("ConfigurationSystem.Client.Helpers.Resources", 'getDIRACPlatforms')
    if not res['OK']:
      self.log.fatal(res['Message'])
    self.getDIRACPlatforms = res['Value']

    self.script = script
    if not script:
      self.workflow = Workflow()
      self.__setJobDefaults()
    else:
      self.workflow = Workflow(script)

  #############################################################################

  def setExecutable(self, executable, arguments='', logFile='',
                    modulesList=None, parameters=None, paramValues=None):
    """Helper function.

       Specify executable script to run with optional arguments and log file
       for standard output.

       These can be either:

       - Submission of a python or shell script to DIRAC
          - Can be inline scripts e.g. C{'/bin/ls'}
          - Scripts as executables e.g. python or shell script file

       Example usage:

       >>> job = Job()
       >>> job.setExecutable('myScript.py')

       :param str executable: Executable
       :param str arguments: Optional arguments to executable
       :param str logFile: Optional log file name
       :param list modulesList: Optional list of modules (to be used mostly when extending this method)
       :param parameters: Optional list of parameters (to be used mostly when extending this method)
       :type parameters: python:list of tuples
       :param paramValues: Optional list of parameters values (to be used mostly when extending this method)
       :type parameters: python:list of tuples
    """
    kwargs = {'executable': executable, 'arguments': arguments, 'logFile': logFile}
    if not isinstance(executable, six.string_types) or not isinstance(arguments, six.string_types) or \
       not isinstance(logFile, six.string_types):
      return self._reportError('Expected strings for executable and arguments', **kwargs)

    if os.path.exists(executable):
      self.log.verbose('Found script executable file %s' % (executable))
      self.addToInputSandbox.append(executable)
      logName = '%s.log' % (os.path.basename(executable))
    else:
      self.log.warn('The executable code could not be found locally')
      logName = 'CodeOutput.log'

    self.stepCount += 1
    stepName = 'RunScriptStep%s' % (self.stepCount)

    if logFile:
      if isinstance(logFile, six.string_types):
        logName = str(logFile)
    else:
      logName = "Script%s_%s" % (self.stepCount, logName)

    if not modulesList:
      modulesList = ['Script']
    if not parameters:
      parameters = [('executable', 'string', '', "Executable Script"),
                    ('arguments', 'string', '', 'Arguments for executable Script'),
                    ('applicationLog', 'string', '', "Log file name")]

    step = getStepDefinition('ScriptStep%s' % (self.stepCount), modulesList, parametersList=parameters)
    self.addToOutputSandbox.append(logName)

    stepInstance = addStepToWorkflow(self.workflow, step, stepName)

    stepInstance.setValue('applicationLog', logName)
    stepInstance.setValue('executable', executable)
    if arguments:
      # If arguments are expressed in terms of parameters, pass them to Workflow
      # These arguments will be resolved on the server side for each parametric job
      if re.search(r'%\(.*\)s', arguments) or re.search('%n', arguments):
        self.parametricWFArguments['arguments'] = arguments
      else:
        stepInstance.setValue('arguments', arguments)
    if paramValues:
      for param, value in paramValues:
        stepInstance.setValue(param, value)

    return S_OK(stepInstance)

  #############################################################################
  def setName(self, jobName):
    """Helper function.

       A name for the job can be specified if desired. This will appear
       in the JobName field of the monitoring webpage. If nothing is
       specified a default value will appear.

       Example usage:

       >>> job=Job()
       >>> job.setName("myJobName")

       :param str jobName: Name of job
    """
    kwargs = {'jobname': jobName}
    if not isinstance(jobName, six.string_types):
      return self._reportError('Expected strings for job name', **kwargs)

    self.workflow.setName(jobName)
    self._addParameter(self.workflow, 'JobName', 'JDL', jobName, 'User specified name')

    return S_OK()

  #############################################################################
  def setInputSandbox(self, files):
    """Helper function.

       Specify input sandbox files less than 10MB in size.  If over 10MB, files
       or a directory may be uploaded to Grid storage, see C{dirac.uploadSandbox()}.

       Paths to the options file and (if required) 'lib/' directory of the DLLs
       are specified here. Default is local directory.
       Executables may be placed in the lib/ directory if desired. The lib/ directory
       is transferred to the Grid Worker Node before the job executes.

       Files / directories can be specified using the `*` character e.g. `*.txt`  these
       are resolved correctly before job execution on the WN.

       Example usage:

       >>> job = Job()
       >>> job.setInputSandbox(['DaVinci.opts'])

       :param files: Input sandbox files, can specify full path
       :type files: Single string or list of strings ['','']
    """
    if isinstance(files, list) and files:
      resolvedFiles = self._resolveInputSandbox(files)
      fileList = ';'.join(resolvedFiles)
      description = 'Input sandbox file list'
      self._addParameter(self.workflow, 'InputSandbox', 'JDL', fileList, description)
      # self.sandboxFiles=resolvedFiles
    elif isinstance(files, six.string_types):
      resolvedFiles = self._resolveInputSandbox([files])
      fileList = ';'.join(resolvedFiles)
      description = 'Input sandbox file'
      # self.sandboxFiles = [files]
      self._addParameter(self.workflow, 'InputSandbox', 'JDL', fileList, description)
    else:
      kwargs = {'files': files}
      return self._reportError('Expected file string or list of files for input sandbox contents', **kwargs)

    return S_OK()

  #############################################################################

  def setOutputSandbox(self, files):
    """Helper function.

       Specify output sandbox files. If specified files are over 10MB, these
       may be uploaded to Grid storage with a notification returned in the
       output sandbox.

       Example usage:

       >>> job = Job()
       >>> job.setOutputSandbox(['DaVinci_v19r12.log','DVNTuples.root'])

       :param files: Output sandbox files
       :type files: Single string or list of strings ['','']

    """
    if isinstance(files, list) and files:
      fileList = ';'.join(files)
      description = 'Output sandbox file list'
      self._addParameter(self.workflow, 'OutputSandbox', 'JDL', fileList, description)
    elif isinstance(files, six.string_types):
      description = 'Output sandbox file'
      self._addParameter(self.workflow, 'OutputSandbox', 'JDL', files, description)
    else:
      kwargs = {'files': files}
      return self._reportError('Expected file string or list of files for output sandbox contents', **kwargs)

    return S_OK()

  #############################################################################

  def setInputData(self, lfns):
    """Helper function.

       Specify input data by Logical File Name (LFN).

       Example usage:

       >>> job = Job()
       >>> job.setInputData(['/lhcb/production/DC04/v2/DST/00000742_00003493_10.dst'])

       :param lfns: Logical File Names
       :type lfns: Single LFN string or list of LFNs
    """
    if isinstance(lfns, list) and lfns:
      for i, _ in enumerate(lfns):
        lfns[i] = lfns[i].replace('LFN:', '')
      inputData = ['LFN:' + x for x in lfns]
      inputDataStr = ';'.join(inputData)
      description = 'List of input data specified by LFNs'
      self._addParameter(self.workflow, 'InputData', 'JDL', inputDataStr, description)
    elif isinstance(lfns, six.string_types):  # single LFN
      description = 'Input data specified by LFN'
      self._addParameter(self.workflow, 'InputData', 'JDL', lfns, description)
    else:
      kwargs = {'lfns': lfns}
      return self._reportError('Expected lfn string or list of lfns for input data', **kwargs)

    return S_OK()

  #############################################################################

  def setParameterSequence(self, name, parameterList, addToWorkflow=False):
    """ Function to define a sequence of values for parametric jobs.

    :param str name: sequence parameter name
    :param list parameterList: list of parameter values
    :param bool addToWorkflow: flag to add parameter to the workflow on the fly, if str, then
                               use as the workflow parameter
    :return: S_OK/S_ERROR
    """

    if self.numberOfParameters == 0:
      self.numberOfParameters = len(parameterList)
    elif self.numberOfParameters != len(parameterList):
      return self._reportError('Parameter sequences of different length',
                               name='setParameterSequence')

    self.parameterSeqs[name] = parameterList
    if addToWorkflow:
      if isinstance(addToWorkflow, six.string_types):
        self.wfArguments[name] = addToWorkflow
      else:
        self.wfArguments[name] = name

    return S_OK()

  #############################################################################
  def setInputDataPolicy(self, policy, dataScheduling=True):
    """Helper function.

       Specify a job input data policy, this takes precedence over any site specific or
       global settings.

       Possible values for policy are 'Download' or 'Protocol' (case-insensitive). This
       requires that the module locations are defined for the VO in the CS.

       Example usage:

       >>> job = Job()
       >>> job.setInputDataPolicy('download')

    """
    kwargs = {'policy': policy, 'dataScheduling': dataScheduling}
    csSection = 'InputDataPolicy'
    possible = ['Download', 'Protocol']
    finalPolicy = ''
    for value in possible:
      if policy.lower() == value.lower():
        finalPolicy = value

    if not finalPolicy:
      return self._reportError('Expected one of %s for input data policy' % (', '.join(possible)),
                               __name__, **kwargs)

    jobPolicy = Operations().getValue('%s/%s' % (csSection, finalPolicy), '')
    if not jobPolicy:
      return self._reportError('Could not get value for Operations option %s/%s' % (csSection, finalPolicy),
                               __name__, **kwargs)

    description = 'User specified input data policy'
    self._addParameter(self.workflow, 'InputDataPolicy', 'JDL', jobPolicy, description)

    if not dataScheduling and policy.lower() == 'download':
      self.log.verbose('Scheduling by input data is disabled, jobs will run anywhere and download input data')
      self._addParameter(self.workflow, 'DisableDataScheduling', 'JDL', 'True', 'Disable scheduling by input data')

    if not dataScheduling and policy.lower() != 'download':
      self.log.error('Expected policy to be "download" for bypassing data scheduling')
      return self._reportError('Expected policy to be "download" for bypassing data scheduling',
                               __name__, **kwargs)

    return S_OK()

  #############################################################################
  def setOutputData(self, lfns, outputSE=None, outputPath=''):
    """Helper function.

       For specifying output data to be registered in Grid storage.  If a list
       of OutputSEs are specified the job wrapper will try each in turn until
       successful.  If the OutputPath is specified this will appear only after
       / <VO> / user / <initial> / <username>
       directory.

       The output data can be LFNs or local file names.
       If they are LFNs they should be pre-prended by "LFN:",
       otherwise they will be interpreted as local files to be found.
       If local files are specified, then specifying the outputPath may become necessary,
       because if it's not specified then it will be constructed starting from the user name.

       Example usage:

       >>> job = Job()
       >>> job.setOutputData(['DVNtuple.root'])

       :param lfns: Output data file or files.
       :type lfns: Single string or list of strings ['','']
       :param outputSE: Optional parameter to specify the Storage Element to store data or files, e.g. CERN-tape
       :type outputSE: string or python:list
       :param outputPath: Optional parameter to specify part of the path in the storage (see above)
       :type outputPath: string

    """
    if outputSE is None:
      outputSE = []
    kwargs = {'lfns': lfns, 'OutputSE': outputSE, 'OutputPath': outputPath}
    if isinstance(lfns, list) and lfns:
      outputDataStr = ';'.join(lfns)
      description = 'List of output data files'
      self._addParameter(self.workflow, 'OutputData',
                         'JDL', outputDataStr, description)
    elif isinstance(lfns, six.string_types):
      description = 'Output data file'
      self._addParameter(self.workflow, 'OutputData', 'JDL', lfns, description)
    else:
      return self._reportError('Expected file name string or list of file names for output data', **kwargs)

    if outputSE:
      description = 'User specified Output SE'
      if isinstance(outputSE, six.string_types):
        outputSE = [outputSE]
      elif not isinstance(outputSE, list):
        return self._reportError('Expected string or list for OutputSE', **kwargs)
      outputSE = ';'.join(oSE.strip() for oSE in outputSE)
      self._addParameter(self.workflow, 'OutputSE',
                         'JDL', outputSE, description)

    if outputPath:
      description = 'User specified Output Path'
      if not isinstance(outputPath, six.string_types):
        return self._reportError('Expected string for OutputPath', **kwargs)
      # Remove leading "/" that might cause problems with os.path.join
      # This will prevent to set OutputPath outside the Home of the User
      while outputPath[0] == '/':
        outputPath = outputPath[1:]
      self._addParameter(self.workflow, 'OutputPath',
                         'JDL', outputPath, description)

    return S_OK()

  #############################################################################
  def setPlatform(self, platform):
    """Developer function: sets the target platform, e.g. Linux_x86_64_glibc-2.17.
       This platform is in the form of what it is returned by the dirac-platform script
       (or dirac-architecture if your extension provides it)
    """
    kwargs = {'platform': platform}

    if not isinstance(platform, six.string_types):
      return self._reportError("Expected string for platform", **kwargs)

    if not platform.lower() == 'any':
      availablePlatforms = self.getDIRACPlatforms()
      if not availablePlatforms['OK']:
        return self._reportError("Can't check for platform", **kwargs)
      if platform in availablePlatforms['Value']:
        self._addParameter(self.workflow, 'Platform', 'JDL',
                           platform, 'Platform ( Operating System )')
      else:
        return self._reportError("Invalid platform", **kwargs)

    return S_OK()

  #############################################################################
  def setSubmitPool(self, backend):
    """Developer function.

       Choose submission pool on which job is executed.
       Default in place for users.
    """
    # should add protection here for list of supported platforms
    kwargs = {'backend': backend}
    if not isinstance(backend, six.string_types):
      return self._reportError('Expected string for SubmitPool', **kwargs)

    if not backend.lower() == 'any':
      self._addParameter(self.workflow, 'SubmitPools', 'JDL', backend, 'Submit Pool')

    return S_OK()

  #############################################################################
  def setCPUTime(self, timeInSecs):
    """Helper function.

       Example usage:

       >>> job = Job()
       >>> job.setCPUTime(5000)

       :param timeInSecs: CPU time
       :type timeInSecs: int
    """
    kwargs = {'timeInSecs': timeInSecs}
    if not isinstance(timeInSecs, int):
      try:
        timeInSecs = int(timeInSecs)
      except ValueError:
        if not re.search('{{', timeInSecs):
          return self._reportError('Expected numerical string or int for CPU time in seconds', **kwargs)

    description = 'CPU time in secs'
    self._addParameter(self.workflow, 'CPUTime', 'JDL', timeInSecs, description)
    return S_OK()

  #############################################################################
  def setDestination(self, destination):
    """Helper function.

       Can specify a desired destination site or sites for job.  This can be useful
       for debugging purposes but often limits the possible candidate sites
       and overall system response time.

       Example usage:

       >>> job = Job()
       >>> job.setDestination('LCG.CERN.ch')

       :param destination: site string
       :type destination: str or python:list

       :return: S_OK/S_ERROR
    """
    kwargs = {'destination': destination}
    if isinstance(destination, six.string_types):
      destination = destination.replace(' ', '').split(',')
      description = 'User specified destination site'
    else:
      description = 'List of sites selected by user'
    if isinstance(destination, list):
      sites = set(site for site in destination if site.lower() != 'any')
      if sites:
        result = self._checkSiteIsValid(sites)
        if not result['OK']:
          return self._reportError('%s is not a valid destination site' % (destination), **kwargs)
      destSites = ';'.join(destination)
      self._addParameter(self.workflow, 'Site', 'JDL', destSites, description)
    else:
      return self._reportError('Invalid destination site, expected string or list', **kwargs)
    return S_OK()

  #############################################################################
  def setNumberOfProcessors(self, numberOfProcessors=None, minNumberOfProcessors=None, maxNumberOfProcessors=None):
    """Helper function.

       Example usage:

       >>> job = Job()
       >>> job.setNumberOfProcessors(numberOfProcessors=2)
       means that the job needs 2 processors

       >>> job = Job()
       >>> job.setNumberOfProcessors(minNumberOfProcessors=4, maxNumberOfProcessors=8)
       means that the job needs at least 4 processors, and that will use at most 8 processors

       >>> job = Job()
       >>> job.setNumberOfProcessors(minNumberOfProcessors=2)
       means that the job needs at least 2 processors, and that will use all the processors available

       >>> job = Job()
       >>> job.setNumberOfProcessors(minNumberOfProcessors=1)
       means that the job can run in SP mode, and that will use all the processors available
       (so the job could run MP, but also SP)

       >>> job = Job()
       >>> job.setNumberOfProcessors(maxNumberOfProcessors=4)
       is equivalent to
       >>> job.setNumberOfProcessors(minNumberOfProcessors=1, maxNumberOfProcessors=4)
       and it means that the job can run in SP mode, and that will use at most 4 processors
       (so the job could run MP, but also SP)

       >>> job = Job()
       >>> job.setNumberOfProcessors(minNumberOfProcessors=6, maxNumberOfProcessors=4)
       is a non-sense, and will lead to consider that the job can run exactly on 4 processors

       >>> job = Job()
       >>> job.setNumberOfProcessors(numberOfProcessors=3, maxNumberOfProcessors=4)
       will lead to ignore the second parameter

       >>> job = Job()
       >>> job.setNumberOfProcessors(numberOfProcessors=3, minNumberOfProcessors=2)
       will lead to ignore the second parameter

       :param int processors: number of processors required by the job (exact number, unless a min/max are set)
       :param int minNumberOfProcessors: optional min number of processors the job applications can use
       :param int maxNumberOfProcessors: optional max number of processors the job applications can use

       :return: S_OK/S_ERROR
    """
    if numberOfProcessors:
      if not minNumberOfProcessors:
        nProc = numberOfProcessors
      else:
        nProc = max(numberOfProcessors, minNumberOfProcessors)
      if nProc > 1:
        self._addParameter(self.workflow, 'NumberOfProcessors', 'JDL', nProc, "Exact number of processors requested")
        self._addParameter(self.workflow, 'MaxNumberOfProcessors', 'JDL', nProc,
                           "Max Number of processors the job applications may use")
      return S_OK()

    if maxNumberOfProcessors and not minNumberOfProcessors:
      minNumberOfProcessors = 1

    if minNumberOfProcessors and maxNumberOfProcessors and minNumberOfProcessors >= maxNumberOfProcessors:
      minNumberOfProcessors = maxNumberOfProcessors

    if minNumberOfProcessors and maxNumberOfProcessors \
        and minNumberOfProcessors == maxNumberOfProcessors \
            and minNumberOfProcessors > 1:
      self._addParameter(self.workflow, 'NumberOfProcessors', 'JDL',
                         minNumberOfProcessors, "Exact number of processors requested")
      self._addParameter(self.workflow, 'MaxNumberOfProcessors', 'JDL',
                         minNumberOfProcessors, "Max Number of processors the job applications may use")
      return S_OK()

    # By this point there should be a min
    self._addParameter(self.workflow, 'MinNumberOfProcessors', 'JDL', minNumberOfProcessors,
                       "Min Number of processors the job applications may use")

    # If not set, will be "all"
    if maxNumberOfProcessors:
      self._addParameter(self.workflow, 'MaxNumberOfProcessors', 'JDL', maxNumberOfProcessors,
                         "Max Number of processors the job applications may use")

    return S_OK()

  #############################################################################
  def setDestinationCE(self, ceName, diracSite=None):
    """ Developer function.

        Allows to direct a job to a particular Grid CE.
    """
    kwargs = {'ceName': ceName}

    if not diracSite:
      res = getCESiteMapping(ceName)
      if not res['OK']:
        return self._reportError(res['Message'], **kwargs)
      if not res['Value']:
        return self._reportError('No DIRAC site name found for CE %s' % (ceName), **kwargs)
      diracSite = res['Value'][ceName]

    self.setDestination(diracSite)
    self._addJDLParameter('GridCE', ceName)
    return S_OK()

  #############################################################################
  def setBannedSites(self, sites):
    """Helper function.

       Can specify a desired destination site for job.  This can be useful
       for debugging purposes but often limits the possible candidate sites
       and overall system response time.

       Example usage:

       >>> job = Job()
       >>> job.setBannedSites(['LCG.GRIDKA.de','LCG.CNAF.it'])

       :param sites: single site string or list
       :type sites: str or python:list
    """
    if isinstance(sites, list) and sites:
      bannedSites = ';'.join(sites)
      description = 'List of sites excluded by user'
      self._addParameter(self.workflow, 'BannedSites', 'JDL', bannedSites, description)
    elif isinstance(sites, six.string_types):
      description = 'Site excluded by user'
      self._addParameter(self.workflow, 'BannedSites', 'JDL', sites, description)
    else:
      kwargs = {'sites': sites}
      return self._reportError('Expected site string or list of sites', **kwargs)
    return S_OK()

  #############################################################################
  def setOwner(self, ownerProvided):
    """Developer function.

       Normally users should always specify their immutable DIRAC nickname.
    """
    if not isinstance(ownerProvided, six.string_types):
      return self._reportError('Expected string for owner', **{'ownerProvided': ownerProvided})

    self._addParameter(self.workflow, 'Owner', 'JDL', ownerProvided, 'User specified ID')
    return S_OK()

  #############################################################################
  def setOwnerGroup(self, ownerGroup):
    """Developer function.

       Allows to force expected owner group of proxy.
    """
    if not isinstance(ownerGroup, six.string_types):
      return self._reportError('Expected string for job owner group', **{'ownerGroup': ownerGroup})

    self._addParameter(self.workflow, 'OwnerGroup', 'JDL', ownerGroup, 'User specified owner group.')
    return S_OK()

  #############################################################################
  def setOwnerDN(self, ownerDN):
    """Developer function.

       Allows to force expected owner DN of proxy.
    """
    if not isinstance(ownerDN, six.string_types):
      return self._reportError('Expected string for job owner DN', **{'ownerGroup': ownerDN})

    self._addParameter(self.workflow, 'OwnerDN', 'JDL', ownerDN, 'User specified owner DN.')
    return S_OK()

  #############################################################################
  def setType(self, jobType):
    """Developer function.

       Specify job type for testing purposes.
    """
    if not isinstance(jobType, six.string_types):
      return self._reportError('Expected string for job type', **{'jobType': jobType})

    self._addParameter(self.workflow, 'JobType', 'JDL', jobType, 'User specified type')
    self.type = jobType
    return S_OK()

  #############################################################################
  def setTag(self, tags):
    """ Set the Tags job requirements

        Example usage:

        >>> job = Job()
        >>> job.setTag( ['WholeNode','8GBMemory'] )

        :param tags: single tag string or a list of tags
        :type tags: str or python:list
    """

    if isinstance(tags, six.string_types):
      tagValue = tags
    elif isinstance(tags, list):
      tagValue = ";".join(tags)
    else:
      return self._reportError('Expected string or list for job tags', tags=tags)

    self._addParameter(self.workflow, 'Tags', 'JDL', tagValue, 'User specified job tags')
    return S_OK()

  #############################################################################
  def setJobGroup(self, jobGroup):
    """Helper function.

       Allows to group certain jobs according to an ID.

       Example usage:

       >>> job = Job()
       >>> job.setJobGroup('Bs2JPsiPhi')

       :param jobGroup: JobGroup name
       :type jobGroup: string
    """
    if not isinstance(jobGroup, six.string_types):
      return self._reportError('Expected string for job group name', **{'jobGroup': jobGroup})

    description = 'User specified job group'
    self._addParameter(self.workflow, 'JobGroup', 'JDL', jobGroup, description)
    return S_OK()

  #############################################################################
  def setLogLevel(self, logLevel):
    """Helper function.

       Optionally specify a DIRAC logging level for the job, e.g.
       ALWAYS, INFO, VERBOSE, WARN, DEBUG
       by default this is set to the info level.

       Example usage:

       >>> job = Job()
       >>> job.setLogLevel('debug')

       :param logLevel: Logging level
       :type logLevel: string
    """
    kwargs = {'logLevel': logLevel}
    if isinstance(logLevel, six.string_types):
      if logLevel.upper() in gLogger.getAllPossibleLevels():
        description = 'User specified logging level'
        self.logLevel = logLevel
        self._addParameter(self.workflow, 'LogLevel', 'JDL', logLevel, description)
      else:
        return self._reportError('Error Level "%s" not valid' % logLevel, **kwargs)
    else:
      return self._reportError('Expected string for logging level', **kwargs)
    return S_OK()

  #############################################################################
  def setConfigArgs(self, cfgString):
    """Developer function. Allow to pass arbitrary settings to the payload
       configuration service environment.
    """
    if not isinstance(cfgString, six.string_types):
      return self._reportError('Expected string for DIRAC Job Config Args', **{'cfgString': cfgString})

    description = 'User specified cfg settings'
    self._addParameter(self.workflow, 'JobConfigArgs', 'JDL', cfgString, description)
    return S_OK()

  #############################################################################
  def setExecutionEnv(self, environmentDict):
    """Helper function.

       Optionally specify a dictionary of key, value pairs to be set before
       the job executes e.g. {'MYVAR':3}

       The standard application environment variables are always set so this
       is intended for user variables only.

       Example usage:

       >>> job = Job()
       >>> job.setExecutionEnv({'<MYVARIABLE>':'<VALUE>'})

       :param environmentDict: Environment variables
       :type environmentDict: dictionary
    """
    kwargs = {'environmentDict': environmentDict}
    if not isinstance(environmentDict, dict):
      return self._reportError('Expected dictionary of environment variables', **kwargs)

    if environmentDict:
      environment = []
      for var, val in environmentDict.items():
        try:
          environment.append('='.join([str(var), urlquote(str(val))]))
        except Exception:
          return self._reportError('Expected string for environment variable key value pairs', **kwargs)

      envStr = ';'.join(environment)
      description = 'Env vars specified by user'
      self._addParameter(self.workflow, 'ExecutionEnvironment', 'JDL', envStr, description)
    return S_OK()

  #############################################################################
  def execute(self):
    """Developer function. Executes the job locally.
    """
    self.workflow.createCode()
    self.workflow.execute()

  #############################################################################
  def _getParameters(self):
    """Developer function.
       Method to return the workflow parameters.
    """
    wfParams = {}
    params = self.workflow.parameters
    for par in params:
      wfParams[par.getName()] = par.getValue()
    return wfParams

  #############################################################################

  def __setJobDefaults(self):
    """Set job default values. Note that the system configuration is set to "ANY".
    """
    self._addParameter(self.workflow, 'JobType', 'JDL', self.type, 'Job Type')
    self._addParameter(self.workflow, 'Priority', 'JDL', self.priority, 'User Job Priority')
    self._addParameter(self.workflow, 'JobGroup', 'JDL', self.group, 'Name of the JobGroup')
    self._addParameter(self.workflow, 'JobName', 'JDL', self.name, 'Name of Job')
    self._addParameter(self.workflow, 'StdOutput', 'JDL', self.stdout, 'Standard output file')
    self._addParameter(self.workflow, 'StdError', 'JDL', self.stderr, 'Standard error file')
    self._addParameter(self.workflow, 'InputData', 'JDL', '', 'Default null input data value')
    self._addParameter(self.workflow, 'LogLevel', 'JDL', self.logLevel, 'Job Logging Level')
    self._addParameter(self.workflow, 'arguments', 'string', '', 'Arguments to executable Step')
    # Those 2 below are need for on-site resolution
    self._addParameter(self.workflow, 'ParametricInputData', 'string', '',
                       'Default null parametric input data value')
    self._addParameter(self.workflow, 'ParametricInputSandbox', 'string', '',
                       'Default null parametric input sandbox value')

  #############################################################################

  @staticmethod
  def _addParameter(wObject, name, ptype, value, description, io='input'):
    """ Internal Function

        Adds a parameter to the object.
    """
    if io == 'input':
      inBool = True
      outBool = False
    elif io == 'output':
      inBool = False
      outBool = True
    else:
      raise TypeError('I/O flag is either input or output')

    par = Parameter(name, value, ptype, "", "", inBool, outBool, description)
    wObject.addParameter(Parameter(parameter=par))

  ############################################################################
  def _resolveInputSandbox(self, inputSandbox):
    """ Internal function.

        Resolves wildcards for input sandbox files.  This is currently linux
        specific and should be modified.
    """
    resolvedIS = []
    for i in inputSandbox:
      if not re.search(r'\*', i):
        if not os.path.isdir(i):
          resolvedIS.append(i)

    for name in inputSandbox:
      if re.search(r'\*', name):  # escape the star character...
        cmd = 'ls -d ' + name
        output = systemCall(10, shlex.split(cmd))
        if not output['OK']:
          self.log.error('Could not perform: ', cmd)
        elif output['Value'][0]:
          self.log.error(" Failed getting the files ", output['Value'][2])
        else:
          files = output['Value'][1].split()
          for check in files:
            if os.path.isfile(check):
              self.log.verbose('Found file ' + check + ' appending to Input Sandbox')
              resolvedIS.append(check)
            if os.path.isdir(check):
              if re.search('/$', check):  # users can specify e.g. /my/dir/lib/
                check = check[:-1]
              tarName = os.path.basename(check)
              directory = os.path.dirname(check)  # if just the directory this is null
              if directory:
                cmd = 'tar cfz ' + tarName + '.tar.gz ' + ' -C ' + directory + ' ' + tarName
              else:
                cmd = 'tar cfz ' + tarName + '.tar.gz ' + tarName

              output = systemCall(60, shlex.split(cmd))
              if not output['OK']:
                self.log.error('Could not perform: %s' % (cmd))
              resolvedIS.append(tarName + '.tar.gz')
              self.log.verbose('Found directory ' + check + ', appending ' + check + '.tar.gz to Input Sandbox')

      if os.path.isdir(name):
        self.log.verbose('Found specified directory ' + name + ', appending ' + name + '.tar.gz to Input Sandbox')
        if re.search('/$', name):  # users can specify e.g. /my/dir/lib/
          name = name[:-1]
        tarName = os.path.basename(name)
        directory = os.path.dirname(name)  # if just the directory this is null
        if directory:
          cmd = 'tar cfz ' + tarName + '.tar.gz ' + ' -C ' + directory + ' ' + tarName
        else:
          cmd = 'tar cfz ' + tarName + '.tar.gz ' + tarName

        output = systemCall(60, shlex.split(cmd))
        if not output['OK']:
          self.log.error('Could not perform: %s' % (cmd))
        else:
          resolvedIS.append(tarName + '.tar.gz')

    return resolvedIS

  #############################################################################

  def _toXML(self):
    """ Returns an XML representation of itself as a Job.
    """
    return self.workflow.toXML()

  def _handleParameterSequences(self, paramsDict, arguments):

    for pName in self.parameterSeqs:
      if pName in paramsDict:
        if pName == "InputSandbox":
          if isinstance(paramsDict[pName]['value'], list):
            paramsDict[pName]['value'].append('%%(%s)s' % pName)
          elif isinstance(paramsDict[pName]['value'], six.string_types):
            if paramsDict[pName]['value']:
              paramsDict[pName]['value'] += ';%%(%s)s' % pName
            else:
              paramsDict[pName]['value'] = '%%(%s)s' % pName
        elif "jdl" in paramsDict[pName]['type'].lower():
          # If a parameter with the same name as the sequence name already exists
          # and is a list, then extend it by the sequence value. If it is not a
          # list, then replace it by the sequence value
          if isinstance(paramsDict[pName]['value'], list):
            currentParams = paramsDict[pName]['value']
            tmpList = []
            pData = self.parameterSeqs[pName]
            if isinstance(pData[0], list):
              for pElement in pData:
                tmpList.append(currentParams + pElement)
            else:
              for pElement in pData:
                tmpList.append(currentParams + [pElement])
            self.parameterSeqs[pName] = tmpList
          paramsDict[pName]['value'] = '%%(%s)s' % pName
      else:
        paramsDict[pName] = {}
        paramsDict[pName]['type'] = 'JDL'
        paramsDict[pName]['value'] = '%%(%s)s' % pName

      paramsDict['Parameters.%s' % pName] = {}
      paramsDict['Parameters.%s' % pName]['value'] = self.parameterSeqs[pName]
      paramsDict['Parameters.%s' % pName]['type'] = 'JDL'
      if pName in self.wfArguments:
        arguments.append(' -p %s=%%(%s)s' % (self.wfArguments[pName],
                                             pName))

    return paramsDict, arguments

  #############################################################################
  def _toJDL(self, xmlFile='', jobDescriptionObject=None):
    """ Creates a JDL representation of itself as a Job.

       Example usage:

       >>> job = Job()
       >>> job._toJDL()

       :param xmlFile: location of the XML file
       :type xmlFile: str
       :param jobDescriptionObject: if not None, it must be a StringIO object
       :type jobDescriptionObject: StringIO

       :returns: JDL (str)
    """
    # Check if we have to do old bootstrap...
    classadJob = ClassAd('[]')

    paramsDict = {}
    params = self.workflow.parameters  # ParameterCollection object

    paramList = params
    for param in paramList:
      paramsDict[param.getName()] = {'type': param.getType(), 'value': param.getValue()}

    arguments = []
    scriptName = 'jobDescription.xml'

    if jobDescriptionObject is None:
      # if we are here it's because there's a real file, on disk, that is named 'jobDescription.xml'
      # Messy but need to account for xml file being in /tmp/guid dir
      if self.script:
        if os.path.exists(self.script):
          scriptName = os.path.abspath(self.script)
          self.log.verbose('Found script name %s' % scriptName)
        else:
          self.log.warn("File not found", self.script)
      else:
        if xmlFile:
          if os.path.exists(xmlFile):
            self.log.verbose('Found XML File %s' % xmlFile)
            scriptName = xmlFile
          else:
            self.log.warn("File not found", xmlFile)
        else:
          if os.path.exists('jobDescription.xml'):
            scriptName = os.path.abspath('jobDescription.xml')
            self.log.verbose('Found script name %s' % scriptName)
          else:
            self.log.warn("Job description XML file not found")
      self.addToInputSandbox.append(scriptName)

    elif isinstance(jobDescriptionObject, StringIO):
      self.log.verbose("jobDescription is passed in as a StringIO object")

    else:
      self.log.error("Where's the job description?")

    arguments.append(os.path.basename(scriptName))
    if 'LogLevel' in paramsDict:
      if paramsDict['LogLevel']['value']:
        arguments.append('-o LogLevel=%s' % (paramsDict['LogLevel']['value']))
      else:
        self.log.warn('Job LogLevel defined with null value')
    if 'DIRACSetup' in paramsDict:
      if paramsDict['DIRACSetup']['value']:
        arguments.append('-o DIRAC/Setup=%s' % (paramsDict['DIRACSetup']['value']))
      else:
        self.log.warn('Job DIRACSetup defined with null value')
    if 'JobConfigArgs' in paramsDict:
      if paramsDict['JobConfigArgs']['value']:
        arguments.append('--cfg %s' % (paramsDict['JobConfigArgs']['value']))
      else:
        self.log.warn('JobConfigArgs defined with null value')
    if self.parametricWFArguments:
      for name, value in self.parametricWFArguments.items():
        arguments.append("-p %s='%s'" % (name, value))

    classadJob.insertAttributeString('Executable', self.executable)
    self.addToOutputSandbox.append(self.stderr)
    self.addToOutputSandbox.append(self.stdout)

    # Extract i/o sandbox parameters from steps and any input data parameters
    # to do when introducing step-level api...

    # To add any additional files to input and output sandboxes
    if self.addToInputSandbox:
      extraFiles = ';'.join(self.addToInputSandbox)
      if 'InputSandbox' in paramsDict:
        currentFiles = paramsDict['InputSandbox']['value']
        finalInputSandbox = currentFiles + ';' + extraFiles
        uniqueInputSandbox = uniqueElements(finalInputSandbox.split(';'))
        paramsDict['InputSandbox']['value'] = ';'.join(uniqueInputSandbox)
        self.log.verbose('Final unique Input Sandbox %s' % (';'.join(uniqueInputSandbox)))
      else:
        paramsDict['InputSandbox'] = {}
        paramsDict['InputSandbox']['value'] = extraFiles
        paramsDict['InputSandbox']['type'] = 'JDL'

    if self.addToOutputSandbox:
      extraFiles = ';'.join(self.addToOutputSandbox)
      if 'OutputSandbox' in paramsDict:
        currentFiles = paramsDict['OutputSandbox']['value']
        finalOutputSandbox = currentFiles + ';' + extraFiles
        uniqueOutputSandbox = uniqueElements(finalOutputSandbox.split(';'))
        paramsDict['OutputSandbox']['value'] = ';'.join(uniqueOutputSandbox)
        self.log.verbose('Final unique Output Sandbox %s' % (';'.join(uniqueOutputSandbox)))
      else:
        paramsDict['OutputSandbox'] = {}
        paramsDict['OutputSandbox']['value'] = extraFiles
        paramsDict['OutputSandbox']['type'] = 'JDL'

    if self.addToInputData:
      extraFiles = ';'.join(self.addToInputData)
      if 'InputData' in paramsDict:
        currentFiles = paramsDict['InputData']['value']
        finalInputData = extraFiles
        if currentFiles:
          finalInputData = currentFiles + ';' + extraFiles
        uniqueInputData = uniqueElements(finalInputData.split(';'))
        paramsDict['InputData']['value'] = ';'.join(uniqueInputData)
        self.log.verbose('Final unique Input Data %s' % (';'.join(uniqueInputData)))
      else:
        paramsDict['InputData'] = {}
        paramsDict['InputData']['value'] = extraFiles
        paramsDict['InputData']['type'] = 'JDL'

    # Handle parameter sequences
    if self.numberOfParameters > 0:
      paramsDict, arguments = self._handleParameterSequences(paramsDict, arguments)

    classadJob.insertAttributeString('Arguments', ' '.join(arguments))

    # Add any JDL parameters to classad obeying lists with ';' rule
    for name, props in paramsDict.items():
      ptype = props['type']
      value = props['value']
      if isinstance(value, six.string_types) and re.search(';', value):
        value = value.split(';')
      if name.lower() == 'requirements' and ptype == 'JDL':
        self.log.verbose('Found existing requirements: %s' % (value))

      if re.search('^JDL', ptype):
        if isinstance(value, list):
          if isinstance(value[0], list):
            classadJob.insertAttributeVectorStringList(name, value)
          else:
            classadJob.insertAttributeVectorInt(name, value)
        elif isinstance(value, six.string_types) and value:
          classadJob.insertAttributeInt(name, value)
        elif isinstance(value, six.integer_types + (float,)):
          classadJob.insertAttributeInt(name, value)

    if self.numberOfParameters > 0:
      classadJob.insertAttributeInt('Parameters', self.numberOfParameters)

    for fToBeRemoved in [scriptName, self.stdout, self.stderr]:
      try:
        self.addToInputSandbox.remove(fToBeRemoved)
      except ValueError:
        pass

    jdl = classadJob.asJDL()
    start = jdl.find('[')
    end = jdl.rfind(']')
    return jdl[(start + 1):(end - 1)]

  #############################################################################
  def _setParamValue(self, name, value):
    """Internal Function. Sets a parameter value, used for production.
    """
    return self.workflow.setValue(name, value)

  #############################################################################
  def _addJDLParameter(self, name, value):
    """Developer function, add an arbitrary JDL parameter.
    """
    self._addParameter(self.workflow, name, 'JDL', value, 'Optional JDL parameter added')
    return self.workflow.setValue(name, value)

  #############################################################################

  def runLocal(self, dirac=None):
    """ The dirac (API) object is for local submission.
    """

    if dirac is None:
      dirac = Dirac()

    return dirac.submitJob(self, mode='local')

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
