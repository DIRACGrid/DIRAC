########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Interfaces/API/Job.py,v 1.51 2009/01/26 17:23:19 acasajus Exp $
# File :   Job.py
# Author : Stuart Paterson
########################################################################

"""
   Job Base Class

   This class provides generic job definition functionality suitable for any VO.

   Helper functions are documented with example usage for the DIRAC API.  An example
   script (for a simple executable) would be::

     from DIRAC.Interfaces.API.Dirac import Dirac
     from DIRAC.Interfaces.API.Job import Job

     j = Job()
     j.setCPUTime(500)
     j.setSystemConfig('slc4_ia32_gcc34')
     j.setExecutable('/bin/echo hello')
     j.setExecutable('yourPythonScript.py')
     j.setExecutable('/bin/echo hello again')
     j.setName('MyJobName')

     dirac = Dirac()
     jobID = dirac.submit(j)
     print 'Submission Result: ',jobID

   Note that several executables can be provided and wil be executed sequentially.
"""

__RCSID__ = "$Id: Job.py,v 1.51 2009/01/26 17:23:19 acasajus Exp $"

import string, re, os, time, shutil, types, copy

from DIRAC.Core.Workflow.Parameter                  import *
from DIRAC.Core.Workflow.Module                     import *
from DIRAC.Core.Workflow.Step                       import *
from DIRAC.Core.Workflow.Workflow                   import *
from DIRAC.Core.Workflow.WorkflowReader             import *
from DIRAC.Core.Utilities.ClassAd.ClassAdLight      import ClassAd
from DIRAC.ConfigurationSystem.Client.Config        import gConfig
from DIRAC.Core.Utilities.Subprocess                import shellCall
from DIRAC.Core.Utilities.List                      import uniqueElements
from DIRAC.Core.Utilities.SiteCEMapping             import getSiteCEMapping
from DIRAC                                          import gLogger

COMPONENT_NAME='/Interfaces/API/Job'

class Job:

  #############################################################################

  def __init__(self,script=None):
    """Instantiates the Workflow object and some default parameters.
    """
    self.log = gLogger
    self.section    = COMPONENT_NAME
    self.dbg = False
    if gConfig.getValue(self.section+'/LogLevel','DEBUG') == 'DEBUG':
      self.dbg = True

    self.defaultOutputSE = 'CERN-USER' # default SE CS location to be decided
    #gConfig.getValue('Tier0SE-tape','SEName')
    self.stepCount = 0
    self.owner = 'NotSpecified'
    self.name = 'Name'
    self.type = 'user'
    self.priority = 1
    self.group = 'lhcb'
    self.site = 'ANY' #ANY
    #self.setup = 'Development'
    self.origin = 'DIRAC'
    self.stdout = 'std.out'
    self.stderr = 'std.err'
    self.logLevel = 'info'
    self.executable = '$DIRACROOT/scripts/jobexec' # to be clarified
    self.addToInputSandbox = []
    self.addToOutputSandbox = []
    self.addToInputData = []
    self.systemConfig = ''
    self.reqParams = {'MaxCPUTime':   'other.NAME>=VALUE',
                      'MinCPUTime':   'other.NAME<=VALUE',
                      'Site':         'other.NAME=="VALUE"',
                      'Platform':     'other.NAME=="VALUE"',
                      #'BannedSites':  '!Member(other.Site,BannedSites)', #doesn't work unfortunately
                      'BannedSites':  'other.Site!="VALUE"',
                      'SystemConfig': 'Member("VALUE",other.CompatiblePlatforms)'}

    self.script = script
    if not script:
      self.workflow = Workflow()
      self.__setJobDefaults()
    else:
      self.workflow = Workflow(script)

  #############################################################################
  def setExecutable(self,executable,arguments='',logFile=''):
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

       @param executable: Executable
       @type executable: string
       @param arguments: Optional arguments to executable
       @type arguments: string
       @param logFile: Optional log file name
       @type logFile: string
    """
    if not type(executable) == type(' ') or not type(arguments) == type(' '):
      raise TypeError,'Expected strings for executable and arguments'

    if os.path.exists(executable):
      self.log.verbose('Found script executable file %s' % (executable))
      self.addToInputSandbox.append(executable)
      logName = os.path.basename(executable)+'.log'
      moduleName = os.path.basename(executable)
    else:
      self.log.verbose('Found executable code')
      logName = 'CodeOutput.log'
      moduleName = 'CodeSegment'

    if logFile:
      if type(logFile) == type(' '):
        logName = logFile

    self.stepCount +=1

    moduleName = moduleName.replace('.','')
    stepNumber = self.stepCount
    stepDefn = 'ScriptStep%s' %(stepNumber)
    step =  self.__getScriptStep(stepDefn)
    stepName = 'RunScriptStep%s' %(stepNumber)
    logPrefix = 'Script%s_' %(stepNumber)
    logName = '%s%s' %(logPrefix,logName)
    self.addToOutputSandbox.append(logName)
    self.workflow.addStep(step)

    # Define Step and its variables
    stepInstance = self.workflow.createStepInstance(stepDefn,stepName)
    stepInstance.setValue("name",moduleName)
    stepInstance.setValue("logFile",logName)
    stepInstance.setValue("executable",executable)
    if arguments:
      stepInstance.setValue("arguments",arguments)

  #############################################################################
  def setName(self,jobname):
    """Helper function.

       A name for the job can be specified if desired. This will appear
       in the JobName field of the monitoring webpage. If nothing is
       specified a default value will appear.

       Example usage:

       >>> job=Job()
       >>> job.setName("myJobName")

       @param jobname: Name of job
       @type jobname: string
    """
    if type(jobname)==type(" "):
      self.workflow.setName(jobname)
      self._addParameter(self.workflow,'JobName','JDL',jobname,'User specified name')
    else:
      raise TypeError,'Expected string for Job name'

  #############################################################################
  def setInputSandbox(self,files):
    """Helper function.

       Specify input sandbox files less than 10MB in size.  If over 10MB, files
       or a directory may be uploaded to Grid storage, see C{dirac.uploadSandbox()}.

       Paths to the options file and (if required) 'lib/' directory of the DLLs
       are specified here. Default is local directory.  CMT requirements files or
       executables may be placed in the lib/ directory if desired. The lib/ directory
       is transferred to the Grid Worker Node before the job executes.

       Files / directories can be specified using the '*' character e.g. *.txt  these
       are resolved correctly before job execution on the WN.

       Example usage:

       >>> job = Job()
       >>> job.setInputSandbox(['DaVinci.opts'])

       @param files: Input sandbox files, can specify full path
       @type files: Single string or list of strings ['','']
    """
    if type(files) == list and len(files):
      resolvedFiles = self._resolveInputSandbox(files)
      fileList = string.join(resolvedFiles,";")
      description = 'Input sandbox file list'
      self._addParameter(self.workflow,'InputSandbox','JDL',fileList,description)
      #self.sandboxFiles=resolvedFiles
    elif type(files) == type(" "):
      resolvedFiles = self._resolveInputSandbox([files])
      fileList = string.join(resolvedFiles,";")
      description = 'Input sandbox file'
      #self.sandboxFiles = [files]
      self._addParameter(self.workflow,'InputSandbox','JDL',fileList,description)
    else:
      raise TypeError,'Expected string or list for InputSandbox'

  #############################################################################
  def setOutputSandbox(self,files):
    """Helper function.

       Specify output sandbox files.  If specified files are over 10MB, these
       may be uploaded to Grid storage with a notification returned in the
       output sandbox.

       Example usage:

       >>> job = Job()
       >>> job.setOutputSandbox(['DaVinci_v19r12.log','DVNTuples.root'])

       @param files: Output sandbox files
       @type files: Single string or list of strings ['','']

    """
    if type(files) == list and len(files):
      fileList = string.join(files,";")
      description = 'Output sandbox file list'
      self._addParameter(self.workflow,'OutputSandbox','JDL',fileList,description)
    elif type(files) == type(" "):
      description = 'Output sandbox file'
      self._addParameter(self.workflow,'OutputSandbox','JDL',files,description)
    else:
      raise TypeError,'Expected string or list for OutputSandbox'

  #############################################################################
  def setInputData(self,lfns):
    """Helper function.

       Specify input data by Logical File Name (LFN).

       Example usage:

       >>> job = Job()
       >>> job.setInputData(['/lhcb/production/DC04/v2/DST/00000742_00003493_10.dst'])

       @param lfns: Logical File Names
       @type lfns: Single LFN string or list of LFNs
    """
    if type(lfns)==list and len(lfns):
      for i in xrange(len(lfns)):
        lfns[i] = lfns[i].replace('LFN:','')

      inputData = map( lambda x: 'LFN:'+x, lfns)
      inputDataStr = string.join(inputData,';')
      description = 'List of input data specified by LFNs'
      self._addParameter(self.workflow,'InputData','JDL',inputDataStr,description)
    elif type(lfns)==type(' '):  #single LFN
      description = 'Input data specified by LFN'
      self._addParameter(self.workflow,'InputData','JDL',lfns,description)
    else:
      raise TypeError,'Expected String or List'

  #############################################################################
  def setOutputData(self,lfns,OutputSE=None):
    """Helper function.

       For specifying output data to be registered in Grid storage
       (Tier-1 storage by default).

       Example usage:

       >>> job = Job()
       >>> job.setOutputData(['DVNtuple.root'])

       @param lfns: Output data file or files
       @type lfns: Single string or list of strings ['','']
       @param OutputSE: Optional parameter to specify the Storage
       Element to store data or files, e.g. CERN-tape
       @type OutputSE: string
    """
    if type(lfns)==list and len(lfns):
      outputDataStr = string.join(lfns,';')
      description = 'List of output data files'
      self._addParameter(self.workflow,'OutputData','JDL',outputDataStr,description)
    elif type(lfns)==type(" "):
      description = 'Output data file'
      self._addParameter(self.workflow,'OutputData','JDL',lfns,description)
    else:
      raise TypeError,'Expected string or list of output data files'

    if OutputSE:
      description = 'User specified Output SE'
      self._addParameter(self.workflow,'OutputSE','JDL',OutputSE,description)
    else:
      description = 'Default Output SE'
      self._addParameter(self.workflow,'OutputSE','JDL',self.defaultOutputSE,description)

  #############################################################################
  def setPlatform(self, backend):
    """Developer function.

       Choose submission pool on which job is executed e.g. DIRAC, LCG.
       Default in place for users.
    """
    #should add protection here for list of supported platforms
    if type(backend) == type(" "):
      description = 'Platform type'
      if not backend.lower()=='any':
        self._addParameter(self.workflow,'SubmitPool','JDL',backend,description)
    else:
      raise TypeError,'Expected string for platform'

  #############################################################################
  def setSystemConfig(self, config):
    """Helper function.

       Choose system configuration (e.g. where user DLLs have been compiled). Default ANY in place
       for user jobs.  Available system configurations can be browsed
       via dirac.checkSupportedPlatforms() method.

       Example usage:

       >>> job=Job()
       >>> job.setSystemConfig("slc4_ia32_gcc34")

       @param config: architecture, CMTCONFIG value
       @type config: string
    """
    if type(config) == type(" "):
      description = 'User specified system configuration for job'
      self._addParameter(self.workflow,'SystemConfig','JDLReqt',config,description)
      self.systemConfig = config
    else:
      raise TypeError,'Expected string for platform'

  #############################################################################
  def setCPUTime(self,timeInSecs):
    """Helper function.

       Under Development. Specify CPU time requirement in DIRAC units.

       Example usage:

       >>> job = Job()
       >>> job.setCPUTime(5000)

       @param timeInSecs: CPU time
       @type timeInSecs: Int
    """
    if not type(timeInSecs)==int:
      try:
        timeInSecs=int(timeInSecs)
      except Exception,x:
        raise TypeError,'Expected Integer for CPU time'

    description = 'CPU time in secs'
    self._addParameter(self.workflow,'MaxCPUTime','JDLReqt',timeInSecs,description)

  #############################################################################
  def setDestination(self,destination):
    """Helper function.

       Can specify a desired destination site or sites for job.  This can be useful
       for debugging purposes but often limits the possible candidate sites
       and overall system response time.

       Example usage:

       >>> job = Job()
       >>> job.setDestination('LCG.CERN.ch')

       @param destination: site string
       @type destination: string or list
    """

    if type(destination) == type("  "):
      if not re.search('^DIRAC.',destination) and not destination.lower()=='any':
        result = self.__checkSiteIsValid(destination)
        if not result['OK']:
          raise TypeError,'%s is not a valid destination site' %(destination)
      description = 'User specified destination site'
      self._addParameter(self.workflow,'Site','JDLReqt',destination,description)
    elif type(destination) == list:
      for site in destination:
        if not re.search('^DIRAC.',site) and not site.lower()=='any':
          result = self.__checkSiteIsValid(site)
          if not result['OK']:
            raise TypeError,'%s is not a valid destination site' %(destination)
      destSites = string.join(destination,';')
      description = 'List of sites selected by user'
      self._addParameter(self.workflow,'Site','JDLReqt',destSites,description)
    else:
      raise TypeError,'Expected string for destination site'

  #############################################################################
  def __checkSiteIsValid(self,site):
    """Internal function to check that a site name is valid.
    """
    sites = getSiteCEMapping()
    if not sites['OK']:
      return S_ERROR('Could not get site CE mapping')
    siteList = sites['Value'].keys()
    if not site in siteList:
      return S_ERROR('Specified site %s is not in list of defined sites' %site)

    return S_OK('%s is valid' %site)

  #############################################################################
  def setBannedSites(self,sites):
    """Helper function.

       Can specify a desired destination site for job.  This can be useful
       for debugging purposes but often limits the possible candidate sites
       and overall system response time.

       Example usage:

       >>> job = Job()
       >>> job.setBannedSites(['LCG.GRIDKA.de','LCG.CNAF.it'])

       @param sites: single site string or list
       @type sites: string or list
    """
    if type(sites)==list and len(sites):
      bannedSites = string.join(sites,';')
      description = 'List of sites excluded by user'
      self._addParameter(self.workflow,'BannedSites','JDLReqt',bannedSites,description)
    elif type(sites)==type(" "):
      description = 'Site excluded by user'
      self._addParameter(self.workflow,'BannedSites','JDLReqt',sites,description)
    else:
      raise TypeError,'Expected string or list of output data files'

  #############################################################################
  def setOwner(self, ownerProvided):
    """Developer function.

       Normally users should always specify their immutable DIRAC nickname.
    """
    if type(ownerProvided)==type("  "):
     # self._removeParameter(self.workflow,'Owner')
      self._addParameter(self.workflow,'Owner','JDL',ownerProvided,'User specified ID')
    else:
      raise TypeError,'Expected string for Job owner'

  #############################################################################
  def setOwnerGroup(self,ownerGroup):
    """Developer function.

       Allows to force expected owner group of proxy.
    """
    if type(ownerGroup)==type(" "):
      self._addParameter(self.workflow,'OwnerGroup','JDL',ownerGroup,'User specified owner group.')
    else:
      raise TypeError,'Expected string for Job owner group'

  #############################################################################
  def setType(self, jobType):
    """Developer function.

       Specify job type for testing purposes.
    """
    if type(jobType)==type("  "):
      #self._removeParameter(self.workflow,'JobType')
      self._addParameter(self.workflow,'JobType','JDL',jobType,'User specified type')
      self.type = jobType
    else:
      raise TypeError,'Expected string for Job type'

  #############################################################################
  def _setSoftwareTags(self, tags):
    """Helper function.

       Choose any software tags if desired.  These are not compulsory but will ensure jobs only
       arrive at an LCG site where the software is preinstalled.  Without the tags, missing software is
       installed automatically by the Job Agent.

       Example usage:

       >>> job=Job()
       >>> job.setSoftwareTags(['VO-lhcb-Brunel-v30r17','VO-lhcb-Boole-v12r10','VO-lhcb-Gauss-v25r12'])

       @param tags: software tags
       @type tags: string or list
    """
    if type(tags) == type(" "):
      self._addParameter(self.workflow,'SoftwareTag','JDL',tags,'VO software tag')
    elif type(tags) == list:
      swTags = string.join(tags,';')
      self._addParameter(self.workflow,'SoftwareTag','JDL',swTags,'List of VO software tags')
    else:
      raise TypeError,'Expected String or List of software tags'

  #############################################################################
  def setJobGroup(self,jobGroup):
    """Helper function.

       Allows to group certain jobs according to an ID.

       Example usage:

       >>> job = Job()
       >>> job.setJobGroup('Bs2JPsiPhi')

       @param jobGroup: JobGroup name
       @type jobGroup: string
    """

    if type(jobGroup) == type("  "):
      description = 'User specified job group'
      self._addParameter(self.workflow,'JobGroup','JDL',jobGroup,description)
    else:
      raise TypeError,'Expected string for job group'

  #############################################################################
  def setLogLevel(self,logLevel):
    """Helper function.

       Optionally specify a DIRAC logging level for the job, e.g.
       ALWAYS, INFO, VERBOSE, WARN, DEBUG
       by default this is set to the info level.

       Example usage:

       >>> job = Job()
       >>> job.setLogLevel('debug')

       @param logLevel: Logging level
       @type logLevel: string
    """
    #TODO: put protection for allowed logging levels...
    if type(logLevel) == type("  "):
      description = 'User specified logging level'
      self.logLevel = logLevel
      self._addParameter(self.workflow,'LogLevel','JDL',logLevel,description)
    else:
      raise TypeError,'Expected string for logging level'

  #############################################################################
  def setMode(self,mode):
    """Developer function. Under development.
    """
    if type(mode) == type("  "):
      description = 'Choose a different DIRAC job mode'
      self._addParameter(self.workflow,'JobMode','JDL',mode,description)
    else:
      raise TypeError,'Expected string for DIRAC Job Mode'

  #############################################################################
  def selectSetup(self,setup):
    """Developer function. Under development.
    """
    if type(setup) == type("  "):
      description = 'Choose a different DIRAC setup in which to execute the job'
      self._addParameter(self.workflow,'DIRACSetup','JDL',setup,description)
    else:
      raise TypeError,'Expected string for DIRAC setup'

  #############################################################################
  def setExecutionEnv(self,environmentDict):
    """Helper function.

       Optionally specify a dictionary of key, value pairs to be set before
       the job executes e.g. {'MYVAR':3}

       The standard application environment variables are always set so this
       is intended for user variables only.

       Example usage:

       >>> job = Job()
       >>> job.setExecutionEnviroment({'<MYVARIABLE>':'<VALUE>'})

       @param environmentDict: Environment variables
       @type environmentDict: dictionary
    """
    if not type(environmentDict)==type({}):
      raise TypeError,'Expected dictionary of environment variables'

    environment = []
    for var,val in environmentDict.items():
      try:
        environment.append(string.join([str(var),str(val)],'='))
      except Exception,x:
        raise TypeError,'Expected string for environment variable key value pairs'

    envStr = string.join(environment,';')
    description = 'Env vars specified by user'
    self._addParameter(self.workflow,'ExecutionEnvironment','JDL',envStr,description)

  #############################################################################
  def sendMail(self):
    """Under development.
    """
    description = 'Optional flag to send email when jobs complete'
    self._addParameter(self.workflow,'SendMail','JDL','True',description)

  #############################################################################
  def createCode(self):
    """Developer function. Wrapper method to create the code.
    """
    return self.workflow.createCode()

  #############################################################################
  def execute(self):
    """Developer function. Executes the job locally.
    """
    code = self.createCode()
    #eval(compile(code,'<string>','exec'))
    self.workflow.execute()

  #############################################################################

  def _dumpParameters(self,showType=None):
    """Developer function.
       Method to print the workflow parameters.
    """
    paramsDict = {}
    paramList = self.workflow.parameters
    for param in paramList:
      paramsDict[param.getName()]= {'type':param.getType(),'value':param.getValue()}
    self.log.info('--------------------------------------')
    self.log.info('Workflow parameter summary:           ')
    self.log.info('--------------------------------------')
    #print self.workflow.parameters
    #print params.getParametersNames()
    for name,props in paramsDict.items():
      ptype = paramsDict[name]['type']
      value = paramsDict[name]['value']
      if showType:
        if ptype==showType:
          self.log.info('NAME: %s\nTYPE: %s\nVALUE: %s ' %(name,ptype,value))
          self.log.info('--------------------------------------')
      else:
        self.log.info('NAME: %s\nTYPE: %s\nVALUE: %s ' %(name,ptype,value))
        self.log.info('--------------------------------------')

  #############################################################################

  def __setJobDefaults(self):
    """Set job default values.  For initial version still using local account string
    for a nickname.
    """
#    try:
#      self.owner = os.getlogin()
#    except Exception, x :
#      if os.environ.has_key('USER'):
#        self.owner = os.environ['USER']
#      else:
#        self.owner = "Unknown"

#    self._addParameter(self.workflow,'Owner','JDL',self.owner,'Job Owner')
    self._addParameter(self.workflow,'JobType','JDL',self.type,'Job Type')
    self._addParameter(self.workflow,'Priority','JDL',self.priority,'User Job Priority')
    self._addParameter(self.workflow,'JobGroup','JDL',self.group,'Corresponding VOMS role')
    self._addParameter(self.workflow,'JobName','JDL',self.name,'Name of Job')
    #self._addParameter(self.workflow,'DIRACSetup','JDL',self.setup,'DIRAC Setup')
    self._addParameter(self.workflow,'Site','JDL',self.site,'Site Requirement')
    self._addParameter(self.workflow,'Origin','JDL',self.origin,'Origin of client')
    self._addParameter(self.workflow,'StdOutput','JDL',self.stdout,'Standard output file')
    self._addParameter(self.workflow,'StdError','JDL',self.stderr,'Standard error file')
    self._addParameter(self.workflow,'InputData','JDL','','Default null input data value')
    self._addParameter(self.workflow,'LogLevel','JDL',self.logLevel,'Job Logging Level')

  #############################################################################
 # def _addStep(self,step):
 #   """Add step to workflow.
 #   """
    #to do

 #   self.workflow.addStep(step)
  #############################################################################
#  def _addModule(self,module,step = 0):
    #to do
 #   self.workflow.addModule(module)

  #############################################################################
  def _addParameter(self,object,name,ptype,value,description,io='input'):
    """ Internal Function

        Adds a parameter to the object.
    """
    if io=='input':
      inBool = True
      outBool = False
    elif io=='output':
      inBool = False
      outBool = True
    else:
      raise TypeError,'I/O flag is either input or output'

    p = Parameter(name,value,ptype,"","",inBool,outBool,description)
    object.addParameter(Parameter(parameter=p))
    return p

  ############################################################################
  def _resolveInputSandbox(self, inputSandbox):
    """ Internal function.

        Resolves wildcards for input sandbox files.  This is currently linux
        specific and should be modified.
    """
    resolvedIS = []
    for i in inputSandbox:
      if not re.search('\*',i):
        if not os.path.isdir(i):
          resolvedIS.append(i)

    for f in inputSandbox:
      if re.search('\*',f): #escape the star character...
        cmd = 'ls -d '+f
        output = shellCall(10,cmd)
        if not output['OK']:
          self.log.error('Could not perform: %s' %(cmd))
        else:
          files = string.split(output['Value'])
          for check in files:
            if os.path.isfile(check):
              self.log.verbose('Found file '+check+' appending to Input Sandbox')
              resolvedIS.append(check)
            if os.path.isdir(check):
              if re.search('/$',check): #users can specify e.g. /my/dir/lib/
                 check = check[:-1]
              tarname = os.path.basename(check)
              directory = os.path.dirname(check) #if just the directory this is null
              if directory:
                cmd = 'tar cfz '+tarname+'.tar.gz '+' -C '+directory+' '+tarname
              else:
                cmd = 'tar cfz '+tarname+'.tar.gz '+tarname

              output = shellCall(60,cmd)
              if not output['OK']:
                self.log.error('Could not perform: %s' %(cmd))
              resolvedIS.append(tarname+'.tar.gz')
              self.log.verbose('Found directory '+check+', appending '+check+'.tar.gz to Input Sandbox')

      if os.path.isdir(f):
        self.log.verbose('Found specified directory '+f+', appending '+f+'.tar.gz to Input Sandbox')
        if re.search('/$',f): #users can specify e.g. /my/dir/lib/
           f = f[:-1]
        tarname = os.path.basename(f)
        directory = os.path.dirname(f) #if just the directory this is null
        if directory:
          cmd = 'tar cfz '+tarname+'.tar.gz '+' -C '+directory+' '+tarname
        else:
          cmd = 'tar cfz '+tarname+'.tar.gz '+tarname

        output = shellCall(60,cmd)
        if not output['OK']:
          self.log.error('Could not perform: %s' %(cmd))
        else:
          resolvedIS.append(tarname+'.tar.gz')

    return resolvedIS

  #############################################################################
  def __getScriptStep(self,name='Script'):
    """Internal function. This method controls the definition for a script module.
    """
    # Create the script module first
    moduleName = 'Script'
    module = ModuleDefinition(moduleName)
    module.setDescription('A  script module that can execute any provided script.')
    body = 'from DIRAC.Core.Workflow.Modules.Script import Script\n'
    module.setBody(body)
    # Create Step definition
    step = StepDefinition(name)
    step.addModule(module)
    moduleInstance = step.createModuleInstance('Script',name)
    # Define step parameters
    step.addParameter(Parameter("name","","string","","",False, False,'Name of executable'))
    step.addParameter(Parameter("executable","","string","","",False, False, 'Executable Script'))
    step.addParameter(Parameter("arguments","","string","","",False, False, 'Arguments for executable Script'))
    step.addParameter(Parameter("logFile","","string","","",False,False,'Log file name'))
    return step

  #############################################################################
  def _toXML(self):
    """Creates an XML representation of itself as a Job.
    """
    return self.workflow.toXML()

  #############################################################################
  def _toJDL(self,xmlFile=''): #messy but need to account for xml file being in /tmp/guid dir
    """Creates a JDL representation of itself as a Job.
    """
    #Check if we have to do old bootstrap...
    classadJob = ClassAd('[]')

    paramsDict = {}
    params = self.workflow.parameters # ParameterCollection object

    paramList =  params
    for param in paramList:
      paramsDict[param.getName()]= {'type':param.getType(),'value':param.getValue()}

    scriptname = 'jobDescription.xml'
    arguments = []
    if self.script:
      if os.path.exists(self.script):
        scriptname = os.path.abspath(self.script)
        self.log.verbose('Found script name %s' %scriptname)
    else:
      if xmlFile:
        self.log.verbose('Found XML File %s' %xmlFile)
        scriptname = xmlFile

    arguments.append(os.path.basename(scriptname))
    self.addToInputSandbox.append(scriptname)
    if paramsDict.has_key('LogLevel'):
      if paramsDict['LogLevel']['value']:
        arguments.append('-o LogLevel=%s' %(paramsDict['LogLevel']['value']))
      else:
        self.log.warn('Job LogLevel defined with null value')
    if paramsDict.has_key('DIRACSetup'):
      if paramsDict['DIRACSetup']['value']:
        arguments.append('-o DIRAC/Setup=%s' %(paramsDict['DIRACSetup']['value']))
      else:
        self.log.warn('Job DIRACSetup defined with null value')
    if paramsDict.has_key('JobMode'):
      if paramsDict['JobMode']['value']:
        arguments.append('-o JobMode=%s' %(paramsDict['JobMode']['value']))
      else:
        self.log.warn('Job Mode defined with null value')

    classadJob.insertAttributeString('Arguments',string.join(arguments,' '))
    classadJob.insertAttributeString('Executable',self.executable)
    self.addToOutputSandbox.append(self.stderr)
    self.addToOutputSandbox.append(self.stdout)

    #Extract i/o sandbox parameters from steps and any input data parameters
    #to do when introducing step-level api...

    #To add any additional files to input and output sandboxes
    if self.addToInputSandbox:
      extraFiles = string.join(self.addToInputSandbox,';')
      if paramsDict.has_key('InputSandbox'):
        currentFiles = paramsDict['InputSandbox']['value']
        finalInputSandbox = currentFiles+';'+extraFiles
        uniqueInputSandbox = uniqueElements(finalInputSandbox.split(';'))
        paramsDict['InputSandbox']['value'] = string.join(uniqueInputSandbox,';')
        self.log.verbose('Final unique Input Sandbox %s' %(string.join(uniqueInputSandbox,';')))
      else:
        paramsDict['InputSandbox'] = {}
        paramsDict['InputSandbox']['value']=extraFiles
        paramsDict['InputSandbox']['type']='JDL'

    if self.addToOutputSandbox:
      extraFiles = string.join(self.addToOutputSandbox,';')
      if paramsDict.has_key('OutputSandbox'):
        currentFiles = paramsDict['OutputSandbox']['value']
        finalOutputSandbox = currentFiles+';'+extraFiles
        uniqueOutputSandbox = uniqueElements(finalOutputSandbox.split(';'))
        paramsDict['OutputSandbox']['value'] = string.join(uniqueOutputSandbox,';')
        self.log.verbose('Final unique Output Sandbox %s' %(string.join(uniqueOutputSandbox,';')))
      else:
        paramsDict['OutputSandbox'] = {}
        paramsDict['OutputSandbox']['value']=extraFiles
        paramsDict['OutputSandbox']['type']='JDL'

    if self.addToInputData:
      extraFiles = string.join(self.addToInputData,';')
      if paramsDict.has_key('InputData'):
        currentFiles = paramsDict['InputData']['value']
        finalInputData = extraFiles
        if currentFiles:
          finalInputData = currentFiles+';'+extraFiles
        uniqueInputData = uniqueElements(finalInputData.split(';'))
        paramsDict['InputData']['value'] = string.join(uniqueInputData,';')
        self.log.verbose('Final unique Input Data %s' %(string.join(uniqueInputData,';')))
      else:
        paramsDict['InputData'] = {}
        paramsDict['InputData']['value']=extraFiles
        paramsDict['InputData']['type']='JDL'

    #Add any JDL parameters to classad obeying lists with ';' rule
    requirements = False
    for name,props in paramsDict.items():
      ptype = paramsDict[name]['type']
      value = paramsDict[name]['value']
      if name.lower()=='requirements' and ptype=='JDL':
        self.log.verbose('Found existing requirements: %s' %(value))
        requirements = True

      if re.search('^JDL',ptype):
        if not re.search(';',value) or name=='GridRequirements': #not a nice fix...
          classadJob.insertAttributeString(name,value)
        else:
          classadJob.insertAttributeVectorString(name,string.split(value,';'))

    if not requirements:
      reqtsDict = self.reqParams
      exprn = ''
      plus = ''
      for name,props in paramsDict.items():
        ptype = paramsDict[name]['type']
        value = paramsDict[name]['value']
        if value and not value.lower()=='any':
          if ptype=='JDLReqt':
            plus = ' && '
            if re.search(';',value):
              for val in value.split(';'):
                exprn += reqtsDict[name].replace('NAME',name).replace('VALUE',str(val))+plus
            else:
              exprn += reqtsDict[name].replace('NAME',name).replace('VALUE',str(value))+plus

      if len(plus):
        exprn = exprn[:-len(plus)]
      if not exprn:
        exprn = 'true'
      self.log.verbose('Requirements: %s' %(exprn))
      classadJob.set_expression('Requirements', exprn)

    self.addToInputSandbox.remove(scriptname)
    self.addToOutputSandbox.remove(self.stdout)
    self.addToOutputSandbox.remove(self.stderr)
    jdl = classadJob.asJDL()
    start = string.find(jdl,'[')
    end   = string.rfind(jdl,']')
    return jdl[(start+1):(end-1)]

  #############################################################################
  def _setParamValue(self,name,value):
    """Internal Function. Sets a parameter value, used for production.
    """
    return self.workflow.setValue(name,value)

  #############################################################################
  def _addJDLParameter(self,name,value):
    """Developer function, add an arbitrary JDL parameter.
    """
    self._addParameter(self.workflow,name,'JDL',value,'Optional JDL parameter added')
    return self.workflow.setValue(name,value)

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#