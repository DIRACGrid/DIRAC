########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Interfaces/API/Job.py,v 1.33 2008/06/19 09:00:35 paterson Exp $
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

__RCSID__ = "$Id: Job.py,v 1.33 2008/06/19 09:00:35 paterson Exp $"

import string, re, os, time, shutil, types, copy

from DIRAC.Core.Workflow.Parameter                  import *
from DIRAC.Core.Workflow.Module                     import *
from DIRAC.Core.Workflow.Step                       import *
from DIRAC.Core.Workflow.Workflow                   import *
from DIRAC.Core.Workflow.WorkflowReader             import *
from DIRAC.Core.Utilities.ClassAd.ClassAdLight      import ClassAd
from DIRAC.ConfigurationSystem.Client.Config        import gConfig
from DIRAC.Core.Utilities.Subprocess                import shellCall
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
    self.priority = 0
    self.group = 'lhcb'
    self.site = '' #ANY
    #self.setup = 'Development'
    self.origin = 'DIRAC'
    self.stdout = 'std.out'
    self.stderr = 'std.err'
    self.logLevel = 'info'
    self.executable = '$DIRACROOT/scripts/jobexec' # to be clarified
    self.addToInputSandbox = []
    self.addToOutputSandbox = []
    self.systemConfig = ''
    self.reqParams = {'MaxCPUTime':   'other.NAME>=VALUE',
                      'MinCPUTime':   'other.NAME<=VALUE',
                      'Site':         'other.NAME=="VALUE"',
                      'Platform':     'other.NAME=="VALUE"',
                      'SystemConfig': 'Member("VALUE",other.CompatiblePlatforms)'}

    self.script = script
    if not script:
      self.workflow = Workflow()
      self.__setJobDefaults()
    else:
      self.workflow = Workflow(script)

  #############################################################################
  def setExecutable(self,executable,logFile=''):
    """Helper function.

       Specify executable for DIRAC jobs.

       These can be either:

        - Submission of a python or shell script to DIRAC
           - Can be inline scripts e.g. C{'/bin/ls'}
           - Scripts as executables e.g. python or shell script file

       Example usage:

       >>> job = Job()
       >>> job.setExecutable('myScript.py')

       @param executable: Executable, can include path to file
       @type executable: string
       @param logFile: Optional log file name
       @type logFile: string
    """

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
    module =  self.__getScriptModule()

    moduleName = moduleName.replace('.','')
    stepNumber = self.stepCount
    stepDefn = 'ScriptStep%s' %(stepNumber)
    stepName = 'RunScriptStep%s' %(stepNumber)

    logPrefix = 'Script%s_' %(stepNumber)
    logName = '%s%s' %(logPrefix,logName)
    self.addToOutputSandbox.append(logName)

    step = StepDefinition(stepDefn)
    step.addModule(module)
    step.addParameter(module.findParameter('Executable'))
    step.addParameter(module.findParameter('Name'))
    step.addParameter(module.findParameter('LogFile'))

    moduleInstance = step.createModuleInstance('Script',moduleName)
    moduleInstance.setLink('Executable','self','Executable')
    moduleInstance.findParameter('Name').link('self','Name')
    moduleInstance.findParameter('LogFile').link('self','LogFile')

    output = moduleInstance.findParameter('Output')
    step.addParameter(Parameter(parameter=output))
    step.findParameter('Output').link(moduleName,'Output')

    self.workflow.addStep(step)
    stepPrefix = '%s_' % stepName
    self.workflow.addParameter(step.findParameter('Executable'),stepPrefix)
    self.workflow.addParameter(step.findParameter('Name'),stepPrefix)
    self.workflow.addParameter(step.findParameter('LogFile'),stepPrefix)
    self.workflow.addParameter(step.findParameter('Output'),stepPrefix)
    stepInstance = self.workflow.createStepInstance(stepDefn,stepName)

    scriptParams = ParameterCollection()
    scriptParams.append(moduleInstance.findParameter('Executable'))
    scriptParams.append(moduleInstance.findParameter('Name'))
    scriptParams.append(moduleInstance.findParameter('LogFile'))
    stepInstance.linkUp(scriptParams,stepPrefix)
    self.workflow.findParameter('%sOutput' %(stepPrefix)).link(stepInstance.getName(),'Output')
    self.workflow.setValue('%sExecutable' %(stepPrefix), executable)
    self.workflow.findParameter('%sName' %(stepPrefix)).setValue(moduleName)
    self.workflow.findParameter('%sLogFile' %(stepPrefix)).setValue(logName)

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
      self.sandboxFiles=resolvedFiles
    elif type(files) == type(" "):
      description = 'Input sandbox file'
      self.sandboxFiles = [files]
      self._addParameter(self.workflow,'InputSandbox','JDL',files,description)
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
       >>> job.setOutputSandbox(['DaVinci_v17r6.log','DVNTuples.root'])

       @param files: Output sandbox files
       @type files: Single string or list of strings ['','']

    """
    if type(files) == list and len(files):
      fileList = string.join(files,";")
      description = 'Output sandbox file list'
      self._addParameter(self.workflow,'OutputSandbox','JDL',fileList,description)
    elif type(files) == type(" "):
      description = 'Output sandbox file'
      self._addParameter(self.workflow,'OutputSandbox','JDL',[files],description)
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

       Choose platform (system) on which job is executed e.g. DIRAC, LCG.
       Default of LCG in place for users.
    """
    #should add protection here for list of supported platforms
    if type(backend) == type(" "):
      description = 'Platform type'
      if not backend.lower()=='any':
        self._addParameter(self.workflow,'Platform','JDLReqt',backend,description)
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
    if type(timeInSecs) == int:
      if timeInSecs:
        description = 'CPU time in secs'
        self._addParameter(self.workflow,'MaxCPUTime','JDLReqt',timeInSecs,description)
    else:
      raise TypeError,'Expected Integer for CPU time'

  #############################################################################
  def setDestination(self,destination):
    """Helper function.

       Can specify a desired destination site for job.  This can be useful
       for debugging purposes but often limits the possible candidate sites
       and overall system response time.

       Example usage:

       >>> job = Job()
       >>> job.setDestination('LCG.CERN.ch')

       @param destination: site string
       @type destination: string
    """

    if type(destination) == type("  "):
      description = 'User specified destination site'
      self._addParameter(self.workflow,'Site','JDLReqt',destination,description)
    else:
      raise TypeError,'Expected string for destination site'

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
      self._addParameter(self.workflow,'BannedSites','JDL',bannedSites,description)
    elif type(lfns)==type(" "):
      description = 'Site excluded by user'
      self._addParameter(self.workflow,'BannedSites','JDL',sites,description)
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
  def setSoftwareTags(self, tags):
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

       @param jobGroup: JobGroup name
       @type jobGroup: string
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
    try:
      self.owner = os.getlogin()
    except Exception, x :
      if os.environ.has_key('USER'):
        self.owner = os.environ['USER']
      else:
        self.owner = "Unknown"

    self._addParameter(self.workflow,'Owner','JDL',self.owner,'Job Owner')
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
  def __getScriptModule(self):
    """Internal function. This method controls the definition for a script module.
    """
    moduleName = 'Script'
    module = ModuleDefinition(moduleName)
    self._addParameter(module,'Name','Parameter','string','Name of executable')
    self._addParameter(module,'Executable','Parameter','string','Executable Script')
    self._addParameter(module,'LogFile','Parameter','string','Log file name')
    self._addParameter(module,'Output','Parameter','string','Script output string',io='output')
    module.setDescription('A module that can execute any provided code segment or script.')
    body = 'from WorkflowLib.Module.Script import Script\n'
    module.setBody(body)
    return module

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
        paramsDict['InputSandbox']['value'] = currentFiles+';'+extraFiles
        self.log.verbose('Final Input Sandbox %s' %(currentFiles+';'+extraFiles))
      else:
        paramsDict['InputSandbox'] = {}
        paramsDict['InputSandbox']['value']=extraFiles
        paramsDict['InputSandbox']['type']='JDL'

    if self.addToOutputSandbox:
      extraFiles = string.join(self.addToOutputSandbox,';')
      if paramsDict.has_key('OutputSandbox'):
        currentFiles = paramsDict['OutputSandbox']['value']
        paramsDict['OutputSandbox']['value'] = currentFiles+';'+extraFiles
        self.log.verbose('Final Output Sandbox %s' %(currentFiles+';'+extraFiles))
      else:
        paramsDict['OutputSandbox'] = {}
        paramsDict['OutputSandbox']['value']=extraFiles
        paramsDict['OutputSandbox']['type']='JDL'

    #Add any JDL parameters to classad obeying lists with ';' rule
    requirements = False
    for name,props in paramsDict.items():
      ptype = paramsDict[name]['type']
      value = paramsDict[name]['value']
      if name.lower()=='requirements' and ptype=='JDL':
        self.log.verbose('Found existing requirements: %s' %(value))
        requirements = True

      if re.search('^JDL',ptype):
        if not re.search(';',value):
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