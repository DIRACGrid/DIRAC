########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/LHCbDIRAC/trunk/LHCbDIRAC/Interfaces/API/LHCbJob.py $
# File :   LHCbJob.py
# Author : Stuart Paterson
########################################################################

"""LHCb Job Class

   The LHCb Job class inherits generic VO functionality from the Job base class
   and provides VO-specific functionality to aid in the construction of
   workflows.

   Helper functions are documented with example usage for the DIRAC API.

   Below are several examples of LHCbJob usage.

   An example DaVinci application script would be::

     from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb
     from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob

     j = LHCbJob()
     j.setCPUTime(5000)
     j.setSystemConfig('slc4_ia32_gcc34')
     j.setApplication('DaVinci','v19r12','DaVinciv19r12.opts',optionsLine='ApplicationMgr.EvtMax=1',inputData=['/lhcb/production/DC06/phys-v2-lumi2/00001650/DST/0000/00001650_00000054_5.dst'])
     j.setName('MyJobName')
     #j.setDestination('LCG.CERN.ch')

     dirac = DiracLHCb()
     jobID = dirac.submit(j)
     print 'Submission Result: ',jobID

   The setDestination() method is optional and takes the DIRAC site name as an argument.

   Another example for executing a script in the Gaudi Application environment is::

     from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb
     from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob

     j = LHCbJob()
     j.setCPUTime(5000)
     j.setSystemConfig('slc4_ia32_gcc34')
     j.setApplicationScript('DaVinci','v19r11','myGaudiPythonScript.py',inputData=['/lhcb/production/DC06/phys-lumi2/00001501/DST/0000/00001501_00000320_5.dst'])
     j.setName('MyJobName')
     #j.setDestination('LCG.CERN.ch')

     dirac = DiracLHCb()
     jobID = dirac.submit(j)
     print 'Submission Result: ',jobID

   For execution of a python Bender module::

     from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb
     from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob

     j = LHCbJob()
     j.setCPUTime(5000)
     j.setSystemConfig('slc4_ia32_gcc34')
     j.setBenderModule('v8r3','BenderExample.PhiMC',inputData=['LFN:/lhcb/production/DC06/phys-v2-lumi2/00001758/DST/0000/00001758_00000001_5.dst'],numberOfEvents=100)
     j.setName('MyJobName')

     dirac = DiracLHCb()
     jobID = dirac.submit(j)
     print 'Submission Result: ',jobID

   To execute a ROOT Macro, Python script and Executable consecutively an example script would be::

     from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb
     from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob

     j = LHCbJob()
     j.setCPUTime(50000)
     j.setSystemConfig('slc4_ia32_gcc34')
     j.setRootMacro('5.18.00a','test.C')
     j.setRootPythonScript('5.18.00a','test.py')
     j.setRootExecutable('5.18.00a','minexam')
     j.setLogLevel('verbose')

     dirac = DiracLHCb()
     jobID = dirac.submit(j,mode='local')
     print 'Submission Result: ',jobID

"""

__RCSID__ = "$Id: LHCbJob.py 19452 2009-12-10 21:14:49Z paterson $"

import string, re, os, time, shutil, types, copy

from DIRAC.Core.Workflow.Parameter                  import *
from DIRAC.Core.Workflow.Module                     import *
from DIRAC.Core.Workflow.Step                       import *
from DIRAC.Core.Workflow.Workflow                   import *
from DIRAC.Core.Workflow.WorkflowReader             import *
from DIRAC.Interfaces.API.Job                       import Job
from DIRAC.Core.Utilities.File                      import makeGuid
from DIRAC.Core.Utilities.List                      import uniqueElements
from DIRAC                                          import gConfig

COMPONENT_NAME='/WorkflowLib/API/LHCbJob'

class LHCbJob(Job):

  #############################################################################

  def __init__(self,script=None):
    """Instantiates the Workflow object and some default parameters.
    """
    Job.__init__(self,script)
    self.gaudiStepCount = 0
    self.currentStepPrefix = ''
    self.inputDataType = 'DATA' #Default, other options are MDF, ETC
    self.scratchDir = gConfig.getValue(self.section+'/LocalSite/ScratchDir','/tmp')
    self.rootSection = '/Operations/SoftwareDistribution/LHCbRoot'
    self.importLocation = 'LHCbDIRAC.Workflow.Modules'

  #############################################################################
  def setApplication(self,appName,appVersion,optionsFiles,inputData='',optionsLine='',inputDataType='',logFile=''):
    """Helper function.

       Specify application for DIRAC workflows.

       For LHCb these could be e.g. Gauss, Boole, Brunel,DaVinci, Bender, etc.

       The optionsFiles parameter can be the path to an options file or a list of paths to options files.
       All options files are automatically appended to the job input sandbox but the first in the case of a
       list is assumed to be the 'master' options file.

       Input data for application script must be specified here, please note that if this is set at the job level,
       e.g. via setInputData() but not above, input data is not in the scope of the specified application.

       Any input data specified at the step level that is not already specified at the job level is added automatically
       as a requirement for the job.

       Example usage:

       >>> job = LHCbJob()
       >>> job.setApplication('DaVinci','v19r5',optionsFiles='MyDV.opts',inputData=['/lhcb/production/DC06/phys-lumi2/00001501/DST/0000/00001501_00000320_5.dst'],logFile='dv.log')

       @param appName: Application name
       @type appName: string
       @param appVersion: Application version
       @type appVersion: string
       @param optionsFiles: Path to options file(s) for application
       @type optionsFiles: string or list
       @param inputData: Input data for application (if a subset of the overall input data for a given job is required)
       @type inputData: single LFN or list of LFNs
       @param optionsLine: Additional options lines for application
       @type optionsLine: string
       @param inputDataType: Input data type for application (e.g. DATA, MDF, ETC)
       @type inputDataType: string
       @param logFile: Optional log file name
       @type logFile: string
    """
    kwargs = {'appName':appName,'appVersion':appVersion,'optionsFiles':optionsFiles,'inputData':inputData,'optionsLine':optionsLine,'inputDataType':inputDataType,'logFile':logFile}
    if not type(appName) in types.StringTypes or not type(appVersion) in types.StringTypes:
      return self._reportError('Expected strings for application name and version',__name__,**kwargs)

    if logFile:
      if type(logFile) in types.StringTypes:
        logName = logFile
      else:
        return self._reportError('Expected string for log file name',__name__,**kwargs)
    else:
      logName = '%s_%s.log' %(appName,appVersion)

    if not type(inputDataType) in types.StringTypes:
      return self._reportError('Expected string for input data type',__name__,**kwargs)
    if not inputDataType:
      inputDataType=self.inputDataType

    optionsFile=None
    if not optionsFiles:
      return self._reportError('Expected string or list for optionsFiles',__name__,**kwargs)
    if type(optionsFiles) in types.StringTypes:
      optionsFiles = [optionsFiles]
    if not type(optionsFiles) == type([]):
      return self._reportError('Expected string or list for optionsFiles',__name__,**kwargs)
    for optsFile in optionsFiles:
      if not optionsFile:
        self.log.verbose('Found master options file %s' %optsFile)
        optionsFile = optsFile
      if os.path.exists(optsFile):
        self.log.verbose('Found specified options file: %s' %optsFile)
        self.addToInputSandbox.append(optsFile)
        optionsFile +=';%s' %optsFile
      elif re.search('\$',optsFile):
        self.log.verbose('Assuming %s is using an environment variable to be resolved during execution' %optsFile)
        if not optionsFile==optsFile:
          optionsFile +=';%s' %optsFile
      else:
        return self._reportError('Specified options file %s does not exist' %(optsFile),__name__,**kwargs)

    #ensure optionsFile list is unique:
    tmpList = string.split(optionsFile,';')
    optionsFile = string.join(uniqueElements(tmpList),';')
    self.log.verbose('Final options list is: %s' %optionsFile)
    if inputData:
      if type(inputData) in types.StringTypes:
        inputData = [inputData]
      if not type(inputData)==type([]):
        return self._reportError('Expected single LFN string or list of LFN(s) for inputData',__name__,**kwargs)
      for i in xrange(len(inputData)):
        inputData[i] = inputData[i].replace('LFN:','')
      inputData = map( lambda x: 'LFN:'+x, inputData)
      inputDataStr = string.join(inputData,';')
      self.addToInputData.append(inputDataStr)

    self.gaudiStepCount +=1
    stepNumber = self.gaudiStepCount
    stepDefn = '%sStep%s' %(appName,stepNumber)
    step =  self.__getGaudiApplicationStep(stepDefn)

    stepName = 'Run%sStep%s' %(appName,stepNumber)

    logPrefix = 'Step%s_' %(stepNumber)
    logName = '%s%s' %(logPrefix,logName)
    self.addToOutputSandbox.append(logName)

    self.workflow.addStep(step)
    stepPrefix = '%s_' % stepName
    self.currentStepPrefix = stepPrefix

    # Define Step and its variables
    stepInstance = self.workflow.createStepInstance(stepDefn,stepName)

    stepInstance.setValue("applicationName",appName)
    stepInstance.setValue("applicationVersion",appVersion)
    stepInstance.setValue("applicationLog",logName)
    if optionsFile:
      stepInstance.setValue("optionsFile",optionsFile)
    if optionsLine:
      stepInstance.setValue("optionsLine",optionsLine)
    if inputDataType:
      stepInstance.setValue("inputDataType",inputDataType)
    if inputData:
      stepInstance.setValue("inputData",string.join(inputData,';'))

    # now we have to tell DIRAC to install the necessary software
    currentApp = '%s.%s' %(appName,appVersion)
    swPackages = 'SoftwarePackages'
    description='List of LHCb Software Packages to be installed'
    if not self.workflow.findParameter(swPackages):
      self._addParameter(self.workflow,swPackages,'JDL',currentApp,description)
    else:
      apps = self.workflow.findParameter(swPackages).getValue()
      if not currentApp in string.split(apps,';'):
        apps += ';'+currentApp
      self._addParameter(self.workflow,swPackages,'JDL',apps,description)
    return S_OK()

  #############################################################################
  def __getGaudiApplicationStep(self,name='GaudiApplication'):
    """Internal function.

        This method controls the definition for a GaudiApplication step.
    """
    # Create the GaudiApplication module first
    moduleName = 'GaudiApplication'
    module = ModuleDefinition(moduleName)
    module.setDescription('A generic Gaudi Application module that can execute any provided project name and version')
    body = 'from %s.%s import %s\n' %(self.importLocation,moduleName,moduleName)
    module.setBody(body)
    # Create Step definition
    step = StepDefinition(name)
    step.addModule(module)
    moduleInstance = step.createModuleInstance('GaudiApplication',name)
    # Define step parameters
    step.addParameter(Parameter("applicationName","","string","","",False, False, "Application Name"))
    step.addParameter(Parameter("applicationVersion","","string","","",False, False, "Application Name"))
    step.addParameter(Parameter("applicationLog","","string","","",False,False,"Name of the output file of the application"))
    step.addParameter(Parameter("optionsFile","","string","","",False,False,"Options File"))
    step.addParameter(Parameter("optionsLine","","string","","",False,False,"This is appended to standard options"))
    #step.addParameter(Parameter("optionsLinePrev","","string","","",False,False,"options to be added first","option"))
    #step.addParameter(Parameter("poolXMLCatName","","string","","",False,False,"POOL XML Catalog file name"))
    step.addParameter(Parameter("inputDataType","","string","","",False, False, "Input Data Type"))
    step.addParameter(Parameter("inputData","","string","","",False, False, "Input Data Type"))
    return step

  #############################################################################
  def setApplicationScript(self,appName,appVersion,script,arguments='',inputData='',inputDataType='',poolXMLCatalog='pool_xml_catalog.xml',logFile=''):
    """Helper function.

       Specify application environment and script to be executed.

       For LHCb these could be e.g. Gauss, Boole, Brunel,
       DaVinci etc.

       The script name and any arguments should also be specified.

       Input data for application script must be specified here, please note that if this is set at the job level,
       e.g. via setInputData() but not above, input data is not in the scope of the specified application.

       Any input data specified at the step level that is not already specified at the job level is added automatically
       as a requirement for the job.

       Example usage:

       >>> job = LHCbJob()
       >>> job.setApplicationScript('DaVinci','v19r12','myScript.py')

       @param appName: Application name
       @type appName: string
       @param appVersion: Application version
       @type appVersion: string
       @param script: Script to execute
       @type script: string
       @param arguments: Optional arguments for script
       @type arguments: string
       @param inputData: Input data for application
       @type inputData: single LFN or list of LFNs
       @param inputDataType: Input data type for application (e.g. DATA, MDF, ETC)
       @type inputDataType: string
       @param arguments: Optional POOL XML Catalog name for any input data files (default is pool_xml_catalog.xml)
       @type arguments: string
       @param logFile: Optional log file name
       @type logFile: string
    """
    kwargs = {'appName':appName,'appVersion':appVersion,'script':script,'arguments':arguments,'inputData':inputData,'inputDataType':inputDataType,'poolXMLCatalog':poolXMLCatalog,'logFile':logFile}
    if not type(appName) in types.StringTypes or not type(appVersion) in types.StringTypes:
      return self._reportError('Expected strings for application name and version',__name__,**kwargs)    

    if not script or not type(script)==type(' '):
      return self._reportError('Expected strings for script name',__name__,**kwargs)    
      
    if not os.path.exists(script):
      return self._reportError('Script must exist locally',__name__,**kwargs)    

    if logFile:
      if type(logFile) == type(' '):
        logName = logFile
      else:
        return self._reportError('Expected string for log file name',__name__,**kwargs)    
    else:
      shortScriptName = os.path.basename(script).split('.')[0]
      logName = '%s_%s_%s.log' %(appName,appVersion,shortScriptName)

    self.addToInputSandbox.append(script)

    if arguments:
      if not type(arguments)==type(' '):
        return self._reportError('Expected string for optional script arguments',__name__,**kwargs)    

    if not type(poolXMLCatalog)==type(" "):
      return self._reportError('Expected string for POOL XML Catalog name',__name__,**kwargs)    

    if inputData:
      if type(inputData) in types.StringTypes:
        inputData = [inputData]
      if not type(inputData)==type([]):
        return self._reportError('Expected single LFN string or list of LFN(s) for inputData',__name__,**kwargs)    
      for i in xrange(len(inputData)):
        inputData[i] = inputData[i].replace('LFN:','')
      inputData = map( lambda x: 'LFN:'+x, inputData)
      inputDataStr = string.join(inputData,';')
      self.addToInputData.append(inputDataStr)

    self.gaudiStepCount +=1
    stepNumber = self.gaudiStepCount
    stepDefn = '%sStep%s' %(appName,stepNumber)
    step =  self.__getGaudiApplicationScriptStep(stepDefn)

    stepName = 'Run%sStep%s' %(appName,stepNumber)

    logPrefix = 'Step%s_' %(stepNumber)
    logName = '%s%s' %(logPrefix,logName)
    self.addToOutputSandbox.append(logName)

    self.workflow.addStep(step)
    stepPrefix = '%s_' % stepName
    self.currentStepPrefix = stepPrefix

    # Define Step and its variables
    stepInstance = self.workflow.createStepInstance(stepDefn,stepName)

    stepInstance.setValue("applicationName",appName)
    stepInstance.setValue("applicationVersion",appVersion)
    stepInstance.setValue("script",script)
    stepInstance.setValue("applicationLog",logName)
    if arguments:
      stepInstance.setValue("arguments",arguments)
    if inputDataType:
      stepInstance.setValue("inputDataType",inputDataType)
    if inputData:
      stepInstance.setValue("inputData",string.join(inputData,';'))
    if poolXMLCatalog:
      stepInstance.setValue("poolXMLCatName",poolXMLCatalog)

    # now we have to tell DIRAC to install the necessary software
    currentApp = '%s.%s' %(appName,appVersion)
    swPackages = 'SoftwarePackages'
    description='List of LHCb Software Packages to be installed'
    if not self.workflow.findParameter(swPackages):
      self._addParameter(self.workflow,swPackages,'JDL',currentApp,description)
    else:
      apps = self.workflow.findParameter(swPackages).getValue()
      if not currentApp in string.split(apps,';'):
        apps += ';'+currentApp
      self._addParameter(self.workflow,swPackages,'JDL',apps,description)
    return S_OK()

  #############################################################################
  def __getGaudiApplicationScriptStep(self,name='GaudiApplicationScript'):
    """Internal function.

      This method controls the definition for a GaudiApplicationScript step.
    """
    # Create the GaudiApplication script module first
    moduleName = 'GaudiApplicationScript'
    module = ModuleDefinition(moduleName)
    module.setDescription('A  Gaudi Application script module that can execute any provided script in the given project name and version environment')
    body = 'from %s.%s import %s\n' %(self.importLocation,moduleName,moduleName)
    module.setBody(body)
    # Create Step definition
    step = StepDefinition(name)
    step.addModule(module)
    moduleInstance = step.createModuleInstance('GaudiApplicationScript',name)
    # Define step parameters
    step.addParameter(Parameter("applicationName","","string","","",False, False, "Application Name"))
    step.addParameter(Parameter("applicationVersion","","string","","",False, False, "Application Name"))
    step.addParameter(Parameter("applicationLog","","string","","",False,False,"Name of the output file of the application"))
    step.addParameter(Parameter("script","","string","","",False,False,"Script name"))
    step.addParameter(Parameter("arguments","","string","","",False,False,"Arguments for script"))
    step.addParameter(Parameter("poolXMLCatName","","string","","",False,False,"POOL XML Catalog file name"))
    step.addParameter(Parameter("inputDataType","","string","","",False, False, "Input Data Type"))
    step.addParameter(Parameter("inputData","","string","","",False, False, "Input Data Type"))
    return step

  #############################################################################
  def setBenderModule(self,benderVersion,modulePath,inputData='',numberOfEvents=-1):
    """Helper function.

       Specify Bender module to be executed.

       Any additional files should be specified in the job input sandbox.  Input data for
       Bender should be specified here (can be string or list).

       Example usage:

       >>> job = LHCbJob()
       >>> job.setBenderModule('v8r3','BenderExample.PhiMC',inputData=['LFN:/lhcb/production/DC06/phys-v2-lumi2/00001758/DST/0000/00001758_00000001_5.dst'],numberOfEvents=100)

       @param benderVersion: Bender Project Version
       @type benderVersion: string
       @param modulePath: Import path to module e.g. BenderExample.PhiMC
       @type modulePath: string
       @param inputData: Input data for application
       @type inputData: single LFN or list of LFNs
       @param numberOfEvents: Number of events to process e.g. -1
       @type numberOfEvents: integer
    """
    kwargs = {'benderVersion':benderVersion,'modulePath':modulePath,'inputData':inputData,'numberOfEvents':numberOfEvents}
    if not type(benderVersion)==type(' '):
      return self._reportError('Bender version should be a string',__name__,**kwargs)    
    if not type(modulePath)==type(' '):
      return self._reportError('Bender module path should be a string',__name__,**kwargs)    
    if not type(numberOfEvents)==type(2):
      try:
        numberOfEvents=int(numberOfEvents)
      except Exception,x:
        return self._reportError('Number of events should be an integer or convertible to an integer',__name__,**kwargs)    
    if type(inputData)==type(" "):
      inputData = [inputData]
    if not type(inputData)==type([]):
      return self._reportError('Input data should be specified as a list or a string',__name__,**kwargs)    
      
    poolCatName='xmlcatalog_file:pool_xml_catalog.xml'
    benderScript = ['#!/usr/bin/env python']
    benderScript.append('from Gaudi.Configuration import FileCatalog')
    benderScript.append('FileCatalog   ( Catalogs = [ "%s" ] )' %poolCatName)
    benderScript.append('import %s as USERMODULE' %modulePath)
    benderScript.append('USERMODULE.configure()')
    benderScript.append('gaudi = USERMODULE.appMgr()')
    benderScript.append('evtSel = gaudi.evtSel()')
    benderScript.append('evtSel.open ( %s ) ' %inputData)
    benderScript.append('USERMODULE.run( %s )\n' %numberOfEvents)
    guid = makeGuid()
    tmpdir = self.scratchDir+'/'+guid
    self.log.verbose('Created temporary directory for submission %s' % (tmpdir))
    os.mkdir(tmpdir)
    fopen = open('%s/BenderScript.py' %tmpdir,'w')
    self.log.verbose('Bender script is: %s/BenderScript.py' %tmpdir)
    fopen.write(string.join(benderScript,'\n'))
    fopen.close()
    #should try all components of the PYTHONPATH before giving up...
    userModule = '%s.py' %(string.split(modulePath,'.')[-1])
    self.log.verbose('Looking for user module with name: %s' %userModule)
    if os.path.exists(userModule):
      self.addToInputSandbox.append(userModule)
    self.setInputData(inputData)
    self.setApplicationScript('Bender', benderVersion, '%s/BenderScript.py' %tmpdir, logFile='Bender%s.log' %benderVersion)
    return S_OK()
  
  #############################################################################
  def setRootMacro(self,rootVersion,rootScript,arguments='',logFile=''):
    """Helper function.

       Specify ROOT version and macro to be executed (e.g. root -b -f <rootScript>).

       Can optionally specify arguments to the script and a name for the output log file.

       Example usage:

       >>> job = LHCbJob()
       >>> j.setRootMacro('5.18.00a','test.C')

       @param rootVersion: LHCb supported ROOT version
       @type rootVersion: string
       @param rootScript: Path to ROOT macro script
       @type rootScript: string
       @param arguments: Optional arguments for macro
       @type arguments: string or list
       @param logFile: Optional log file name
       @type logFile: string
    """
    rootType = 'c'
    return self.__configureRootModule(rootVersion, rootScript, rootType, arguments, logFile)

  #############################################################################
  def setRootPythonScript(self,rootVersion,rootScript,arguments='',logFile=''):
    """Helper function.

       Specify ROOT version and python script to be executed (e.g. python <rootScript>).

       Can optionally specify arguments to the script and a name for the output log file.

       Example usage:

       >>> job = LHCbJob()
       >>> j.setRootPythonScript('5.18.00a','test.py')

       @param rootVersion: LHCb supported ROOT version
       @type rootVersion: string
       @param rootScript: Path to ROOT python script
       @type rootScript: string
       @param arguments: Optional arguments for python script
       @type arguments: string or list
       @param logFile: Optional log file name
       @type logFile: string
    """
    rootType = 'py'
    return self.__configureRootModule(rootVersion, rootScript, rootType, arguments, logFile)

  #############################################################################
  def setRootExecutable(self,rootVersion,rootScript,arguments='',logFile=''):
    """Helper function.

       Specify ROOT version and executable (e.g. ./<rootScript>).

       Can optionally specify arguments to the script and a name for the output log file.

       Example usage:

       >>> job = LHCbJob()
       >>> j.setRootExecutable('5.18.00a','minexam')

       @param rootVersion: LHCb supported ROOT version
       @type rootVersion: string
       @param rootScript: Path to ROOT macro script
       @type rootScript: string
       @param arguments: Optional arguments for macro
       @type arguments: string or list
       @param logFile: Optional log file name
       @type logFile: string
    """
    rootType='bin'
    return self.__configureRootModule(rootVersion, rootScript, rootType, arguments, logFile)

  #############################################################################
  def __configureRootModule(self,rootVersion,rootScript,rootType,arguments,logFile):
    """ Internal function.

        Supports the root macro, python and executable wrapper functions.
    """
    kwargs = {'rootVersion':rootVersion,'rootScript':rootScript,'rootType':rootType,'arguments':arguments,'logFile':logFile}
    if not type(arguments) == types.ListType:
      arguments = [arguments]

    for param in [rootVersion,rootScript,rootType,logFile]:
      if not type(param) in types.StringTypes:
        return self._reportError('Expected strings for Root application input parameters',__name__,**kwargs)    

    if not os.path.exists(rootScript):
      return self._reportError('ROOT Script %s must exist locally' %(rootScript),__name__,**kwargs)    
     
    self.addToInputSandbox.append(rootScript)

    #Must check if ROOT version in available versions and define appName appVersion...
    rootVersions = gConfig.getOptions(self.rootSection,[])
    if not rootVersions['OK']:
      return self._reportError('Could not contact DIRAC Configuration Service for supported ROOT version list',__name__,**kwargs)          

    rootList = rootVersions['Value']
    if not rootVersion in rootList:
      return self._reportError('Requested ROOT version %s is not in supported list: %s' %(rootVersion,string.join(rootList,', ')),__name__,**kwargs)                

    rootName = os.path.basename(rootScript).replace('.','')
    if logFile:
      logName = logFile
    else:
      logName = '%s_%s.log' %(rootName,rootVersion.replace('.',''))

    self.gaudiStepCount +=1
    stepNumber = self.gaudiStepCount
    stepDefn = '%sStep%s' %(rootName,stepNumber)
    step =  self.__getRootApplicationStep(stepDefn)

    stepName = 'Run%sStep%s' %(rootName,stepNumber)

    logPrefix = 'Step%s_' %(stepNumber)
    logName = '%s%s' %(logPrefix,logName)
    self.addToOutputSandbox.append(logName)

    self.workflow.addStep(step)
    stepPrefix = '%s_' % stepName
    self.currentStepPrefix = stepPrefix

    # Define Step and its variables
    stepInstance = self.workflow.createStepInstance(stepDefn,stepName)
    stepInstance.setValue("rootVersion",rootVersion)
    stepInstance.setValue("rootType",rootType)
    stepInstance.setValue("rootScript",os.path.basename(rootScript))
    stepInstance.setValue("logFile",logName)
    if arguments:
      stepInstance.setValue("arguments",arguments)

    # now we have to tell DIRAC to install the necessary software
    appRoot = '%s/%s' %(self.rootSection,rootVersion)
    currentApp = gConfig.getValue(appRoot,'')
    if not currentApp:
      return self._reportError('Could not get value from DIRAC Configuration Service for option %s' %appRoot,__name__,**kwargs)                
    swPackages = 'SoftwarePackages'
    description='List of LHCb Software Packages to be installed'
    if not self.workflow.findParameter(swPackages):
      self._addParameter(self.workflow,swPackages,'JDL',currentApp,description)
    else:
      apps = self.workflow.findParameter(swPackages).getValue()
      if not currentApp in string.split(apps,';'):
        apps += ';'+currentApp
      self._addParameter(self.workflow,swPackages,'JDL',apps,description)
    return S_OK()  

  #############################################################################
  def __getRootApplicationStep(self,name='RootApplication'):
    """Internal function.

        This method controls the definition for a RootApplication step.
    """
    # Create the GaudiApplication module first
    moduleName = 'RootApplication'
    module = ModuleDefinition(moduleName)
    module.setDescription('A generic Root Application module that can execute macros, python scripts or executables')
    body = 'from %s.%s import %s\n' %(self.importLocation,moduleName,moduleName)    
    module.setBody(body)
    # Create Step definition
    step = StepDefinition(name)
    step.addModule(module)
    moduleInstance = step.createModuleInstance('RootApplication',name)
    # Define step parameters
    step.addParameter(Parameter("rootVersion","","string","","",False, False, "Root version."))
    step.addParameter(Parameter("rootScript","","string","","",False, False, "Root script."))
    step.addParameter(Parameter("rootType","","string","","",False, False, "Root type."))
    step.addParameter(Parameter("arguments",[],"list","","",False, False, "Optional arguments for payload."))
    step.addParameter(Parameter("logFile","","string","","",False, False, "Log file name."))
    return step

  #############################################################################
  def __getCurrentStepPrefix(self):
    """Internal function, returns current step prefix for setting parameters.
    """
    return self.currentStepPrefix

  #############################################################################
  def addPackage(self,appName,appVersion):
    """Helper function.

       Specify additional software packages to be installed on Grid
       Worker Node before job execution commences.

       Example usage:

       >>> job = LHCbJob()
       >>> job.addPackage('DaVinci','v19r12')

       @param appName: Package name
       @type appName: string
       @param appVersion: Package version
       @type appVersion: Package version string

    """
    kwargs = {'appName':appName,'appVersion':appVersion}
    if not type(appName) == type(' ') or not type(appVersion) == type(' '):
      return self._reportError('Expected strings for application name and version',__name__,**kwargs)                
    
    currentApp = '%s.%s' %(appName,appVersion)
    swPackages = 'SoftwarePackages'
    description='List of LHCb Software Packages to be installed'
    if not self.workflow.findParameter(swPackages):
      self._addParameter(self.workflow,swPackages,'JDL',currentApp,description)
    else:
      apps = self.workflow.findParameter(swPackages).getValue()
      if apps:
        if not currentApp in string.split(apps,';'):
          apps += ';'+currentApp
        self._addParameter(self.workflow,swPackages,'JDL',apps,description)

    return S_OK()

  #############################################################################
  def setAncestorDepth(self,depth):
    """Helper function.

       Level at which ancestor files are retrieved from the bookkeeping.

       For analysis jobs running over RDSTs the ancestor depth may be specified
       to ensure that the parent DIGI / DST files are staged before job execution.

       Example usage:

       >>> job = LHCbJob()
       >>> job.setAncestorDepth(2)

       @param depth: Ancestor depth
       @type depth: string or int

    """
    kwargs = {'depth':depth}
    description = 'Level at which ancestor files are retrieved from the bookkeeping'
    if type(depth)==type(" "):
      try:
        self._addParameter(self.workflow,'AncestorDepth','JDL',int(depth),description)
      except Exception,x:
        return self._reportError('Expected integer for Ancestor Depth',__name__,**kwargs)                
    elif type(depth)==type(1):
      self._addParameter(self.workflow,'AncestorDepth','JDL',depth,description)
    else:
      return self._reportError('Expected integer for Ancestor Depth',__name__,**kwargs)                
    return S_OK()

  #############################################################################
  def setInputDataType(self,inputDataType):
    """Helper function.

       Explicitly set the input data type to be conveyed to Gaudi Applications.

       Default is DATA, e.g. for DST / RDST files.  Other options include:
        - MDF, for .raw files
        - ETC, for running on a public or private Event Tag Collections.

       Example usage:

       >>> job = LHCbJob()
       >>> job.setInputDataType('ETC')

       @param inputDataType: Input Data Type
       @type inputDataType: String

    """
    description = 'User specified input data type'
    if not type(inputDataType)==type(" "):
      try:
        inputDataType = str(inputDataType)
      except Exception,x:
        return self._reportError('Expected string for input data type',__name__,**{'inputDataType':inputDataType})                

    self.inputDataType = inputDataType
    self._addParameter(self.workflow,'InputDataType','JDL',inputDataType,description)
    return S_OK()

  #############################################################################
  def setCondDBTags(self,condDict):
    """Under development. Helper function.

       Specify Conditions Database tags by by Logical File Name (LFN).

       The input dictionary is of the form: {<DB>:<TAG>} as in the example below.

       Example usage:

       >>> job = LHCbJob()
       >>> job.setCondDBTags({'DDDB':'DC06','LHCBCOND':'DC06'})

       @param condDict: CondDB tags
       @type condDict: Dict of DB, tag pairs
    """
    kwargs = {'condDict':condDict}
    if not type(condDict)==type({}):
      return self._reportError('Expected dictionary for CondDB tags',__name__,**kwargs)                

    conditions = []
    for db,tag in condDict.items():
      try:
        db = str(db)
        tag = str(tag)
        conditions.append(string.join([db,tag],'.'))
      except Exception,x:
        return self._reportError('Expected string for conditions',__name__,**kwargs)                

    condStr = string.join(conditions,';')
    description = 'List of CondDB tags'
    self._addParameter(self.workflow,'CondDBTags','JDL',condStr,description)
    return S_OK()

  #############################################################################
  def setInputDataPolicy(self,policy):
    """Helper function.

       Specify a job input data policy, this takes precedence over any site specific or
       global settings.

       Possible values for policy are 'Download' or 'Protocol' (case-insensitive).

       Example usage:

       >>> job = LHCbJob()
       >>> job.setInputDataPolicy('download')

    """
    kwargs = {'policy':policy}
    csSection = '/Operations/InputDataPolicy'
    possible = ['Download','Protocol']
    finalPolicy = ''
    for p in possible:
      if string.lower(policy)==string.lower(p):
        finalPolicy = p

    if not finalPolicy:
      return self._reportError('Expected one of %s for input data policy' %(string.join(possible,', ')),__name__,**kwargs)                

    jobPolicy = gConfig.getValue('%s/%s' %(csSection,finalPolicy),'')
    if not jobPolicy:
      return self._reportError('Could not get value for CS option %s/%s' %(csSection,finalPolicy),__name__,**kwargs)                

    description = 'User specified input data policy'
    self._addParameter(self.workflow,'InputDataPolicy','JDL',jobPolicy,description)
    return S_OK()

#Below to be thought about further when adding user finalization

#  #############################################################################
#  def setOutputData(self,lfns,OutputSE=[],OutputPath=''):
#    """Helper function, used in preference to Job.setOutputData() for LHCb.
#
#       For specifying output data to be registered in Grid storage.  If a list
#       of OutputSEs are specified the job wrapper will try each in turn until
#       successful.
#
#       Example usage:
#
#       >>> job = Job()
#       >>> job.setOutputData(['DVNtuple.root'])
#
#       @param lfns: Output data file or files
#       @type lfns: Single string or list of strings ['','']
#       @param OutputSE: Optional parameter to specify the Storage
#       @param OutputPath: Optional parameter to specify the Path in the Storage
#       Element to store data or files, e.g. CERN-tape
#       @type OutputSE: string or list
#       @type OutputPath: string
#    """
#    if type(lfns)==list and len(lfns):
#      outputDataStr = string.join(lfns,';')
#      description = 'List of output data files'
#      self._addParameter(self.workflow,'UserOutputData','JDL',outputDataStr,description)
#    elif type(lfns)==type(" "):
#      description = 'Output data file'
#      self._addParameter(self.workflow,'UserOutputData','JDL',lfns,description)
#    else:
#      raise TypeError,'Expected string or list of output data files'
#      return self._reportError('Expected integer for Ancestor Depth',__name__,**kwargs)                
#
#    if OutputSE:
#      description = 'User specified Output SE'
#      if type(OutputSE) in types.StringTypes:
#        OutputSE = [OutputSE]
#      elif type(OutputSE) != types.ListType:
#        raise TypeError,'Expected string or list for OutputSE'
#        return self._reportError('Expected integer for Ancestor Depth',__name__,**kwargs)                
#      
#      OutputSE = ';'.join(OutputSE)
#      self._addParameter(self.workflow,'UserOutputSE','JDL',OutputSE,description)
#
#    if OutputPath:
#      description = 'User specified Output Path'
#      if not type(OutputPath) in types.StringTypes:
#        raise TypeError,'Expected string for OutputPath'
#        return self._reportError('Expected integer for Ancestor Depth',__name__,**kwargs)                
#      
#      # Remove leading "/" that might cause problems with os.path.join
#      while OutputPath[0] == '/': OutputPath=OutputPath[1:]
#      self._addParameter(self.workflow,'UserOutputPath','JDL',OutputPath,description)

#  #############################################################################
#  def _addFinalization(self):
#    """ Internal function, add LHCb user finalization module auotmatically in case
#    """

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#