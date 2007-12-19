########################################################################
# $Id: JobWrapper.py,v 1.9 2007/12/19 15:16:18 paterson Exp $
# File :   JobWrapper.py
# Author : Stuart Paterson
########################################################################

""" The Job Wrapper Class is instantiated with arguments tailored for running
    a particular job. The JobWrapper starts a thread for execution of the job
    and a Watchdog Agent that can monitor progress.
"""

__RCSID__ = "$Id: JobWrapper.py,v 1.9 2007/12/19 15:16:18 paterson Exp $"

from DIRAC.DataManagementSystem.Client.ReplicaManager               import ReplicaManager
from DIRAC.DataManagementSystem.Client.PoolXMLCatalog               import PoolXMLCatalog
from DIRAC.WorkloadManagementSystem.Client.SandboxClient            import SandboxClient
from DIRAC.WorkloadManagementSystem.JobWrapper.WatchdogFactory      import WatchdogFactory
from DIRAC.Core.DISET.RPCClient                                     import RPCClient
from DIRAC.Core.Utilities.Subprocess                                import shellCall
from DIRAC.Core.Utilities.Subprocess                                import Subprocess
from DIRAC                                                          import S_OK, S_ERROR, gConfig, gLogger
import DIRAC

COMPONENT_NAME = '/LocalSite/JobWrapper'

import os, re, sys, string, time, shutil, threading, tarfile

EXECUTION_RESULT = {}

class JobWrapper:

  #############################################################################
  def __init__(self, jobID=None):
    """ Standard constructor
    """
    self.section = COMPONENT_NAME
    self.log = gLogger
    #self.log.setLevel('debug')
    self.jobID = jobID
    self.root = os.getcwd()
    self.jobReport  = RPCClient('WorkloadManagement/JobStateUpdate')
    self.inputSandboxClient = SandboxClient()
    self.outputSandboxClient = SandboxClient('Output')
    self.diracVersion = 'DIRAC version v%dr%d build %d' %(DIRAC.majorVersion,DIRAC.minorVersion,DIRAC.patchLevel)
    self.maxPeekLines = gConfig.getValue(self.section+'/MaxJobPeekLines',200)
    self.defaultCPUTime = gConfig.getValue(self.section+'/DefaultCPUTime',600)
    self.defaultOutputFile = gConfig.getValue(self.section+'/DefaultOutputFile','std.out')
    self.defaultErrorFile = gConfig.getValue(self.section+'/DefaultErrorFile','std.err')
    self.cleanUpFlag  = gConfig.getValue(self.section+'/CleanUpFlag',False)
    self.localSiteRoot = gConfig.getValue('/LocalSite/Root',self.root)
    self.__loadLocalCFGFiles(self.localSiteRoot)
    self.vo = gConfig.getValue('/DIRAC/VirtualOrganization','lhcb')
    self.rm = ReplicaManager()
    self.log.verbose('===========================================================================')
    self.log.verbose('CVS version %s' %(__RCSID__))
    self.log.verbose(self.diracVersion)
    self.log.verbose('Developer tag: 1')
    currentPID = os.getpid()
    self.log.verbose('Job Wrapper started under PID: %s' % currentPID )
    self.log.verbose('==========================================================================')
    self.log.debug('Sys path is: \n%s' %(string.join(sys.path,'\n')))
    self.log.debug('==========================================================================')
    pypath = os.environ['PYTHONPATH']
    print pypath
    if not pypath:
      self.log.debug('PYTHONPATH is: null')
    else:
      self.log.debug('PYTHONPATH is: \n%s' %(string.join(string.split(pypath,':'),'\n')))
      self.log.debug('==========================================================================')
    if not self.cleanUpFlag:
      self.log.debug('CleanUp Flag is disabled by configuration')
    self.log.verbose('Trying to import LFC File Catalog client')
    try:
      from DIRAC.DataManagementSystem.Client.Catalog.LcgFileCatalogCombinedClient import LcgFileCatalogCombinedClient
      self.fileCatalog = LcgFileCatalogCombinedClient()
    except Exception,x:
      msg = 'Failed to create LcgFileCatalogClient with exception:'
      self.log.fatal(msg)
      self.log.fatal(str(x))

  #############################################################################
  def initialize(self, arguments):
    """ Initializes parameters and environment for job.
    """
    self.__report('Running','Job Initialization')
    self.log.info('Starting Job Wrapper Initialization for Job %s' %(self.jobID))
    jobArgs = arguments['Job']
    self.log.debug(jobArgs)
    ceArgs = arguments ['CE']
    self.log.debug(ceArgs)
    self.__setInitialJobParameters(arguments)

    # Prepare the working directory and cd to there
    if os.path.exists(self.jobID):
      shutil.rmtree(str(self.jobID))
    os.mkdir(str(self.jobID))
    os.chdir(str(self.jobID))

  #############################################################################
  def __loadLocalCFGFiles(self,localRoot):
    """Loads any extra CFG files residing in the local DIRAC site root.
    """
    files = os.listdir(localRoot)
    for i in files:
      if re.search('.cfg$',i):
        gConfig.loadFile(i)

  #############################################################################
  def execute(self, arguments):
    """The main execution method of the Job Wrapper
    """
    self.log.info('Job Wrapper is starting execution phase for job %s' %(self.jobID))
    os.environ['DIRACROOT'] = self.localSiteRoot
    self.log.verbose('DIRACROOT = %s' %(self.localSiteRoot))
    os.environ['DIRACPYTHON'] = sys.executable
    self.log.verbose('DIRACPYTHON = %s' %(sys.executable))

    jobArgs = arguments['Job']
    ceArgs = arguments ['CE']

    if jobArgs.has_key('MaxCPUTime'):
      jobCPUTime = int(jobArgs['MaxCPUTime'])
    else:
      self.log.info('Job %s has no CPU time limit specified, applying default of %s' %(self.jobID,self.defaultCPUTime))
      jobCPUTime = self.defaultCPUTime

    if jobArgs.has_key('Executable'):
      executable = jobArgs['Executable']
    else:
      msg = 'Job %s has no specified executable' %(self.jobID)
      self.log.warn(msg)
      return S_ERROR(msg)

    jobArguments = ' '
    if jobArgs.has_key('Arguments'):
      jobArguments = jobArgs['Arguments']

    executable = os.path.expandvars(executable)
    thread = None
    spObject = None

    if re.search('DIRACROOT',executable):
      executable = executable.replace('$DIRACROOT',self.localSiteRoot)
      self.log.debug('Replaced $DIRACROOT for executable as %s' %(self.localSiteRoot))

    if os.path.exists(executable):
      self.__report('Running','Application')
      spObject = Subprocess( 0 )
      command = '%s %s' % (executable,os.path.basename(jobArguments))
      self.log.verbose('Execution command: %s' %(command))
      maxPeekLines = self.maxPeekLines
      thread = ExecutionThread(spObject,command, maxPeekLines)
      thread.start()
    else:
      return S_ERROR('Path to executable %s not found' %(executable))

    pid = os.getpid()
    watchdogFactory = WatchdogFactory()
    watchdogInstance = watchdogFactory.getWatchdog(pid, thread, spObject, jobCPUTime)
    if not watchdogInstance['OK']:
      return watchdogInstance

    watchdog = watchdogInstance['Value']
    self.log.verbose('Calibrating Watchdog instance')
    watchdog.calibrate()
    if thread.isAlive():
      self.log.info('Application thread is started in Job Wrapper')
      watchdog.run()
    else:
      self.log.warn('Application thread stopped very quickly...')

    self.log.debug( 'Execution Result is : ')
    self.log.debug( EXECUTION_RESULT )
    outputs = None
    if EXECUTION_RESULT.has_key('Thread'):
      threadResult = EXECUTION_RESULT['Thread']
      if not threadResult['OK']:
        self.log.warn(threadResult['Message'])
      else:
        outputs = threadResult['Value']

    if outputs:
      errorFileName = self.defaultErrorFile
      outputFileName = self.defaultOutputFile
      status = threadResult['Value'][0]
      stdout = threadResult['Value'][1]
      stderr = threadResult['Value'][2]
      self.log.debug('Execution thread status = %s' %(status))
      if jobArgs.has_key('StdError'):
        errorFileName = jobArgs['StdError']
      if jobArgs.has_key('StdOutput'):
        outputFileName = jobArgs['StdOutput']
      outputFile = open(outputFileName,'w')
      print >> outputFile, stdout
      outputFile.close()
      errorFile = open(errorFileName,'w')
      print >> errorFile, stderr
      errorFile.close()
    else:
      self.log.warn('No outputs generated from job execution')

    return S_OK()

  #############################################################################
  def resolveInputData(self,arguments):
    """Input data is resolved here for the first iteration of SRM2 testing.
    """
    self.__report('Running','Input Data Resolution')
    self.log.info('Initial iteration of input data resolution in Job Wrapper for SRM2 testing')
    jobArgs = arguments['Job']
    if not jobArgs.has_key('InputData'):
      msg = 'Could not obtain job input data requirement from available parameters'
      self.log.warn(msg)
      return S_ERROR(msg)

    ceArgs = arguments['CE']
    if not ceArgs.has_key('LocalSE'):
      csLocalSE = gConfig.getValue('LocalSite/LocalSE','')
      if not csLocalSE:
        msg = 'Job has input data requirement but no LocalSE defined'
        self.log.warn(msg)
        return S_ERROR(msg)
      else:
        ceArgs['LocalSE'] = csLocalSE

    inputData = jobArgs['InputData']
    self.log.debug('Input Data is: \n%s' %(inputData))
    if type(inputData)==type(' '):
      inputData = [inputData]

    localSEList = ceArgs['LocalSE']
    if type(localSEList)==type(' '):
      localSEList=localSEList.split(',')

    msg = 'Job Wrapper cannot resolve input data with null '
    if not inputData:
      msg += 'job input data parameter '
      self.log.warn(msg)
      return S_ERROR(msg)
    if not localSEList:
      msg += 'site localSE list'
      self.log.warn(msg)
      return S_ERROR(msg)

    self.log.verbose('Job input data requirement is \n%s' %(string.join(inputData,',\n')))
    self.log.verbose('Site has the following local SEs: %s' %(string.join(localSEList,', ')))

    lfns = [string.replace(fname,'LFN:','') for fname in inputData]
    start = time.time()
    result = self.fileCatalog.getReplicas(lfns)
    timing = time.time() - start
    self.log.info('LFC Lookup Time: %.2f seconds ' % (timing) )
    if not result['OK']:
      self.log.warn(result['Message'])
      return result

    badLFNCount = 0
    badLFNs = []
    catalogResult = result['Value']

    if catalogResult.has_key('Failed'):
      for lfn,cause in catalogResult['Failed'].items():
        badLFNCount+=1
        badLFNs.append('LFN:%s Problem: %s' %(lfn,cause))

    if catalogResult.has_key('Successful'):
      for lfn,replicas in catalogResult['Successful'].items():
        if not replicas:
          badLFNCount+=1
          badLFNs.append('LFN:%s Problem: Null replica value' %(lfn))

    if badLFNCount:
      self.log.warn('Job Wrapper found %s problematic LFN(s) for job %s' % (badLFNCount,self.jobID))
      param = string.join(badLFNs,'\n')
      self.log.info(param)
      result = self.__setJobParam('MissingLFNs',param)
      return S_ERROR('Input Data Not Available')

    replicas = catalogResult['Successful']
    self.log.debug(replicas)
    seFilesDict = {}
    failedReplicas = []
    pfnList = []

    for lfn,reps in replicas.items():
      localReplica = False
      for localSE in localSEList:
        if reps.has_key(localSE):
          localReplica = True
      if not localReplica:
        failedReplicas.append(lfn)

    if failedReplicas:
      msg = 'The following files were found not to have replicas for available LocalSEs:\n%s' %(string.join(failedReplicas,',\n'))
      return S_ERROR(msg)

    #For the unlikely case that a file is found on two SEs at the same site
    #only the first SURL/SE is taken
    trackLFNs = []
    for localSE in localSEList:
      for lfn,reps in replicas.items():
        if reps.has_key(localSE):
          pfn = reps[localSE]
          if seFilesDict.has_key(localSE):
            currentFiles = seFilesDict[localSE]
            if not lfn in trackLFNs:
              currentFiles.append(pfn)
            seFilesDict[localSE] = currentFiles
            trackLFNs.append(lfn)
          else:
            seFilesDict[localSE] = [pfn]
            trackLFNs.append(lfn)

    self.log.debug(seFilesDict)

    for se,pfnList in seFilesDict.items():
      seTotal = len(pfnList)
      self.log.verbose(' %s SURLs found from catalog for LocalSE %s' %(seTotal,se))
      for pfn in pfnList:
        self.log.debug('%s %s' % (se,pfn))

    # Can now start to resolve turls... finally
    resolvedData = {}
    for se,pfnList in seFilesDict.items():
      result = self.rm.getPhysicalFileAccessUrl(pfnList,se)
      self.log.debug(result)
      if not result['OK']:
        self.log.warn(result['Message'])
        return result

      badTURLCount = 0
      badTURLs = []
      seResult = result['Value']

      if seResult.has_key('Failed'):
        for pfn,cause in seResult['Failed'].items():
          badTURLCount+=1
          badTURLs.append('%s Problem: %s' %(pfn,cause))

      if seResult.has_key('Successful'):
        for pfn,turl in seResult['Successful'].items():
          if not turl:
            badTURLCount+=1
            badTURLs.append('%s problem: null TURL returned' %(pfn))

      if badTURLCount:
        self.log.warn('Job Wrapper found %s problematic TURL(s) for job %s' % (badLFNCount,self.jobID))
        param = string.join(badTURLs,'\n')
        self.log.info(param)
        result = self.__setJobParam('MissingTURLs',param)
        return S_ERROR('TURL resoulution error')

      pfnTurlDict = seResult['Successful']
      for lfn,reps in replicas.items():
        for se,rep in reps.items():
          for pfn in pfnTurlDict.keys():
            if rep == pfn:
              turl = pfnTurlDict[pfn]
              resolvedData[lfn] = {'turl':turl,'pfn':pfn,'se':se}
              self.log.debug('Resolved %s %s\n %s\n %s' %(se,lfn,pfn,turl))

    #Must retrieve GUIDs from LFC for files
    guids = {}
    self.log.debug(resolvedData)
    lfns = resolvedData.keys()
    guidDict = self.fileCatalog.getFileMetadata(lfns)

    if not guidDict['OK']:
      self.log.warn(guidDict['Message'])
      return guidDict

    failed = guidDict['Value']['Failed']
    if failed:
      self.log.warn(failed)
      return failed

    for lfn,mdata in resolvedData.items():
      se = mdata['se']
      self.log.debug('Attempting to get GUID for %s %s' %(lfn,se))
      guids[lfn]=guidDict['Value']['Successful'][lfn]['GUID']

    self.log.debug(guids)
    for lfn,guid in guids.items():
      resolvedData[lfn]['guid'] = guid

    #Create POOL XML slice for applications
    self.log.debug(resolvedData)
    xmlResult = self.__createXMLSlice(resolvedData)
    if not xmlResult['OK']:
      self.log.warn(xmlResult)

    return S_OK('Input Data Resolved')

  #############################################################################
  def __createXMLSlice(self,dataDict):
    """Given a dictionary of resolved input data, this will create a POOL
       XML slice.
    """
    poolXMLCatName = 'pool_xml_catalog.xml'
    try:
      poolXMLCat = PoolXMLCatalog()
      self.log.verbose('Creating POOL XML slice')

      for lfn,mdata in dataDict.items():
        local = os.path.basename(mdata['pfn'])
        #lfn,pfn,size,se,guid tuple taken by POOL XML Catalogue
        if os.path.exists(local):
          poolXMLCat.addFile((lfn,os.path.abspath(local),0,mdata['se'],mdata['guid']))
        else:
          poolXMLCat.addFile((lfn,mdata['turl'],0,mdata['se'],mdata['guid']))

      xmlSlice = poolXMLCat.toXML()
      self.log.debug('POOL XML Slice is: ')
      self.log.debug(xmlSlice)
      poolSlice = open(poolXMLCatName,'w')
      poolSlice.write(xmlSlice)
      poolSlice.close()
      self.log.verbose('POOL XML Catalogue slice written to %s' %(poolXMLCatName))
      # Temporary solution to the problem of storing the SE in the Pool XML
      poolSlice_temp = open('%s.temp' %(poolXMLCatName),'w')
      xmlSlice = poolXMLCat.toXML(True)
      poolSlice_temp.write(xmlSlice)
      poolSlice_temp.close()
    except Exception,x:
      self.log.warn(str(x))
      return S_ERROR(x)

    return S_OK('POOL XML Slice created')

  #############################################################################
  def processJobOutputs(self,arguments):
    """Outputs for a job may be treated here.
    """
    self.__report('Completed','Uploading Job Outputs')
    jobArgs = arguments['Job']

    #first iteration of this, no checking of wildcards or oversize sandbox files etc.
    outputSandbox = []
    if jobArgs.has_key('OutputSandbox'):
      outputSandbox = jobArgs['OutputSandbox']
      self.log.verbose('OutputSandbox files are: %s' %(string.join(outputSandbox,', ')))
    outputData = []
    if jobArgs.has_key('OutputData'):
      outputData = jobArgs['OutputData']
      self.log.verbose('OutputData files are: %s' %(string.join(outputData,', ')))

    fileList = []
    missingFiles = []
    for local in outputSandbox:
      if os.path.exists(local):
        fileList.append(local)
      else:
        missingFiles.append(local)

    if missingFiles:
      self.__setJobParam('OutputSandbox','MissingFiles: %s' %(string.join(missingFiles,', ')))

    self.__report('Running','Uploading Output Sandbox')
    result = self.outputSandboxClient.sendFiles(self.jobID, fileList)
    if not result['OK']:
      self.log.debug('Output sandbox upload failed:')
      self.log.warn(result['Message'])

    if jobArgs.has_key('Owner'):
      owner = jobArgs['Owner']
    else:
      msg = 'Job has no owner specified'
      self.log.warn(msg)
      return S_OK(msg)

    if jobArgs.has_key('OutputSE'):
      outputSE = jobArgs['OutputSE']
    else:
      outputSE = 'CERN-USER' # should move to a default CS location

    if outputData:
      self.__transferOutputDataFiles(owner,outputData,outputSE)

    return S_OK()

  #############################################################################
  def __transferOutputDataFiles(self,owner,outputData,outputSE):
    """Performs the upload and registration in the LFC
    """
    self.log.debug('Uploading output data files')
    self.__report('Running','Uploading Output Data')
    self.log.verbose('Output data files %s to be uploaded to %s SE' %(string.join(outputData,', '),outputSE))
    for outputFile in outputData:
      if os.path.exists(outputFile):
        lfn = self.__getLFNfromOutputFile(owner,outputFile)
        upload = self.rm.putAndRegister(lfn, outputFile, outputSE)
        self.log.info(upload)
      else:
        self.log.warn('Output data file: %s is missing after execution' %(outputFile))

    return S_OK()

  #############################################################################
  def __getLFNfromOutputFile(self, owner, outputFile):
    """Provides a generic convention for VO output data
       files if no path is specified.
    """
    localfile = os.path.basename(string.replace(outputFile,"LFN:",""))
    lfn = outputFile
    if not re.search('^LFN:',outputFile):
      initial = owner[:1]
      lfn = '/'+self.vo+'/user/'+initial+'/'+owner+'/'+str(self.jobID)+'/'+localfile
    else:
      lfn = string.replace(outputFile,"LFN:","")

    return lfn

  #############################################################################
  def transferInputSandbox(self,inputSandbox):
    """Downloads the input sandbox for the job
    """
    sandboxFiles = []
    self.__report('Running','Downloading InputSandbox')
    for i in inputSandbox: sandboxFiles.append(os.path.basename(i))
    self.log.info('Downloading InputSandbox for job %s: %s' %(self.jobID,string.join(sandboxFiles)))

    if os.path.exists('%s/inputsandbox' %(self.root)):
      # This is a debugging tool
      # Get the file from local storage to debug Job Wrapper
      sandboxFiles.append('jobDescription.xml')
      for inputFile in sandboxFiles:
        if os.path.exists('%s/inputsandbox/%s' %(self.root,inputFile)):
          self.log.info('Getting InputSandbox file %s from local directory for testing' %(inputFile))
          shutil.copy(self.root+'/inputsandbox/'+inputFile,inputFile)
      result = S_OK(sandboxFiles)
    else:
      result =  self.inputSandboxClient.getSandbox(int(self.jobID))
      if not result['OK']:
        self.__report('Running','Failed Downloading InputSandbox')
        return S_ERROR('InputSandbox download failed for job %s and sandbox %s' %(self.jobID,sandboxFiles))

    self.log.verbose('Sandbox download result: %s' %(result))
    return result

  #############################################################################
  def finalize(self,arguments):
    """Perform any final actions to clean up after job execution.
    """
    self.__report('Done','Execution Complete')
    self.__cleanUp()
    return S_OK()

  #############################################################################
  def __cleanUp(self):
    """Cleans up after job processing. Can be switched off via environment
       variable DO_NOT_DO_JOB_CLEANUP or by JobWrapper configuration option.
    """
    if os.environ.has_key('DO_NOT_DO_JOB_CLEANUP') or not self.cleanUpFlag:
      cleanUp = False
    else:
      cleanUp = True

    os.chdir(self.root)
    if cleanUp:
      self.log.verbose('Cleaning up job working directory')
      if os.path.exists(self.jobID):
        shutil.rmtree(self.jobID)

  #############################################################################
  def __setInitialJobParameters(self,arguments):
    """Sets some initial job parameters
    """
    parameters = []
    if os.environ.has_key('EDG_WL_JOBID'):
      parameters.append(('EDG_WL_JOBID', os.environ['EDG_WL_JOBID']))
    if os.environ.has_key('GLITE_WMS_JOBID'):
      parameters.append(('GLITE_WMS_JOBID', os.environ['GLITE_WMS_JOBID']))

    ceArgs = arguments['CE']
    if ceArgs.has_key('LocalSE'):
      parameters.append(('AgentLocalSE',ceArgs['LocalSE']))
    if ceArgs.has_key('CompatiblePlatforms'):
      parameters.append(('AgentCompatiblePlatforms',string.join(ceArgs['CompatiblePlatforms'],',')))

    parameters.append (('PilotAgent',self.diracVersion))
    result = self.__setJobParamList(parameters)
    return result

  #############################################################################
  def __report(self,status,minorStatus):
    """Wraps around setJobStatus of state update client
    """
    jobStatus = self.jobReport.setJobStatus(int(self.jobID),status,minorStatus,'JobWrapper')
    self.log.debug('setJobStatus(%s,%s,%s,%s)' %(self.jobID,status,minorStatus,'JobWrapper'))
    if not jobStatus['OK']:
        self.log.warn(jobStatus['Message'])

    return jobStatus

  #############################################################################
  def __setJobParam(self,name,value):
    """Wraps around setJobParameter of state update client
    """
    jobParam = self.jobReport.setJobParameter(int(self.jobID),str(name),str(value))
    self.log.debug('setJobParameter(%s,%s,%s)' %(self.jobID,name,value))
    if not jobParam['OK']:
        self.log.warn(jobParam['Message'])

    return jobParam

  #############################################################################
  def __setJobParamList(self,value):
    """Wraps around setJobParameters of state update client
    """
    jobParam = self.jobReport.setJobParameters(int(self.jobID),value)
    self.log.debug('setJobParameters(%s,%s)' %(self.jobID,value))
    if not jobParam['OK']:
        self.log.warn(jobParam['Message'])

    return jobParam

###############################################################################
###############################################################################

class ExecutionThread(threading.Thread):

  #############################################################################
  def __init__(self,spObject,cmd,maxPeekLines):
    threading.Thread.__init__(self)
    self.cmd = cmd
    self.spObject = spObject
    self.outputLines = []
    self.maxPeekLines = maxPeekLines

  #############################################################################
  def run(self):
    cmd = self.cmd
    spObject = self.spObject
    pid = os.getpid()
    start = time.time()
    output = spObject.systemCall( cmd, callbackFunction = self.sendOutput, shell = True )
    EXECUTION_RESULT['Thread'] = output
    timing = time.time() - start
    EXECUTION_RESULT['PID']=pid
    EXECUTION_RESULT['Timing']=timing

  #############################################################################
  def sendOutput(self,stdid,line):
    self.outputLines.append(line)

  #############################################################################
  def getOutput(self,lines=0):
    if self.outputLines:
      size = len(self.outputLines)
      #reduce max size of output peeking
      if size > self.maxPeekLines:
        cut = size - self.maxPeekLines
        self.outputLines = self.outputLines[cut:]
      #restrict to smaller number of lines for regular
      #peeking by the watchdog
      if lines:
        size = len(self.outputLines)
        cut  = size - lines
        self.outputLines = self.outputLines[cut:]

      result = S_OK()
      result['Value'] = self.outputLines
    else:
      result = S_ERROR('No Job output found')

    return result

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#