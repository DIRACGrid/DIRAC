########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/PilotAgent/Attic/gLitePilotDirector.py,v 1.14 2008/05/21 15:34:02 paterson Exp $
# File :   gLitePilotDirector.py
# Author : Stuart Paterson
########################################################################

"""  The Pilot Director for gLite provides implementations for the submitJob()
     method called in the parent Pilot Director class.

     Since ResourceBrokers / WM Systems are defined at the level of specific Grids,
     the invokation of the Pilot Director instance is performed here.
"""

__RCSID__ = "$Id: gLitePilotDirector.py,v 1.14 2008/05/21 15:34:02 paterson Exp $"

from DIRACEnvironment                                        import DIRAC
from DIRAC.Core.Utilities                                    import List
from DIRAC.Core.Utilities.Subprocess                         import shellCall
from DIRAC.WorkloadManagementSystem.PilotAgent.PilotDirector import PilotDirector
from DIRAC                                                   import S_OK, S_ERROR, gConfig, gLogger

import os, sys, re, string, time

class gLitePilotDirector(PilotDirector):

  #############################################################################
  def __init__(self, configPath, resourceBroker, mode ):
    self.type = mode
    self.resourceBroker = resourceBroker
    self.name = '%sPilotDirector' %(self.type)
    self.log = gLogger.getSubLogger(self.name)
    self.log.info('Starting %s for RB %s' %(self.name,self.resourceBroker))
    self.sectionPath = configPath
    self.diracRoot = gConfig.getValue( '/LocalSite/Root')
    self.pilotScript = gConfig.getValue(self.sectionPath+'/PilotScript','%s/DIRAC/WorkloadManagementSystem/PilotAgent/dirac-pilot' %(self.diracRoot))
    self.diracInstallScript = gConfig.getValue(self.sectionPath+'/DIRACInstallScript','%s/scripts/dirac-install' %(self.diracRoot))
    self.archScript = gConfig.getValue(self.sectionPath+'/ArchitectureScript','%s/scripts/dirac-architecture' %(self.diracRoot))
    self.voSoftwareDir = gConfig.getValue(self.sectionPath+'/VOSoftware','VO_LHCB_SW_DIR')
    self.loggingService = gConfig.getValue(self.sectionPath+'/LoggingService','lb101.cern.ch')
    self.diracSetup = gConfig.getValue(self.sectionPath+'/Setup','LHCb-Development')
    self.enableListMatch = gConfig.getValue(self.sectionPath+'/EnableListMatch',1)
    self.listMatchDelay = gConfig.getValue(self.sectionPath+'/ListMatchDelay',15*60)
    self.diracTag = gConfig.getValue(self.sectionPath+'/DIRACDistributionTag','CCRC08-v4')
    self.confFile1 = None
    self.pilotDirConfig = '/%s/%s' % ( '/'.join( List.fromChar(configPath, '/' )[:-1] ), 'PilotDirector')
    self.jobsWithoutCEs = {}
    PilotDirector.__init__(self,self.pilotDirConfig,self.resourceBroker)

  #############################################################################
  def __checkProxy(self):
    """Print some debugging information for the current proxy.
    """
    proxyInfo = shellCall(30,'voms-proxy-info -all')
    status = proxyInfo['Value'][0]
    stdout = proxyInfo['Value'][1]
    stderr = proxyInfo['Value'][2]
    self.log.verbose('Status %s' %status)
    self.log.verbose(stdout)
    self.log.verbose(stderr)

  #############################################################################
  def __exeCommand(self,cmd):
    """Runs a submit / list-match command and prints debugging information.
    """
    start = time.time()
    self.log.verbose( cmd )
    result = shellCall(60,cmd)

    status = result['Value'][0]
    stdout = result['Value'][1]
    stderr = result['Value'][2]
    self.log.verbose('Status = %s' %status)
    self.log.verbose(stdout)
    if stderr:
      self.log.warn(stderr)
    result['Status']=status
    result['StdOut']=stdout
    result['StdErr']=stderr
    subtime = time.time() - start
    result['Time']=subtime
    return result

  #############################################################################
  def submitJob(self,job,workingDirectory,siteList,cpuRequirement,ownerGroup,inputSandbox=None,gridRequirements=None,executable=None,softwareTag=None):
    """ Submit Pilot Job to the gLite Resource Broker
    """

    self.log.verbose('Preparing %s pilot for job %s in %s' %(self.type,job,workingDirectory))
    confFiles = self.__writeConfFiles(job,workingDirectory)
    if not confFiles['OK']:
      return confFiles
    gLiteJDL = self.__writeJDL(job,workingDirectory,siteList,cpuRequirement,ownerGroup,inputSandbox,gridRequirements,executable,softwareTag)
    if not gLiteJDL['OK']:
      return gLiteJDL

    self.__checkProxy() # debuggging tool
    gLiteJDLFile = gLiteJDL['Value']['JDLFile']
    gLiteJDLRequirements = gLiteJDL['Value']['JDLRequirements']

    #list-match before each submission
    listMatchResult = self.__checkCEsForJob(job,gLiteJDLFile)
    if not listMatchResult['OK']:
      return listMatchResult

    self.log.info( '--- Executing glite-wms-job-submit for %s' % job )

    cmd = "glite-wms-job-submit -a -c %s %s" % (self.confFile1,gLiteJDLFile)
    result = self.__exeCommand(cmd)

    if not result['OK']:
      self.log.warn(result)
      return result

    status = result['Status']
    stdout = result['StdOut']
    stderr = result['StdErr']
    subtime = result['Time']
    self.log.info( '>>> gLite Submission time %.2fs' % subtime )

    submittedPilot = None
    if status==0:
      failed = 1
      for line in string.split(stdout):
        m = re.search("(https:\S+)",line)
        if (m):
          glite_id = m.group(1)
          submittedPilot = glite_id
          self.log.info( '>>> gLite Reference %s for job %s' % ( glite_id, job ) )
          failed = 0

      if failed:
        return S_ERROR('>>> gLite Submission Failed for Job %s with status %s, no result found' %(job,status))
    else:
      self.log.warn( stdout )
      self.log.warn( stderr )
      return S_ERROR('>>> gLite Submission Failed for job %s with status %s' %(job,status))

    resultDict = {}
    resultDict['PilotReference'] = submittedPilot
    resultDict['PilotRequirements'] = gLiteJDLRequirements
    return S_OK(resultDict)

  #############################################################################
  def __writeJDL(self,job,workingDirectory,siteList,cpuRequirement,ownerGroup,
                 inputSandbox,gridRequirements=None,executable=None,softwareTag=None):
    """ Implementation of the writeJdl() method for the gLite Resource Broker
        case. Prepares the gLite job JDL file.
    """
    gLiteJDLFile = '%s/%s/%s.jdl' % (workingDirectory,job,job)
    self.log.verbose( 'Writing gLite JDL file %s ' %gLiteJDLFile)

    try:
      if os.path.exists(gLiteJDLFile):
        os.remove(gLiteJDLFile)

      gLiteJDL = open(gLiteJDLFile,'w')
      myPolicyTime = 'gLiteTimeRef * 500 / other.GlueHostBenchmarkSI00 / 60'
      myPolicyTime = gConfig.getValue(self.sectionPath+'/MyPolicyTime',myPolicyTime)

      if executable:
        gLiteJDL.write( 'Executable = "%s";\n'     % executable )
      else:
        gLiteJDL.write( 'Executable = "%s";\n'     % os.path.basename(self.pilotScript))

      gLiteJDL.write( 'Arguments  = "-o /DIRAC/Setup=%s -T %s -G %s -v %s";\n'  % (self.diracSetup,cpuRequirement,ownerGroup,self.diracTag) )
      gLiteJDL.write( 'gLiteTimeRef = %s ;\n'      % cpuRequirement )
      gLiteJDL.write( 'MyPolicyTime = ( %s );\n' % myPolicyTime )

      requirements = ['other.GlueCEPolicyMaxCPUTime > MyPolicyTime','Rank > -2']
      requirements = gConfig.getValue(self.sectionPath+'/Requirements',requirements)

      tmp_list = [ 'other.GlueCEInfoHostName == "'+s+'"' for s in siteList ]
      site_requirement = "( "+string.join(tmp_list,"\n      || ")+"\n    )"
      requirements.append(site_requirement)

      if softwareTag:
        for tag in softwareTag:
          requirements.append('Member("%s",other.GlueHostApplicationSoftwareRunTimeEnvironment)' % tag)

      if gridRequirements:
        requirements = []
        requirements.append(gridRequirements)

      reqString = string.join(requirements,"\n && ")
      gLiteJDL.write( 'Requirements = '+reqString+';\n')

      #Ranking of gLite jobs
      rank = '( other.GlueCEStateWaitingJobs == 0 ? other.GlueCEStateFreeCPUs * 10 / other.GlueCEInfoTotalCPUs : -other.GlueCEStateWaitingJobs * 4 / (other.GlueCEStateRunningJobs + 1 ) - 1 )'
      rank = gConfig.getValue(self.sectionPath+'/Rank' , rank )
      fuzzy = gConfig.getValue(self.sectionPath+'/FuzzyRank','true')

      gLiteJDL.write( 'Rank = %s ;\n' % rank )
      gLiteJDL.write( 'FuzzyRank = %s;\n' %fuzzy )
      gLiteJDL.write( 'StdOutput     = "std.out";\n' )
      gLiteJDL.write( 'StdError      = "std.err";\n' )

      #Input Sandbox
      diracinstallPath = self.diracInstallScript
      executablePath = self.pilotScript
      guessplatformPath = self.archScript
      if executable:
        executablePath   = self.diracRoot+'/DIRAC/WorkloadManagementSystem/PilotAgent/'+executable

      inputSandboxList = [diracinstallPath, executablePath ]

      for inFile in inputSandbox: inputSandboxList.append(inFile)

      inputSandbox = string.join(inputSandboxList,'","')
      gLiteJDL.write('InputSandbox = {"'+inputSandbox+'"}; \n')
      gLiteJDL.write('OutputSandbox = {"std.out","std.err",".BrokerInfo","pilotOutput.log","wrappers.tar.gz"};\n')
      gLiteJDL.close()
      gLiteJDL = open( gLiteJDLFile, 'r' )
      self.log.verbose( 'Contents of gLite JDL File... \n%s' % string.join(gLiteJDL.readlines(),'') )
      gLiteJDL.close()

    except Exception, x:
      self.log.warn( 'Failed to create JDL file "%s"' % gLiteJDLFile )
      self.log.warn( str(x) )
      return S_ERROR(x)

    resultDict = {}
    resultDict['JDLFile'] = gLiteJDLFile
    resultDict['JDLRequirements'] = reqString
    return S_OK(resultDict)

  #############################################################################
  def __writeConfFiles(self,job,workingDirectory):
    """ Creates configuration files necessary for the gLite job submission
    """
    self.log.verbose('Writing configuration files for gLite job submission')
    self.confFile1 = '%s/%s/gliteLHCb1.conf' % (workingDirectory,job)

    confFile1 = open(self.confFile1,'w')
    confFile1.write("""
[
WmsClient = [
ErrorStorage="/var/tmp";
OutputStorage=  "/tmp";
ListenerStorage="/tmp";
virtualorganisation="lhcb";
requirements = other.GlueCEStateStatus == "Production";
RetryCount = 0;
ShallowRetryCount = 0;
WMProxyEndPoints = {"https://%s:7443/glite_wms_wmproxy_server"};
LBEndPoints = {"https://%s:9000"};
];
]
    """ %(self.resourceBroker,self.loggingService))
    confFile1.close()

    rbstring = '"%s:7772"' % self.resourceBroker
    curlyrbstring = '"%s:9000"' % self.resourceBroker

    if os.path.exists(self.confFile1):
      return S_OK()
    else:
      return S_ERROR('Configuration files not written')

  #############################################################################
  def __listMatchJob(self, job, jdlFile):
    """ Get available gLite CEs for Pilot Job
    """
    self.log.info( '--- Executing glite-wms-job-list-match for %s' % job )

    cmd = "glite-wms-job-list-match -a -c  %s %s" % (self.confFile1,jdlFile )
    result = self.__exeCommand(cmd)
    if not result['OK']:
      self.log.warn(result)
      return result

    status = result['Status']
    stdout = result['StdOut']
    stderr = result['StdErr']
    subtime = result['Time']
    self.log.info( '>>> gLite List-Match time %.2fs' % subtime )

    availableCEs = []
    if status == 0:
      failed = 1
      for line in string.split(stdout):
        m = re.search("(.*\/\S+)",line)
        if (m):
          ce = m.group(1)
          if not re.search('^https',ce):
            availableCEs.append(ce)

      if not availableCEs:
        self.log.warn( '>>> gLite List-Match failed to find CEs for Job %s' %job )
        self.log.warn( stdout )
        self.log.warn( stderr )

      self.log.info( '>>> %s gLite CEs for job %s' % (len(availableCEs),job) )

    else:
      self.log.warn( stdout )
      self.log.warn( stderr )
      return S_ERROR('>>> gLite List-Match Failed for job %s with status %s' %(job,status))

    return S_OK(availableCEs)

  #############################################################################
  def __checkCEsForJob(self,job,jdlFile):
    """ Method to check number of suitable CEs for job
        and prevent Pilot submission for jobs with no
        available CEs. Wraps around list-match command.
    """
    minimumCEs = 1 #this will only catch jobs that the RB cannot schedule
    noCEsAvailable = self.jobsWithoutCEs
    submitFlag = True

    if not self.enableListMatch:
      self.log.verbose( 'gLite List-Match is disabled by configuration' )
    else:
      check = self.jobDB.getJobParameters(int(job),['Available_CEs'])
      if not check['OK']:
        return check

      check = check['Value']
      self.log.verbose( check )

      if check.has_key('Available_CEs'):
        self.log.verbose( 'Available CEs already found for job: %s' % job )
        ces = check['Available_CEs'].strip()
        numberOfCEs = int(ces[0])

        if numberOfCEs < minimumCEs:
          self.log.verbose( 'Job %s has no available CEs' % job )
          if noCEsAvailable.has_key(job):
            lastCheck = noCEsAvailable[job] # in Python time units
            waitingTime = time.time() - lastCheck
            if waitingTime > self.listMatchDelay:
              waitingMins = round(waitingTime/60)
              self.log.verbose( 'Job has waited %s mins so retry list match' % waitingMins )
              getCEs = self.__listMatchJob(job,jdlFile)
              if not getCEs['OK']:
                return getCEs
              self.log.verbose(getCEs['Value'])
              newCEs = len(getCEs['Value'])
              if newCEs < minimumCEs:
                noCEsAvailable[job] = time.time()
                submitFlag = False
              else:
                self.log.verbose( 'Number of CEs has changed for Job: %s  updating Available_CEs' % job )
                self.jobDB.setJobParameter( job, 'Available_CEs' , '%s CEs returned from list-match on %s' %(newCEs,time.asctime()) )

            else:
              waitingTime = time.time() - lastCheck
              waitingTime = round(waitingTime/60)
              self.log.verbose( 'Skipping list match for Job %s, has waited %s mins so far' % (job,waitingTime) )
              submitFlag = False

          else:
            getCEs = self.__listMatchJob(job,jdlFile)  #if restarted, must redo list-match
            if not getCEs['OK']:
              return getCEs
            newCEs = len(getCEs['Value'])
            self.log.verbose(getCEs['Value'])
            if newCEs < minimumCEs:
              noCEsAvailable[job] = time.time()
              submitFlag = False
            else:
              self.log.verbose( 'Number of CEs has changed for Job: %s  updating Available_CEs' % job )
              self.jobDB.setJobParameter( job, 'Available_CEs' , '%s CEs returned from list-match on %s' %(newCEs,time.asctime()) )
      else:
        self.log.verbose( 'Job %s has not yet passed through list match' % job )
        getCEs = self.__listMatchJob(job,jdlFile)
        self.log.verbose(getCEs['Value'])
        if not getCEs['OK']:
          return getCEs
        numberOfCEs = len(getCEs['Value'])
        self.jobDB.setJobParameter( job, 'Available_CEs' , '%s CEs returned from list-match on %s' %(numberOfCEs,time.asctime()) )

        if numberOfCEs < minimumCEs:
          self.log.info('No CEs available for job  %s ' % job )
          submitFlag = False #do not submit pilots for these jobs
          noCEsAvailable[job] = time.time()

    self.jobsWithoutCEs = noCEsAvailable #update list of jobs with no CEs
    if submitFlag:
      return S_OK('Submission OK')
    else:
      return S_ERROR('Job %s has no CEs available' % job)

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
