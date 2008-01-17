########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/PilotAgent/Attic/LCGPilotDirector.py,v 1.2 2008/01/17 10:32:05 paterson Exp $
# File :   LCGPilotDirector.py
# Author : Stuart Paterson
########################################################################

"""  The Pilot Director for LCG provides implementations for the submitJob()
     method called in the parent Pilot Director class.

     Since ResourceBrokers / WM Systems are defined at the level of specific Grids,
     the invokation of the Pilot Director instance is performed here.
"""

__RCSID__ = "$Id: LCGPilotDirector.py,v 1.2 2008/01/17 10:32:05 paterson Exp $"

from DIRACEnvironment                                        import DIRAC
from DIRAC.Core.Utilities                                    import List
from DIRAC.Core.Utilities.Subprocess                         import shellCall
from DIRAC.WorkloadManagementSystem.PilotAgent.PilotDirector import PilotDirector
from DIRAC                                                   import S_OK, S_ERROR, gConfig, gLogger

import os, sys, re, string, time

class LCGPilotDirector(PilotDirector):

  #############################################################################
  def __init__(self, configPath, resourceBroker, mode ):
    self.type = mode
    self.resourceBroker = resourceBroker
    self.name = '%sPilotDirector' %(self.type)
    self.log = gLogger.getSubLogger(self.name)
    self.log.info('Starting %s for RB %s' %(self.name,self.resourceBroker))
    self.sectionPath = configPath
    self.diracRoot = gConfig.getValue(self.sectionPath+'/DIRACRoot','/opt/dirac')
    self.pilotScript = gConfig.getValue(self.sectionPath+'/PilotScript','%s/DIRAC/WorkloadManagementSystem/PilotAgent/dirac-pilot-lcg.py' %(self.diracRoot))
    self.diracInstallScript = gConfig.getValue(self.sectionPath+'/DIRACInstallScript','%s/scripts/dirac-install' %(self.diracRoot))
    self.archScript = gConfig.getValue(self.sectionPath+'/ArchitectureScript','%s/scripts/dirac-architecture' %(self.diracRoot))
    self.voSoftwareDir = gConfig.getValue(self.sectionPath+'VOSoftware','VO_LHCB_SW_DIR')
    self.diracSetup = gConfig.getValue(self.sectionPath+'/Setup','LHCb-Development')
    self.enableListMatch = gConfig.getValue(self.sectionPath+'/EnableListMatch',1)
    self.listMatchDelay = gConfig.getValue(self.sectionPath+'/ListMatchDelay',15*60)
    self.confFile1 = None
    self.confFile2 = None
    self.pilotDirConfig = '/%s/%s' % ( '/'.join( List.fromChar(configPath, '/' )[:-1] ), 'PilotDirector')
    self.jobsWithoutCEs = {}
    PilotDirector.__init__(self,configPath,self.resourceBroker)

  #############################################################################
  def __checkProxy(self):
    """Print some debugging information for the current proxy.
    """
    proxyInfo = shellCall(30,'grid-proxy-info -debug')
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
  def submitJob(self,job,workingDirectory,siteList,cpuRequirement,inputSandbox=None,gridRequirements=None,executable=None,softwareTag=None):
    """ Submit Pilot Job to the LCG Resource Broker
    """

    self.log.verbose('Preparing %s pilot for job %s in %s' %(self.type,job,workingDirectory))
    confFiles = self.__writeConfFiles(job,workingDirectory)
    if not confFiles['OK']:
      return confFiles
    lcgJDL = self.__writeJDL(job,workingDirectory,siteList,cpuRequirement,inputSandbox,gridRequirements,executable,softwareTag)
    if not lcgJDL['OK']:
      return lcgJDL

    self.__checkProxy() # debuggging tool

    #list-match before each submission
    listMatchResult = self.__checkCEsForJob()
    if not listMatchResult['OK']:
      return listMatchResult

    lcgJDLFile = lcgJDL['Value']
    self.log.info( '--- Executing edg-job-submit for %s' % job )

    cmd = "edg-job-submit -config %s --config-vo %s %s" % (self.confFile1,self.confFile2,lcgJDLFile)
    result = self.__exeCommand(cmd)

    if not result['OK']:
      self.log.warn(result)
      return result

    status = result['Status']
    stdout = result['StdOut']
    stderr = result['StdErr']
    subtime = result['Time']
    self.log.info( '>>> LCG Submission time %.2fs' % subtime )

    submittedPilot = None
    if status==0:
      failed = 1
      for line in string.split(stdout):
        m = re.search("(https:\S+)",line)
        if (m):
          lcg_id = m.group(1)
          submittedPilot = lcg_id
          self.log.info( '>>> LCG Reference %s for job %s' % ( lcg_id, job ) )
          failed = 0

      if failed:
        return S_ERROR('>>> LCG Submission Failed for Job %s with status %s, no result found' %(job,status))
    else:
      self.log.warn( stdout )
      self.log.warn( stderr )
      return S_ERROR('>>> LCG Submission Failed for job %s with status %s' %(job,status))

    return S_OK(submittedPilot)

  #############################################################################
  def __writeJDL(self,job,workingDirectory,siteList,cpuRequirement,inputSandbox,gridRequirements=None,executable=None,softwareTag=None):
    """ Implementation of the writeJdl() method for the LCG Resource Broker
        case. Prepares the LCG job JDL file.
    """
    lcgJDLFile = '%s/%s/%s.jdl' % (workingDirectory,job,job)
    self.log.verbose( 'Writing LCG JDL file %s ' %lcgJDLFile)

    try:
      if os.path.exists(lcgJDLFile):
        os.remove(lcgJDLFile)

      lcgJDL = open(lcgJDLFile,'w')
      myPolicyTime = 'LCGTimeRef * 500 / other.GlueHostBenchmarkSI00 / 60'
      myPolicyTime = gConfig.getValue(self.sectionPath+'/MyPolicyTime',myPolicyTime)

      if executable:
        lcgJDL.write( 'Executable = "%s";\n'     % executable )
      else:
        lcgJDL.write( 'Executable = "%s";\n'     % self.pilotScript )

      lcgJDL.write( 'Arguments  = "%s %s %s";\n'  % (self.diracSetup,cpuRequirement,self.voSoftwareDir) )
      lcgJDL.write( 'LCGTimeRef = %s ;\n'      % cpuRequirement )
      lcgJDL.write( 'MyPolicyTime = ( %s );\n' % myPolicyTime )

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

      lcgJDL.write( 'Requirements = '+string.join(requirements,"\n && ")+';\n')

      #Ranking of LCG jobs
      rank = '( other.GlueCEStateWaitingJobs == 0 ? other.GlueCEStateFreeCPUs * 10 / other.GlueCEInfoTotalCPUs : -other.GlueCEStateWaitingJobs * 4 / (other.GlueCEStateRunningJobs + 1 ) - 1 )'
      rank = gConfig.getValue(self.sectionPath+'/Rank' , rank )
      fuzzy = gConfig.getValue(self.sectionPath+'/FuzzyRank','true')

      lcgJDL.write( 'Rank = %s ;\n' % rank )
      lcgJDL.write( 'FuzzyRank = %s;\n' %fuzzy )
      lcgJDL.write( 'StdOutput     = "std.out";\n' )
      lcgJDL.write( 'StdError      = "std.err";\n' )

      #Input Sandbox
      diracinstallPath = self.diracInstallScript
      executablePath = self.pilotScript
      guessplatformPath = self.archScript
      if executable:
        executablePath   = self.diracRoot+'/DIRAC/WorkloadManagementSystem/PilotAgent/'+executable

      inputSandboxList = [diracinstallPath, executablePath, guessplatformPath]

      for inFile in inputSandbox: inputSandboxList.append(inFile)

      inputSandbox = string.join(inputSandboxList,'","')
      lcgJDL.write('InputSandbox = {"'+inputSandbox+'"}; \n')
      lcgJDL.write('OutputSandbox = {"std.out","std.err",".BrokerInfo","pilotOutput.log","wrappers.tar.gz"};\n')
      lcgJDL.close()
      lcgJDL = open( lcgJDLFile, 'r' )
      self.log.verbose( 'Contents of LCG JDL File... \n%s' % string.join(lcgJDL.readlines(),'') )
      lcgJDL.close()

    except Exception, x:
      self.log.warn( 'Failed to create JDL file "%s"' % lcgJDLFile )
      self.log.warn( str(x) )
      return S_ERROR(x)

    return S_OK(lcgJDLFile)

  #############################################################################
  def __writeConfFiles(self,job,workingDirectory):
    """ Creates configuration files necessary for the LCG job submission
    """
    self.log.verbose('Writing configuration files for LCG job submission')
    self.confFile1 = '%s/%s/edgLHCb1.conf' % (workingDirectory,job)
    confFile1 = open(self.confFile1,'w')
    confFile1.write("""
[
requirements = other.GlueCEStateStatus == "Production";
RetryCount = 0;
ErrorStorage = "/tmp";
OutputStorage = "/tmp/jobOutput";
ListenerPort = 44000;
ListenerStorage = "/tmp";
LoggingTimeout = 30;
LoggingSyncTimeout = 30;
LoggingDestination = "%s:9002";
# Default NS logger level is set to 0 (null)
# max value is 6 (very ugly)
NSLoggerLevel = 0;
DefaultLogInfoLevel = 0;
DefaultStatusLevel = 0;
DefaultVo = "lhcb";
]
    """ % self.resourceBroker )
    confFile1.close()

    rbstring = '"%s:7772"' % self.resourceBroker
    curlyrbstring = '"%s:9000"' % self.resourceBroker

    self.confFile2 = '%s/%s/edgLHCb2.conf' % (workingDirectory,job)
    confFile2 = open(self.confFile2,'w')
    confFile2.write("""
[
VirtualOrganisation = "lhcb";
NSAddresses = %s;
LBAddresses = %s;
MyProxyServer = "myproxy.cern.ch";
]
    """ % (rbstring,curlyrbstring) )
    confFile2.close()

    if os.path.exists(self.confFile1) and os.path.exists(self.confFile2):
      return S_OK()
    else:
      return S_ERROR('Configuration files not written')

  #############################################################################
  def __listMatchJob(self, job, jdlFile):
    """ Get available LCG CEs for Pilot Job
    """
    self.log.info( '--- Executing edg-job-list-match for %s' % job )

    cmd = "edg-job-list-match -config %s --config-vo %s %s" % (self.confFile1,self.confFile2,jdlFile )

    result = self.__exeCommand(cmd)
    if not result['OK']:
      self.log.warn(result)
      return result

    status = result['Status']
    stdout = result['StdOut']
    stderr = result['StdErr']
    subtime = result['Time']
    self.log.info( '>>> LCG List-Match time %.2fs' % subtime )

    availableCEs = []
    if status == 0:
      failed = 1
      for line in string.split(stdout):
        m = re.search("(.*\/\S+)",line)
        if (m):
          ce = m.group(1)
          availableCEs.append(ce)

      if not availableCEs:
        self.log.warn( '>>> LCG List-Match failed to find CEs for Job %s' %job )
        self.log.warn( stdout )
        self.log.warn( stderr )

      self.log.info( '>>> %s LCG CEs for job %s' % (job,len(availableCEs)) )

    else:
      self.log.warn( stdout )
      self.log.warn( stderr )
      return S_ERROR('>>> LCG List-Match Failed for job %s with status %s' %(job,status))

    return S_OK(availableCEs)

  #############################################################################
  def __checkCEsForJob(self,job):
    """ Method to check number of suitable CEs for job
        and prevent Pilot submission for jobs with no
        available CEs. Wraps around list-match command.
    """

    flag = self.protectRBs
    minimumCEs = 1 #this will only catch jobs that the RB cannot schedule
    noCEsAvailable = self.jobsWithoutCEs
    submitFlag = True

    if flag != True:
      self.log.verbose( 'LCG List-Match is disabled by configuration' )
    else:
      check = self.jobDB.getJobParameters(int(job),['Available_CEs'])
      if not check['OK']:
        return existingParam

      check = check['Value']
      self.log.verbose( check )

      if check['Available_CEs']:
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
              getCEs = self.__listMatchJob(job)
              if not getCEs['OK']:
                return getCEs
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
            getCEs = self.__listMatchJob(job)  #if restarted, must redo list-match
            if not getCEs['OK']:
              return getCEs
            newCEs = len(getCEs)
            if newCEs < minimumCEs:
              noCEsAvailable[job] = time.time()
              submitFlag = False
            else:
              self.log.verbose( 'Number of CEs has changed for Job: %s  updating Available_CEs' % job )
              self.jobDB.setJobParameter( job, 'Available_CEs' , '%s CEs returned from list-match on %s' %(newCEs,time.asctime()) )

      else:
        self.log.verbose( 'Job %s has not yet passed through list match' % job )
        getCEs = self.__listMatchJob(job)
        if not getCEs['OK']:
          return getCEs
        numberOfCEs = len(getCEs)
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