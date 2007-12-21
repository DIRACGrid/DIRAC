########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/PilotAgent/Attic/LCGAgentDirector.py,v 1.3 2007/12/21 14:21:09 paterson Exp $
# File :   LCGAgentDirector.py
# Author : Stuart Paterson
########################################################################

"""  The Agent Director for LCG provides implementations for the submitJob()
     method called in the parent Agent Director class.

     Since ResourceBrokers / WM Systems are defined at the level of specific Grids,
     the invokation of the Agent Director instance is performed here.
"""

__RCSID__ = "$Id: LCGAgentDirector.py,v 1.3 2007/12/21 14:21:09 paterson Exp $"

from DIRACEnvironment                                        import DIRAC
from DIRAC.Core.Utilities.Subprocess                         import shellCall
from DIRAC.WorkloadManagementSystem.DB.JobDB                 import JobDB
from DIRAC.ConfigurationSystem.Client.LocalConfiguration     import LocalConfiguration
from DIRAC.WorkloadManagementSystem.PilotAgent.AgentDirector import AgentDirector
from DIRAC                                                   import S_OK, S_ERROR, gConfig, gLogger

import os, sys, re, string, time

class LCGAgentDirector(AgentDirector):

  #############################################################################
  def __init__(self, resourceBroker=None ):
    self.jobDB = JobDB()
    self.name = 'LCG'
    self.type = 'LCG'
    self.log  = gLogger
    self.root = '/opt/dirac'
    self.log.debug('Starting LCGAgentDirector')
    self.resourceBroker = resourceBroker
    self.pilotScript = 'dirac-pilot-lcg.py'
    self.section = '/DIRAC/WorkloadManagementSystem/PilotAgent/LCGAgentDirector'
    self.diracSetup = gConfig.getValue(self.section+'/Setup','LHCb-Development')
    self.confFile1 = None
    self.confFile2 = None
    AgentDirector.__init__(self,self.jobDB,resourceBroker)

  #############################################################################
  def submitJob(self,job,workingDirectory,siteList,cpuRequirement,inputSandbox=None,gridRequirements=None,executable=None,softwareTag=None):
    """ Submit Pilot Job to the LCG Resource Broker
    """
    self.log.debug('Preparing LCG pilot for job %s in %s' %(job,workingDirectory))
    confFiles = self.__writeConfFiles(job,workingDirectory)
    if not confFiles['OK']:
      return confFiles
    lcgJDL = self.__writeJDL(job,workingDirectory,siteList,cpuRequirement,inputSandbox,gridRequirements,executable,softwareTag)
    if not lcgJDL['OK']:
      return lcgJDL

    lcgJDLFile = lcgJDL['Value']

    self.log.info( '--- Executing edg-job-submit for %s' % job )
    proxyFile = '/opt/dirac/runit/LCGAgentDirector/proxy'
    self.log.info('Temporarily using %s ' %(proxyFile))
    os.environ['X509_USER_PROXY'] = proxyFile
    os.environ['GRID_PROXY_FILE'] = proxyFile
    cmd = "edg-job-submit -config %s --config-vo %s %s" % (self.confFile1,self.confFile2,lcgJDLFile)
    start = time.time()
    self.log.debug( cmd )
    result = shellCall(60,cmd)

    if not result['OK']:
      return result

    status = result['Value'][0]
    stdout = result['Value'][1]
    stderr = result['Value'][2]
    self.log.debug('Status = %s' %status)
    self.log.debug(stdout)
    self.log.debug(stderr)

    submittedPilot = None
    if status==0:
      failed = 1
      for line in string.split(stdout):
        m = re.search("(https:\S+)",line)
        if (m):
          lcg_id = m.group(1)
          submittedPilot = lcg_id
          self.log.info( '>>> LCG Reference %s for job %s' % ( job, lcg_id ) )
          subtime = time.time() - start
          self.log.verbose( '>>> LCG Submission time %.2fs' % subtime )
          failed = 0

      if failed:
        self.log.warn( '>>> LCG Submission Failed for Job %s with status %s' %(job,status))
        return S_ERROR('LCG Submission failed with status %s' %(status))

    return S_OK(submittedPilot)

  #############################################################################
  def __writeJDL(self,job,workingDirectory,siteList,cpuRequirement,inputSandbox,gridRequirements=None,executable=None,softwareTag=None):
    """ Implementation of the writeJdl() method for the LCG Resource Broker
        case. Prepares the LCG job JDL file.
    """
    lcgJDLFile = '%s/%s/%s.jdl' % (workingDirectory,job,job)
    self.log.debug( 'Writing LCG JDL file %s ' %lcgJDLFile)

    try:
      if os.path.exists(lcgJDLFile):
        os.remove(lcgJDLFile)

      lcgJDL = open(lcgJDLFile,'w')
      myPolicyTime = 'LCGTimeRef * 500 / other.GlueHostBenchmarkSI00 / 60'
      myPolicyTime = gConfig.getValue(self.section+'/MyPolicyTime',myPolicyTime)

      if executable:
        lcgJDL.write( 'Executable = "%s";\n'     % executable )
      else:
        lcgJDL.write( 'Executable = "%s";\n'     % self.pilotScript )

      lcgJDL.write( 'Arguments  = "%s %s";\n'  % (self.diracSetup,cpuRequirement) )
      lcgJDL.write( 'LCGTimeRef = %s ;\n'      % cpuRequirement )
      lcgJDL.write( 'MyPolicyTime = ( %s );\n' % myPolicyTime )

      requirements = ['other.GlueCEPolicyMaxCPUTime > MyPolicyTime','Rank > -2']
      requirements = gConfig.getValue(self.section+'/Requirements',requirements)

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
      rank = gConfig.getValue(self.section+'/Rank' , rank )
      fuzzy = gConfig.getValue(self.section+'/FuzzyRank','true')

      lcgJDL.write( 'Rank = %s ;\n' % rank )
      lcgJDL.write( 'FuzzyRank = %s;\n' %fuzzy )
      lcgJDL.write( 'StdOutput     = "std.out";\n' )
      lcgJDL.write( 'StdError      = "std.err";\n' )

      #Input Sandbox
      diracinstallPath = self.root+'/scripts/dirac-install'
      executablePath   = self.root+'/DIRAC/WorkloadManagementSystem/PilotAgent/'+self.pilotScript
      guessplatformPath = self.root+'/scripts/dirac-architecture'
      inputSandboxList = [diracinstallPath, executablePath, guessplatformPath]

      for inFile in inputSandbox: inputSandboxList.append(inFile)

      inputSandbox = string.join(inputSandboxList,'","')
      lcgJDL.write('InputSandbox = {"'+inputSandbox+'"}; \n')
      lcgJDL.write('OutputSandbox = {"std.out","std.err",".BrokerInfo","pilotOutput.log"};\n')
      lcgJDL.close()
      lcgJDL = open( lcgJDLFile, 'r' )
      self.log.debug( 'Contents of LCG JDL File... \n%s' % string.join(lcgJDL.readlines(),'') )
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
    self.log.debug('Writing configuration files for LCG job submission')
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
MyProxyServer = "myproxy.cern.ch"
]
    """ % (rbstring,curlyrbstring) )
    confFile2.close()

    if os.path.exists(self.confFile1) and os.path.exists(self.confFile2):
      return S_OK()
    else:
      return S_ERROR('Configuration files not written')

###############################################################################
if __name__ == "__main__":
  """ Main execution method.
  """
  agent = {}
  localCfg = LocalConfiguration()
  localCfg.setConfigurationForScript('LCGAgentDirector')
  localCfg.addMandatoryEntry( "/DIRAC/Setup" )
  localCfg.addDefaultEntry( "/DIRAC/Security/UseServerCertificate", "yes" )
  resultDict = localCfg.loadUserData()

  pollingTime = 100

  if not resultDict[ 'OK' ]:
    gLogger.warn( "There were errors when loading configuration", resultDict[ 'Message' ] )
    sys.exit(1)

  resourceBrokers = gConfig.getValue('LCGAgentDirector/ResourceBrokers','lhcb-lcg-rb04.cern.ch')
  if  not type(resourceBrokers)==type([]):
    resourceBrokers = resourceBrokers.split(',')
  for rb in resourceBrokers:
    gLogger.verbose( "Starting thread for RB %s" % rb )
    agent[rb] = LCGAgentDirector(rb)
    agent[rb].start()
    time.sleep(5)

  while ( 1 ):
    time.sleep(pollingTime)
    for rb,th in agent.items():
      if th.isAlive():
        gLogger.debug('Thread for RB %s is alive' %(rb))
      else:
        gLogger.debug('Thread isAlive() = %s' %(th.isAlive()))
        gLogger.warn('Thread for RB %s is dead, restarting ...' %(rb))
        th.start()

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#


