########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/Agent/Attic/Director.py,v 1.41 2008/07/24 17:42:40 rgracian Exp $
# File :   Director.py
# Author : Stuart Paterson, Ricardo Graciani
########################################################################

"""  The Director Agent controls the submission of pilots via the
     PilotDirectors and Grid-specific PilotDirector sub-classes.
     This is a simple wrapper that performs the instantiation and monitoring
     of the PilotDirector instances and add workload to them via ThreadPool
     mechanism.

     From the base Agent class it uses the following configuration Parameters
       - WorkDir:
       - PollingTime:
       - ControlDirectory:
       - MaxCycles:

     The following parameters are searched for in WorkloadManagement/Director:
       - ThreadStartDelay:
       - JobSelectLimit:

     It looks in the WorkloadManagement/Director section for the
     list of Directors to be instantiated (one for each defined GridMiddleware)
       - GridMiddlewares

     It will use those Directors to submit pilots for each of the Supported SubmitPools
       - SubmitPools
     For every SubmitPool there must be a corresponding Section with the
     necessary paramenters:

       - GridMiddleware: <GridMiddleware>PilotDirector module from the PilotAgent directory will
               be used, currently LCG and gLite types are supported
       - Pool: if a dedicated Threadpool is desired for this SubmitPool

     For every GridMiddleware there must be a corresponding Section with the
     necessary paramenters:
       gLite:


       LCG:

       The following paramenters will be taken from the Director section if not
       present in the corresponding section
       - GenericPilotDN:
       - GenericPilotGroup:
       - DefaultPilotGridMiddleware:

"""

__RCSID__ = "$Id: Director.py,v 1.41 2008/07/24 17:42:40 rgracian Exp $"

import types, time

from DIRAC.Core.Base.Agent                        import Agent
from DIRAC.Core.Utilities                         import List
from DIRAC.WorkloadManagementSystem.DB.JobDB      import JobDB
from DIRAC.WorkloadManagementSystem.DB.JobLoggingDB        import JobLoggingDB
from DIRAC.WorkloadManagementSystem.DB.PilotAgentsDB       import PilotAgentsDB

from DIRAC.FrameworkSystem.Client.ProxyManagerClient       import gProxyManager

from DIRAC.Core.Security.CS                       import getPropertiesForGroup
from DIRAC.Core.Utilities.ClassAd.ClassAdLight    import ClassAd
from DIRAC.Core.Utilities.ThreadPool              import ThreadPool
from DIRAC                                        import S_OK, S_ERROR, gConfig, gLogger, abort, Source, systemCall, Time
import DIRAC

MAJOR_WAIT       = 'Waiting'
MINOR_SUBMIT     = 'Pilot Agent Submission'
MINOR_RESPONSE   = 'Pilot Agent Response'
MINOR_SUBMITTING = 'Director Submitting'

import os, sys, re, string, time, shutil

AGENT_NAME = 'WorkloadManagement/Director'

jobDB             = JobDB()
jobLoggingDB      = JobLoggingDB()
pilotAgentsDB     = PilotAgentsDB()

class Director(Agent):

  #############################################################################
  def __init__(self):
    """ Standard constructor for Agent
    """
    Agent.__init__(self,AGENT_NAME)
    # some default values:
    self.threadStartDelay = 5
    self.jobDicts = {}
    self.log   = gLogger
    # self.jobDB = JobDB()

  #############################################################################
  def initialize(self):
    """Sets defaults
    """
    result = Agent.initialize(self)
    if not result['OK']:
      return result

    self.directors = {}
    self.pools = {}
    self.__checkSubmitPools()

    return S_OK()

  def execute(self):
    """
      The Director execution method.
      0.- Check DB status and look for changes in the configuration
      1.- Retrieve jobs Waiting for pilot submission from the WMS
      (eventually this should be modified to work with TaskQueues), all pending
      jobs are retrieved on each execution cycle, but not all will be considered,
      sorted by their LastUpdate TimeStamp, older jobs will be handled first.
      2.- Loop over retrieved jobs.
        2.1.- Determine the requested SubmitPool for the Job. This is an expensive
        operation and it is an inmutable Attribute of the Job, therefore it is
        kept to avoid further iteration with DB.
        2.2.- Attempt to insert the job into the corresponding Queue of the
        TreadPool associated with the SubmitPool.
        2.3.- Stop considering a given Queue when first job find the Queue full
        2.4.- Iterate until all Qeueus are full
      3.- Reconfigure and Sleep
    """

    jobDB._connect()
    jobLoggingDB._connect()
    self.__checkSubmitPools()

    for job in self.__getJobs():

      if  not job in self.jobDicts:
        if not self.__getJobDict(job):
          continue
      else:
        # We already have seen this job, do not check again constant Job Properties
        pass

      # Now try to process the job
      self.log.verbose( 'Try to submit pilot for Job:', job )
      self.__submitPilot(job)

    return S_OK()

  def __submitPilot(self, job):
    """
      Try to insert the job in the corresponding Thread Pool, disable the Thread Pool
      until next itration once it becomes full
    """
    jobdict = self.jobDicts[job]
    submitPool = jobdict['SubmitPool']

    if submitPool == 'ANY':
      # It is a special case, all submitPool should be considered
      submitPools = List.randomize( self.defaultSubmitPools )
    else:
      submitPools = [submitPool]

    for submitPool in submitPools:
      self.log.verbose( 'Trying SubmitPool:',submitPool )

      if not submitPool in self.directors or not self.directors[submitPool]['isEnabled']:
        self.log.verbose( 'Not Enabled' )
        continue

      pool = self.pools[self.directors[submitPool]['pool']]
      director = self.directors[submitPool]['director']
      ret = pool.generateJobAndQueueIt( director.submitPilot,
                                        args=(jobdict, self ),
                                        oCallback=self.callBack,
                                        oExceptionCallback=director.exceptionCallBack,
                                        blocking=False )
      if not ret['OK']:
        # Disable submission until next iteration
        self.directors[submitPool]['isEnabled'] = False
      else:
        updateJobStatus( self.log, AGENT_NAME, job, MAJOR_WAIT, MINOR_SUBMITTING, logRecord=True )
        time.sleep( self.threadStartDelay )
        break

  def __getJobs(self):
    """
      Retrieve all Jobs in "Waiting Pilot Submission" from WMS
    """

    selection = {'Status':'Waiting','MinorStatus':MINOR_SUBMIT}
    result = jobDB.selectJobs(selection, orderAttribute='LastUpdateTime')
    if not result['OK']:
      self.log.warn(result['Message'])
      return []

    jobs = result['Value']
    if not jobs:
      self.log.info('No eligible jobs selected from DB')
    else:
      if len(jobs)>15:
        self.log.info( 'Selected jobs %s...' % string.join(jobs[0:14],', ') )
      else:
        self.log.info('Selected jobs %s' % string.join(jobs,', '))

    return jobs

  def __getJobDict(self, job):
    """
     Get job constant Properties and keep them local to avoid quering the DB each time
    """
    result = jobDB.getJobJDL(job)
    if not result['OK']:
      self.log.error(result['Message'])
      updateJobStatus( self.log, AGENT_NAME, job, 'Failed', 'No Job JDL Available', logRecord=True)
      return False

    # FIXME: this should be checked by the JobManager and the Optimizers
    jdl = result['Value']
    classAdJob = ClassAd(jdl)
    if not classAdJob.isOK():
      self.log.error( 'Illegal JDL for job:', job )
      updateJobStatus( self.log, AGENT_NAME, job, 'Failed', 'Job JDL Illegal', logRecord=True )
      return False

    jobDict = {'JobID': job}
    jobDict['JDL'] = jdl

    JobJDLStringMandatoryAttributes = [ 'SubmitPool', 'Requirements' ]

    JobJDLStringAttributes = [ 'PilotType', 'GridExecutable' ]

    JobJDLListAttributes = [ 'SoftwareTag', 'Site', 'BannedSites', 'GridRequiredCEs' ]

    JobJDLIntAttributes = [ 'MaxCPUTime' ]

    for attr in JobJDLStringMandatoryAttributes:
      jobDict[attr] = stringFromClassAd(classAdJob, attr)
      # FIXME: this should be checked by the JobManager and the Optimizers
      if not jobDict[attr]:
        self.log.error( '%s not defined for job:' % attr, job )
        updateJobStatus( self.log, AGENT_NAME, job, 'Failed', 'No %s Specified' % attr, logRecord=True )
        return False

    for attr in JobJDLStringAttributes:
      jobDict[attr] = stringFromClassAd(classAdJob, attr)

    for attr in JobJDLListAttributes:
      jobDict[attr] = stringFromClassAd(classAdJob, attr)
      jobDict[attr] = string.replace( string.replace(
                                               jobDict[attr], '{', '' ), '}', '' )
      jobDict[attr] = List.fromChar( jobDict[attr] )

    if 'Site' in jobDict and jobDict['Site'] == [ 'ANY' ]:
      del jobDict['Site']

    for attr in JobJDLIntAttributes:
      jobDict[attr] = intFromClassAd(classAdJob, attr)

    # Check now Job Attributes
    ret = jobDB.getJobAttributes(job)
    if not ret['OK']:
      self.log.warn(result['Message'])
      return False
    attributes = ret['Value']

    # FIXME: this should be checked by the JobManager and the Optimizers
    for attr in ['Owner','OwnerDN','JobType','OwnerGroup']:
      if not attributes.has_key(attr):
        updateJobStatus( self.log, AGENT_NAME, job, 'Failed', '%s Undefined' % attr, logRecord=True )
        self.log.error( 'Missing Job Attribute "%s":' %attr, job )
        return False
      jobDict[attr] = attributes[attr]

    currentStatus = attributes['Status']
    if not currentStatus == MAJOR_WAIT:
      self.log.verbose('Job has changed status to %s and will be ignored:' % currentStatus, job )
      return False

    currentMinorStatus = attributes['MinorStatus']
    if not currentMinorStatus == MINOR_SUBMIT:
      self.log.verbose('Job has changed minor status to %s and will be ignored:' % currentMinorStatus, job )
      return False

    self.jobDicts[job] = jobDict

    self.log.verbose('JobID: %s' % job)
    self.log.verbose('Owner: %s' % jobDict['Owner'])
    self.log.verbose('OwnerDN: %s' % jobDict['OwnerDN'])
    self.log.verbose('JobType: %s' % jobDict['JobType'])
    self.log.verbose('MaxCPUTime: %s' % jobDict['MaxCPUTime'])
    self.log.verbose('Requirements: %s' % jobDict['Requirements'])

    return True

  def __checkSubmitPools(self):
    # this method is called at initalization and at the begining of each execution
    # in this way running parameters can be dynamically changed via the remote
    # configuration.

    # First update common Configuration for all Directors
    self.__configureDirector()

    # Now we need to initialize one thread for each Director in the List,
    # and check its configuration:
    submitPools = gConfig.getValue( self.section+'/SubmitPools', [] )
    self.defaultSubmitPools = gConfig.getValue( self.section+'/DefaultSubmitPools', [] )

    for submitPool in submitPools:
      # check if the Director is initialized, then reconfigure
      if submitPool in self.directors:
        self.__configureDirector(submitPool)
      else:
        # instantiate a new Director
        self.__createDirector(submitPool)

      # Now enable the director for this iteration, if any RB is defined
      if submitPool in self.directors and self.directors[submitPool]['director'].resourceBrokers:
        self.directors[submitPool]['isEnabled'] = True

    # Now remove directors that are not Enable (they have been used but are no
    # longer required in the CS).
    pools = []
    for submitPool in self.directors.keys():
      if not self.directors[submitPool]['isEnabled']:
        self.log.info( 'Deleting Director for SubmitPool:', submitPool )
        director = self.directors[submitPool]['director']
        del self.directors[submitPool]
        del director
      else:
        pools.append( self.directors[submitPool]['pool'] )

    # Finally delete ThreadPools that are no longer in use
    for pool in self.pools:
      if pool != 'Default' and not pool in pools:
        pool = self.pools[pool]
        # del pool
        # del self.pools[pool]

  def __createDirector(self,submitPool):
    """
     Instantiate a new PilotDirector for the given SubmitPool
    """

    self.log.info( 'Creating Director for SubmitPool:', submitPool )
    # 1. check the GridMiddleware
    directorGridMiddleware = gConfig.getValue( self.section+'/'+submitPool+'/GridMiddleware','' )
    if not directorGridMiddleware:
      self.log.error( 'No Director GridMiddleware defined for SubmitPool:', submitPool )
      return

    directorName = '%sPilotDirector' % directorGridMiddleware

    try:
      self.log.info( 'Instantiating Director Object:', directorName )
      director = eval( '%s( )' %  ( directorName ) )
    except Exception,x:
      self.log.exception(x)
      return

    self.log.info( 'Director Object instantiated:', directorName )

    # 2. check the requested ThreadPool (it not defined use the default one)
    directorPool = gConfig.getValue( self.section+'/'+submitPool+'/Pool','Default' )
    if not directorPool in self.pools:
      self.log.info( 'Adding Thread Pool:', directorPool)
      poolName = self.__addPool( directorPool )
      if not poolName:
        self.log.error( 'Can not create Thread Pool:', directorPool )
        return

    # 3. add New director
    self.directors[ submitPool ] = { 'director': director,
                                     'pool': directorPool,
                                     'isEnabled': False,
                                   }

    self.log.verbose( 'Created Director for SubmitPool', submitPool )

    return

  def __configureDirector( self, submitPool=None ):
    # Update Configuration from CS
    # if submitPool == None then, only the do it for the Director
    # else do it for the PilotDirector of that SubmitPool
    if not submitPool:
      # This are defined by the Base Class and thus will be available
      # on the first call
      self.workDir     = gConfig.getValue( self.section+'/WorkDir', self.workDir )
      self.pollingTime = gConfig.getValue( self.section+'/PollingTime', self.pollingTime )
      self.controlDir  = gConfig.getValue( self.section+'/ControlDirectory', self.controlDir )
      self.maxcount    = gConfig.getValue( self.section+'/MaxCycles', self.maxcount )

      # Default values are defined on the __init__
      self.threadStartDelay = gConfig.getValue(self.section+'/ThreadStartDelay', self.threadStartDelay )

      # By default disable all directors
      for director in self.directors:
        self.directors[director]['isEnabled'] = False

    else:
      if submitPool not in self.directors:
        abort(-1)
      director = self.directors[submitPool]['director']
      
      # Pass reference to our CS section so that defaults can be taken from there
      director.configure( self.section, submitPool )
      
      # Enable director for pilot submission
      self.directors[submitPool]['isEnabled'] = True

  def __addPool(self, poolName):
    # create a new thread Pool, by default it has 2 executing threads and 40 requests
    # in the Queue
    # FIXME: get from CS
    if not poolName:
      return None
    if poolName in self.pools:
      return None
    pool = ThreadPool( 0,2,40 )
    pool.daemonize()
    self.pools[poolName] = pool
    return poolName

  def callBack(self, threadedJob, submitResult):
    if not submitResult['OK']:
      self.log.verbose( submitResult['Message'] )
    else:
      self.jobDicts[submitResult['Value']['JobID']] = submitResult['Value']


def stringFromClassAd( classAd, name ):
  value = ''
  if classAd.lookupAttribute( name ):
    value = string.replace(classAd.get_expression( name ), '"', '')
  return value

def intFromClassAd( classAd, name ):
  value = 0
  if classAd.lookupAttribute( name ):
    value = int(string.replace(classAd.get_expression( name ), '"', ''))
  return value


def updateJobStatus( logger, name, jobID, majorStatus, minorStatus=None, logRecord=False ):
  """This method updates the job status in the JobDB.
  """
  # FIXME: this log entry should go into JobDB
  logger.verbose("jobDB.setJobAttribute( %s, 'Status', '%s', update=True )" % ( jobID, majorStatus ) )
  result = jobDB.setJobAttribute( jobID, 'Status', majorStatus, update=True )

  if result['OK']:
    if minorStatus:
      # FIXME: this log entry should go into JobDB
      logger.verbose("jobDB.setJobAttribute( %s, 'MinorStatus', '%s', update=True)" % ( jobID, minorStatus) )
      result = jobDB.setJobAttribute( jobID, 'MinorStatus', minorStatus, update=True )

  if logRecord and result['OK']:
    result = jobLoggingDB.addLoggingRecord( jobID, majorStatus, minorStatus, source=name )

  if not result['OK']:
    logger.error(result['Message'])
    return False

  return True


# Some reasonable Defaults
DIRAC_PILOT   = os.path.join( DIRAC.rootPath, 'DIRAC', 'WorkloadManagementSystem', 'PilotAgent', 'dirac-pilot' )
DIRAC_INSTALL = os.path.join( DIRAC.rootPath, 'scripts', 'dirac-install' )
DIRAC_VERSION = 'Production'
DIRAC_VERSION = 'HEAD'
DIRAC_SETUP   = 'LHCb-Development'

ENABLE_LISTMATCH = 1
LISTMATCH_DELAY  = 5
GRIDENV          = ''
PILOT_DN         = '/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=paterson/CN=607602/CN=Stuart Paterson'
PILOT_DN         = '/DC=es/DC=irisgrid/O=ecm-ub/CN=Ricardo-Graciani-Diaz'
PILOT_GROUP      = 'lhcb_pilot'
TIME_POLICY      = 'TimeRef * 500 / other.GlueHostBenchmarkSI00 / 60'
REQUIREMENTS     = ['Rank > -2']
RANK             = '( other.GlueCEStateWaitingJobs == 0 ? other.GlueCEStateFreeCPUs * 10 / other.GlueCEInfoTotalCPUs : -other.GlueCEStateWaitingJobs * 4 / (other.GlueCEStateRunningJobs + 1 ) - 1 )'
FUZZY_RANK       = 'true'


LOGGING_SERVER   = 'lb101.cern.ch'

class PilotDirector:
  def __init__(self):
    self.log = gLogger.getSubLogger('%sPilotDirector' % self.gridMiddleware)
    if not  'log' in self.__dict__:
      self.log = gLogger.getSubLogger('PilotDirector')
    self.log.info('Initialized')
    self.listMatch = {}

  def configure(self, csSection, submitPool ):
    """
     Here goes common configuration for all PilotDirectors
    """
    self.install            = gConfig.getValue( csSection+'/InstallScript'     , DIRAC_INSTALL )
    """
     First define some defaults
    """
    self.diracSetup         = gConfig.getValue( '/DIRAC/Setup', DIRAC_SETUP )
    self.pilot              = DIRAC_PILOT
    self.diracVersion       = DIRAC_VERSION
    self.install            = DIRAC_INSTALL

    self.enableListMatch    = ENABLE_LISTMATCH
    self.listMatchDelay     = LISTMATCH_DELAY
    self.gridEnv            = GRIDENV
    self.loggingServers     = [ LOGGING_SERVER ]
    self.genericPilotDN     = PILOT_DN
    self.genericPilotGroup  = PILOT_GROUP
    self.timePolicy         = TIME_POLICY
    self.requirements       = REQUIREMENTS
    self.rank               = RANK
    self.fuzzyRank          = FUZZY_RANK

    self.configureFromSection( csSection )
    """
     Common Configuration can be overwriten for each GridMiddleware
    """
    mySection   = csSection+'/'+self.gridMiddleware
    self.configureFromSection( mySection )
    """
     And Again for each SubmitPool
    """
    mySection   = csSection+'/'+submitPool
    self.configureFromSection( mySection )

    self.log.info( '===============================================' )
    self.log.info( 'Configuration:' )
    self.log.info( '' )
    self.log.info( ' Install script: ', self.install )
    self.log.info( ' Pilot script:   ', self.pilot )
    self.log.info( ' DIRAC Version:  ', self.diracVersion )
    self.log.info( ' DIRAC Setup:    ', self.diracSetup )
    self.log.info( ' ListMatch:      ', self.enableListMatch )
    if self.enableListMatch:
      self.log.info( ' ListMatch Delay:', self.listMatchDelay )
    if self.gridEnv:
      self.log.info( ' GridEnv:        ', self.gridEnv )
    if self.resourceBrokers:
      self.log.info( ' ResourceBrokers:', ', '.join(self.resourceBrokers) )
    if self.loggingServers:
      self.log.info( ' LoggingServers:', ', '.join(self.loggingServers) )



  def configureFromSection( self, mySection ):

    self.pilot              = gConfig.getValue( mySection+'/PilotScript'       , self.pilot )
    self.diracVersion       = gConfig.getValue( mySection+'/DIRACVersion'      , self.diracVersion )
    self.diracSetup         = gConfig.getValue( mySection+'/Setup'             , self.diracSetup )
    self.install            = gConfig.getValue( mySection+'/InstallScript'     , self.install )

    self.enableListMatch    = gConfig.getValue( mySection+'/EnableListMatch'   , self.enableListMatch )
    self.listMatchDelay     = gConfig.getValue( mySection+'/ListMatchDelay'    , self.listMatchDelay )
    self.gridEnv            = gConfig.getValue( mySection+'/GridEnv     '      , self.gridEnv )
    self.resourceBrokers    = gConfig.getValue( mySection+'/ResourceBrokers'   , self.resourceBrokers )
    self.genericPilotDN     = gConfig.getValue( mySection+'/GenericPilotDN'    , self.genericPilotDN )
    self.genericPilotGroup  = gConfig.getValue( mySection+'/GenericPilotGroup' , self.genericPilotGroup )
    self.timePolicy         = gConfig.getValue( mySection+'/TimePolicy'        , self.timePolicy )
    self.requirements       = gConfig.getValue( mySection+'/Requirements'      , self.requirements )
    self.rank               = gConfig.getValue( mySection+'/Rank'              , self.rank )
    self.fuzzyRank          = gConfig.getValue( mySection+'/FuzzyRank'         , self.fuzzyRank )
    self.loggingServers     = gConfig.getValue( mySection+'/LoggingServers'    , self.loggingServers )

  def submitPilot(self, jobDict, director):
    """
      Submit pilot for the given job, this is done from the Thread Pool job
    """
    try:
      job = jobDict['JobID']
      ret = jobDB.getJobAttribute(job, 'Status')
      if ret['OK'] and not ret['Value'] == MAJOR_WAIT:
        self.log.warn( 'Job is no longer in %s Status:' % MAJOR_WAIT, job )
        return S_ERROR( 'Job is no longer in %s Status:' % MAJOR_WAIT )
  
      self.log.verbose( 'Submitting Pilot' )
      ceMask = self.__resolveCECandidates( jobDict )
      if not ceMask: return S_ERROR( 'No CE available for job' )
      self.workDir = director.workDir
      workingDirectory = os.path.join( self.workDir, job)
  
      if os.path.isdir( workingDirectory ):
        shutil.rmtree( workingDirectory )
      elif os.path.lexists( workingDirectory ):
        os.remove( workingDirectory )
  
      os.makedirs(workingDirectory)
  
      inputSandbox = []
      pilotOptions = []
      if jobDict['PilotType'].lower()=='private':
        self.log.verbose('Job %s will be submitted with a privat pilot' % job)
        ownerDN    = jobDict['OwnerDN']
        ownerGroup = jobDict['OwnerGroup']
        # User Group requirement
        pilotOptions.append( '-G %s' % jobDict['OwnerGroup'] )
        # check if group allows jobsharing
        ownerGroupProperties = getPropertiesForGroup( ownerGroup )
        if not 'JobSharing' in ownerGroupProperties:
          # Add Owner requirement to pilot
          pilotOptions.append( "-O '%s'" % ownerDN )
        pilotOptions.append( '-o /AgentJobRequirements/PilotType=private' )
        pilotOptions.append( '-o /Resources/Computing/InProcess/PilotType=private' )
      else:
        self.log.verbose('Job %s will be submitted with a generic pilot' % job)
        ownerDN    = self.genericPilotDN
        ownerGroup = self.genericPilotGroup
        pilotOptions.append( '-o /AgentJobRequirements/PilotType=generic' )
        pilotOptions.append( '-o /Resources/Computing/InProcess/PilotType=generic' )
  
      # Requested version of DIRAC
      pilotOptions.append( '-v %s' % self.diracVersion )
      # Requested CPU time
      pilotOptions.append( '-T %s' % jobDict['MaxCPUTime'] )
      # Default Setup. It may be overwriten by the Arguments in the Job JDL
      pilotOptions.append( '-o /DIRAC/Setup=%s' % self.diracSetup )
  
      # Write JDL
      retDict = self._prepareJDL( jobDict, pilotOptions, ceMask )
      jdl = retDict['JDL']
      jobRequirements = retDict['Requirements']
      pilots = retDict['Pilots']
      if not jdl:
        try:
          shutil.rmtree( workingDirectory )
        except:
          pass
        updateJobStatus( self.log, AGENT_NAME, job, MAJOR_WAIT, MINOR_SUBMIT, logRecord=True )
        return S_ERROR( 'Could not create JDL:', job )
  
      # get a valid proxy
      ret = gProxyManager.getPilotProxyFromDIRACGroup( ownerDN, ownerGroup )
      if not ret['OK']:
        self.log.error( ret['Message'] )
        self.log.error( 'Could not get proxy:', 'User "%s", Group "%s"' % ( ownerDN, ownerGroup ) )
        try:
          shutil.rmtree( workingDirectory )
        except:
          pass
        updateJobStatus( self.log, AGENT_NAME, job, MAJOR_WAIT, MINOR_SUBMIT, logRecord=True )
        return S_ERROR( 'Could not get proxy' )
      proxy = ret['Value']
      # Need to get VOMS extension for the later interctions with WMS
      ret = gProxyManager.getVOMSAttributes(proxy)
      if not ret['OK']:
        updateJobStatus( self.log, AGENT_NAME, job, MAJOR_WAIT, MINOR_SUBMIT, logRecord=True )
        return ret
      if not ret['Value']:
        updateJobStatus( self.log, AGENT_NAME, job, MAJOR_WAIT, MINOR_SUBMIT, logRecord=True )
        return S_ERROR( 'getPilotProxyFromDIRACGroup returns a proxy without VOMS Extensions' )
      vomsGroup = ret['Value'][0]
  
      # Check that there are available queues for the Job:
      if self.enableListMatch:
        availableCEs = []
        now = Time.dateTime()
        if not jobRequirements in self.listMatch:
          availableCEs = self._listMatch( proxy, job, jdl )
          if availableCEs != False:
            self.listMatch[jobRequirements] = {'LastListMatch': now}
            self.listMatch[jobRequirements]['AvailableCEs'] = availableCEs
        else:
          availableCEs = self.listMatch[jobRequirements]['AvailableCEs']
          if not Time.timeInterval( self.listMatch[jobRequirements]['LastListMatch'],
                                    self.listMatchDelay * Time.minute  ).includes( now ):
            availableCEs = self._listMatch( proxy, job, jdl )
            if availableCEs != False:
              self.listMatch[jobRequirements] = {'LastListMatch': now}
              self.listMatch[jobRequirements]['AvailableCEs'] = availableCEs
            else:
              del self.listMatch[jobRequirements]
          else:
            self.log.verbose( 'LastListMatch', self.listMatch[jobRequirements]['LastListMatch'] )
            self.log.verbose( 'AvailableCEs ', availableCEs )
  
        if type(availableCEs) == types.ListType :
          jobDB.setJobParameter( job, 'Available_CEs' , '%s CEs returned from list-match on %s' %
                                 ( len(availableCEs), Time.toString() ) )
        if not availableCEs:
          # FIXME: set Job Minor Status
          try:
            shutil.rmtree( workingDirectory )
          except:
            pass
          updateJobStatus( self.log, AGENT_NAME, job, MAJOR_WAIT, MINOR_SUBMIT, logRecord=True )
          return S_ERROR( 'No queue available for job' )
  
      # Now we are ready for the actual submission, so
  
      self.log.verbose('Submitting Pilot Agent for job:', job )
      submitRet = self._submitPilot( proxy, job, jdl )
      try:
        shutil.rmtree( workingDirectory )
      except:
        pass
      if not submitRet:
        updateJobStatus( self.log, AGENT_NAME, job, MAJOR_WAIT, MINOR_SUBMIT, logRecord=True )
        return S_ERROR( 'Pilot Submission Failed' )
      pilotReference, resourceBroker = submitRet
  
      # Now, update the job Minor Status
      if pilots > 1 :
        pilotReference = self._getChildrenReferences( proxy, pilotReference, job )
      else:
        pilotReference = [pilotReference]
      pilotAgentsDB.addPilotReference( pilotReference, job, ownerDN, vomsGroup, broker=resourceBroker, gridType=self.gridMiddleware, requirements=jobRequirements )
      ret = jobDB.getJobAttribute(job, 'Status')
      if ret['OK'] and ret['Value'] == MAJOR_WAIT:
        updateJobStatus( self.log, AGENT_NAME, job, MAJOR_WAIT, MINOR_RESPONSE, logRecord=True )
      else:
        self.log.warn( 'Job is no longer in %s Status:' % MAJOR_WAIT, job )

    except Exception,x:
      self.log.exception( 'Error during pilot submission',str(x)  )
      try:
        updateJobStatus( self.log, AGENT_NAME, job, MAJOR_WAIT, MINOR_RESPONSE, logRecord=True )
      except:
        pass
  
    return S_OK(jobDict)

  def _JobJDL(self, jobDict, pilotOptions, ceMask ):
    """
     The Job JDL is the same for LCG and GLite
    """
    if 'GridExecutable' in jobDict and jobDict['GridExecutable']:
      # if an executable is given it is assume to be in the same location as the pilot
      jobJDL = 'Executable     = "%s";\n' % os.path.basename( jobDict['GridExecutable'] )
      executable = os.path.join( os.path.dirname( self.pilot ), jobDict['GridExecutable'] )
    else:
      jobJDL = 'Executable     = "%s";\n' % os.path.basename( self.pilot )
      executable = self.pilot

    jobJDL += 'Arguments     = "%s";\n' % ' '.join( pilotOptions )

    jobJDL += 'TimeRef       = %s;\n' % jobDict['MaxCPUTime']

    jobJDL += 'TimePolicy    = ( %s );\n' % self.timePolicy

    requirements = list(self.requirements)
    requirements.append( 'other.GlueCEPolicyMaxCPUTime > TimePolicy' )

    siteRequirements = '\n || '.join( [ 'other.GlueCEInfoHostName == "%s"' % s for s in ceMask ] )
    requirements.append( "( %s\n )" %  siteRequirements )

    if 'SoftwareTag' in jobDict:
      for tag in jobDict['SoftwareTag']:
        requirements.append('Member( "%s" , other.GlueHostApplicationSoftwareRunTimeEnvironment )' % tag)

    jobRequirements = '\n && '.join( requirements )

    jobJDL += 'jobRequirements  = %s;\n' % jobRequirements

    jobJDL += 'Rank          = %s;\n' % self.rank
    jobJDL += 'FuzzyRank     = %s;\n' % self.fuzzyRank
    jobJDL += 'StdOutput     = "std.out";\n'
    jobJDL += 'StdError      = "std.err";\n'

    jobJDL += 'InputSandbox  = { "%s" };\n' % '", "'.join( [ self.install, executable ] )

    jobJDL += 'OutputSandbox = { "std.out", "std.err" };\n'

    self.log.verbose( jobJDL )

    return (jobJDL,jobRequirements)

  def _gridCommand(self, proxy, cmd):
    """
     Execute cmd tuple after sourcing GridEnv
    """
    gridEnv = dict(os.environ)
    if self.gridEnv:
      self.log.verbose( 'Sourcing GridEnv script:', self.gridEnv )
      ret = Source( 10, self.gridEnv )
      if not ret['OK']:
        self.log.error( 'Failed sourcing GridEnv:', ret['Message'] )
        return S_ERROR( 'Failed sourcing GridEnv' )
      if ret['stdout']: self.log.verbose( ret['stdout'] )
      if ret['stderr']: self.log.warn( ret['stderr'] )
      gridEnv = ret['outputEnv']

    ret = gProxyManager.dumpProxyToFile( proxy )
    if not ret['OK']:
      self.log.error( 'Failed to dump Proxy to file' )
      return ret
    gridEnv[ 'X509_USER_PROXY' ] = ret['Value']
    self.log.verbose( 'Executing', ' '.join(cmd) )
    return systemCall( 120, cmd, env = gridEnv )


  def exceptionCallBack(self, threadedJob, exceptionInfo ):
    self.log.exception( "Exception in Pilot Submission", lException = exceptionInfo )

  def _prepareJDL(self, jobDict, pilotOptions, ceMask ):
    """
      This method should be overridden in a subclass
    """
    self.log.error( '_prepareJDL() method should be implemented in a subclass' )
    sys.exit()

  def __resolveCECandidates( self, jobDict ):
    """
      Return a list of CE's
    """
    # assume user knows what they're doing and avoid site mask e.g. sam jobs
    if 'GridRequiredCEs' in jobDict and jobDict['GridRequiredCEs']:
      return jobDict['GridRequiredCEs']

    # Get the mask
    ret = jobDB.getSiteMask()
    if not ret['OK']:
      self.log.error( 'Can not retrieve site Mask from DB:', ret['Message'] )
      return []

    siteMask = ret['Value']
    if not siteMask:
      self.log.error( 'Site mask is empty' )
      return []

    self.log.verbose( 'Site Mask: %s' % ', '.join(siteMask) )

    # remove banned sites from siteMask
    for site in jobDict['BannedSites']:
      if site in siteMask:
        siteMask.remove(site)
        self.log.verbose('Removing banned site %s from site Mask' % site )

    # remove from the mask if a Site is given
    siteMask = [ site for site in siteMask if not jobDict['Site'] or site in jobDict['Site'] ]

    if not siteMask:
      # job can not be submitted
      # FIXME: the best is to use the ApplicationStatus to report this, this will allow
      # further iterations to consider the job as candidate without any further action
      jobDB.setJobStatus( jobDict['JobID'], application='No Candidate Sites in Mask' )
      return []

    self.log.info( 'Candidates Sites for Job %s:' % jobDict['JobID'], ', '.join(siteMask) )

    # Get CE's associates to the given site Names
    ceMask = []

    section = '/Resources/Sites/%s' % self.gridMiddleware
    ret = gConfig.getSections(section)
    if not ret['OK']:
      # To avoid duplicating sites listed in LCG for gLite for example.
      # This could be passed as a parameter from
      # the sub class to avoid below...
      section = '/Resources/Sites/LCG'
      ret = gConfig.getSections(section)

    if not ret['OK'] or not ret['Value']:
      self.log.error( 'Could not obtain CEs from CS', ret['Message'] )
      return []

    gridSites = ret['Value']
    for siteName in gridSites:
      if siteName in siteMask:
        ret = gConfig.getValue( '%s/%s/CE' % ( section, siteName), [] )
        if ret:
          ceMask.extend( ret )

    if not ceMask:
      self.log.error( 'No CE found for requested Sites:', ', '.join(siteMask) )

    return ceMask

  def parseListMatchStdout(self, proxy, cmd, job):
    """
      Parse List Match stdout to return list of matched CE's
    """
    self.log.verbose( 'Executing List Match for job:', job )

    start = time.time()
    ret = self._gridCommand( proxy, cmd )

    if not ret['OK']:
      self.log.error( 'Failed to execute List Match', ret['Message'] )
      return False
    if ret['Value'][0] != 0:
      self.log.error( 'Error executing List Match:', str(ret['Value'][0]) + '\n'.join( ret['Value'][1:3] ) )
      return False
    self.log.info( 'List Match Execution Time:', time.time()-start )

    stdout = ret['Value'][1]
    stderr = ret['Value'][2]
    availableCEs = []
    # Parse std.out
    for line in List.fromChar(stdout,'\n'):
      if re.search('jobmanager',line):
        # FIXME: the line has to be stripped from extra info
        availableCEs.append(line)

    if not availableCEs:
      self.log.info( 'List-Match failed to find CEs for Job:', job )
      self.log.info( stdout )
      self.log.info( stderr )
    else:
      self.log.debug( 'List-Match returns:', str(ret['Value'][0]) + '\n'.join( ret['Value'][1:3] ) )
      self.log.info( 'List-Match found %s CEs for Job:' % len(availableCEs), job )
      self.log.verbose( ', '.join(availableCEs) )

    return availableCEs

  def parseJobSubmitStdout(self, proxy, cmd, job):
    """
      Parse Job Submit stdout to return job reference
    """
    start = time.time()
    self.log.verbose( 'Executing Job Submit for job:', job )

    ret = self._gridCommand( proxy, cmd )

    if not ret['OK']:
      self.log.error( 'Failed to execute Job Submit', ret['Message'] )
      return False
    if ret['Value'][0] != 0:
      self.log.error( 'Error executing Job Submit:', str(ret['Value'][0]) + '\n'.join( ret['Value'][1:3] ) )
      return False
    self.log.info( 'Job Submit Execution Time:', time.time()-start )

    stdout = ret['Value'][1]
    stderr = ret['Value'][2]

    submittedPilot = None

    failed = 1
    rb = ''
    for line in List.fromChar(stdout,'\n'):
      m = re.search("(https:\S+)",line)
      if (m):
        glite_id = m.group(1)
        submittedPilot = glite_id
        if not rb:
          m = re.search("https://(.+):.+",glite_id)
          rb = m.group(1)
        self.log.info( 'Reference %s for job %s' % ( glite_id, job ) )
        failed = 0
    if failed:
      self.log.error( 'Job Submit returns no Reference:', str(ret['Value'][0]) + '\n'.join( ret['Value'][1:3] ) )
      return False

    return glite_id,rb

  def _writeJDL(self, filename, jdlList):
    try:
      f = open(filename,'w')
      f.write( '\n'.join( jdlList) )
      f.close()
    except Exception, x:
      self.log.exception( x )
      return ''

    return filename

class gLitePilotDirector(PilotDirector):
  def __init__(self):
    self.gridMiddleware = 'gLite'
    self.resourceBrokers    = ['wms101.cern.ch']
    PilotDirector.__init__(self)

  def configure(self, csSection, submitPool):
    """
     Here goes especific configuration for gLite PilotDirectors
    """
    PilotDirector.configure(self, csSection, submitPool )
    self.log.info( '' )
    self.log.info( '===============================================' )

  def _prepareJDL(self, jobDict, pilotOptions, ceMask ):
    """
      Write JDL for Pilot Submission
    """
    RBs = []
    for RB in self.resourceBrokers:
      RBs.append( '"https://%s:7443/glite_wms_wmproxy_server"' % RB )

    LBs = []
    for LB in self.loggingServers:
      LBs.append('"https://%s:9000"' % LB)

    nPilots = 1

    wmsClientJDL = """

RetryCount = 0;
ShallowRetryCount = 0;

Requirements = jobRequirements;
WmsClient = [
Requirements = other.GlueCEStateStatus == "Production";
ErrorStorage = "%s/Error";
OutputStorage = "%s/jobOutput";
# ListenerPort = 44000;
ListenerStorage = "%s/Storage";
VirtualOrganisation = "lhcb";
RetryCount = 0;
ShallowRetryCount = 0;
WMProxyEndPoints = { %s };
LBEndPoints = { %s };
MyProxyServer = "myproxy.cern.ch";
];
""" % ( self.workDir, self.workDir, self.workDir, ', '.join(RBs), ', '.join(LBs) )

    if jobDict['OwnerGroup'] == 'lhcb_prod':
      wmsClientJDL += """
JobType = "Parametric";
Parameters= 20;
ParameterStep =1;
ParameterStart = 0;
"""
      nPilots = 20


    (jobJDL , jobRequirements) = self._JobJDL( jobDict, pilotOptions, ceMask )

    jdl = os.path.join( self.workDir, '%s' % jobDict['JobID'], '%s.jdl' % jobDict['JobID'] )
    jdl = self._writeJDL( jdl, [jobJDL, wmsClientJDL] )

    return {'JDL':jdl, 'Requirements':jobRequirements, 'Pilots':nPilots }

  def _listMatch(self, proxy, job, jdl):
    """
     Check the number of available queues for the jobs to prevent submission
     if there are no matching resources.
    """
    cmd = [ 'glite-wms-job-list-match', '-a', '-c', '%s' % jdl, '%s' % jdl ]
    return self.parseListMatchStdout( proxy, cmd, job )

  def _submitPilot(self, proxy, job, jdl):
    """
     Submit pilot and get back the reference
    """
    cmd = [ 'glite-wms-job-submit', '-a', '-c', '%s' % jdl, '%s' % jdl ]
    return  self.parseJobSubmitStdout( proxy, cmd, job )

  def _getChildrenReferences(self, proxy, parentReference, job ):
    """
     Get reference for all Children
    """
    cmd = [ 'glite-wms-job-status', parentReference ]

    start = time.time()
    self.log.verbose( 'Executing Job Status for job:', job )

    ret = self._gridCommand( proxy, cmd )

    if not ret['OK']:
      self.log.error( 'Failed to execute Job Status', ret['Message'] )
      return False
    if ret['Value'][0] != 0:
      self.log.error( 'Error executing Job Status:', str(ret['Value'][0]) + '\n'.join( ret['Value'][1:3] ) )
      return False
    self.log.info( 'Job Status Execution Time:', time.time()-start )

    stdout = ret['Value'][1]
    stderr = ret['Value'][2]

    references = []

    failed = 1
    for line in List.fromChar(stdout,'\n'):
      m = re.search("Status info for the Job : (https:\S+)",line)
      if (m):
        glite_id = m.group(1)
        if glite_id not in references:
          references.append( glite_id )
        failed = 0
    if failed:
      self.log.error( 'Job Status returns no Child Reference:', str(ret['Value'][0]) + '\n'.join( ret['Value'][1:3] ) )
      return [parentReference]

    return references


class LCGPilotDirector(PilotDirector):
  def __init__(self):
    self.gridMiddleware = 'LCG'
    self.resourceBrokers    = ['rb123.cern.ch']
    PilotDirector.__init__(self)

  def configure(self, csSection, submitPool):
    """
     Here goes especific configuration for LCG PilotDirectors
    """
    PilotDirector.configure(self, csSection, submitPool )
    self.log.info( '' )
    self.log.info( '===============================================' )

  def _prepareJDL(self, jobDict, pilotOptions, ceMask ):
    """
      Write JDL for Pilot Submission
    """
    # RB = List.randomize( self.resourceBrokers )[0]
    LDs = []
    NSs = []
    LBs = []
    for RB in self.resourceBrokers:
      LDs.append( '"%s:9002"' % RB )
      LBs.append( '"%s:9000"' % RB )

    for LB in self.loggingServers:
      NSs.append( '"%s:7772"' % LB )

    LD = ', '.join(LDs)
    NS = ', '.join(NSs)
    LB = ', '.join(LBs)

    rbJDL = """
Requirements = jobRequirements && other.GlueCEStateStatus == "Production";
RetryCount = 0;
ErrorStorage = "%s/Error";
OutputStorage = "%s/jobOutput";
# ListenerPort = 44000;
ListenerStorage = "%s/Storage";
VirtualOrganisation = "lhcb";
LoggingTimeout = 30;
LoggingSyncTimeout = 30;
LoggingDestination = { %s };
# Default NS logger level is set to 0 (null)
# max value is 6 (very ugly)
NSLoggerLevel = 0;
DefaultLogInfoLevel = 0;
DefaultStatusLevel = 0;
NSAddresses = { %s };
LBAddresses = { %s };
MyProxyServer = "myproxy.cern.ch";
""" % (self.workDir, self.workDir, self.workDir, LD, NS, LB)

    jobJDL,jobRequirements = self._JobJDL( jobDict, pilotOptions, ceMask )

    jdl = os.path.join( self.workDir, '%s' % jobDict['JobID'], '%s.jdl' % jobDict['JobID'] )
    jdl = self._writeJDL( jdl, [jobJDL, rbJDL] )

    return {'JDL':jdl, 'Requirements':jobRequirements, 'Pilots': 1 }

  def _listMatch(self, proxy, job, jdl):
    """
     Check the number of available queues for the jobs to prevent submission
     if there are no matching resources.
    """
    cmd = ['edg-job-list-match','-c','%s' % jdl , '--config-vo', '%s' % jdl, '%s' % jdl]
    return self.parseListMatchStdout( proxy, cmd, job )

  def _submitPilot(self, proxy, job, jdl):
    """
     Submit pilot and get back the reference
    """
    cmd = [ 'edg-job-submit', '-c', '%s' % jdl, '--config-vo', '%s' % jdl, '%s' % jdl ]
    return self.parseJobSubmitStdout( proxy, cmd, job )
