########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/Agent/TaskQueueDirector.py,v 1.7 2008/12/18 17:34:10 rgracian Exp $
# File :   TaskQueueDirector.py
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

     The following parameters are searched for in WorkloadManagement/TaskQueueDirector:
       - ThreadStartDelay:
       - SubmitPools: All the Submit pools that are to be initialized
       - DefaultSubmitPools: If no specific pool is requested, use these         

     It will use those Directors to submit pilots for each of the Supported SubmitPools
       - SubmitPools (see above)

     For every SubmitPool there must be a corresponding Section with the
     necessary paramenters:

       - GridMiddleware: <GridMiddleware>PilotDirector module from the PilotAgent directory will
               be used, currently LCG and gLite types are supported
       - Pool: if a dedicated Threadpool is desired for this SubmitPool

     For every GridMiddleware there must be a corresponding Section with the
     necessary paramenters:
       gLite:


       LCG:

       The following paramenters will be taken from the TaskQueueDirector section if not
       present in the corresponding SubmitPool section
       - GenericPilotDN:
       - GenericPilotGroup:


      The pilot submission logic is as follows:

        - Determine prioritySum: sum of the Priorities for all TaskQueues in the system.

        - Determine pilotsPerPriority: result of dividing the  number of pilots to submit 
          per itteration by the prioritySum.

        - For each TaskQueue determine a target number of pilots to submit:

          - Multiply the priority by pilotsPerPriority.
          - Apply a correction factor for proportional to maxCPU divided by CPU of the 
            TaskQueue ( double number of pilots will be submitted for a TaskQueue with 
            half CPU required ). To apply this correction the minimum CPU considered is 
            lowestCPUBoost.
          - Apply poisson statistics to determine the target number of pilots to submit 
            (even a TQ with a very small priorities will get a chance of getting 
            pilots submitted).
          - Determine a maximum number of "Waiting" pilots in the system: 
            ( 1 + extraPilotFraction ) * [No. of Jobs in TaskQueue] + extraPilots
          - Attempt to submit as many pilots a the minimum between both number.
          - Pilot submission request is inserted into a ThreadPool.

        - Report the sum of the Target number of pilots to be submitted.

        - Wait until the ThreadPool is empty.

        - Report the actual number of pilots submitted.

        In summary:

        All TaskQueues are considered on every itteration, pilots are submitted 
        statistically proportional to the priority of the TaskQueue, boosted for
        the TaskQueues with lower CPU requirements and limited by the difference 
        between the number of waiting jobs and the number of waiting pilots.
        
      Obsolete Job JDL Option:
        GridExecutable
        SoftwareTag
        SubmitPool (may want to recover it for SAM jobs)

"""
__RCSID__ = "$Id: TaskQueueDirector.py,v 1.7 2008/12/18 17:34:10 rgracian Exp $"

from DIRAC.Core.Base.AgentModule import AgentModule

from DIRAC.WorkloadManagementSystem.DB.JobLoggingDB        import JobLoggingDB
from DIRAC.WorkloadManagementSystem.DB.PilotAgentsDB       import PilotAgentsDB
from DIRAC.WorkloadManagementSystem.DB.TaskQueueDB         import TaskQueueDB
from DIRAC.WorkloadManagementSystem.DB.JobDB               import JobDB

from DIRAC.FrameworkSystem.Client.ProxyManagerClient       import gProxyManager
from DIRAC.Core.Security.CS                                import getPropertiesForGroup

from DIRAC.Core.Utilities.ThreadPool                       import ThreadPool
from DIRAC import S_OK, S_ERROR, gLogger, gConfig, List, Time, Source, systemCall

import DIRAC

jobLoggingDB      = JobLoggingDB()
pilotAgentsDB     = PilotAgentsDB()
taskQueueDB       = TaskQueueDB()
jobDB             = JobDB()

class TaskQueueDirector(AgentModule):

  def initialize(self):
    """ Standard constructor
    """
    import threading
    
    self.am_setOption( "PollingTime", 60.0 )

    self.am_setOption( "pilotsPerIteration", 20.0 )
    self.am_setOption( "lowestCPUBoost", 7200.0 )
    self.am_setOption( "extraPilotFraction", 0.2 )
    self.am_setOption( "extraPilots", 4 )
    
    self.am_setOption('ThreadStartDelay', 1 )
    self.am_setOption('SubmitPools', [] )
    self.am_setOption('DefaultSubmitPools', [] )
    

    self.am_setOption('minThreadsInPool', 0 )
    self.am_setOption('maxThreadsInPool', 2 )
    self.am_setOption('totalThreadsInPool', 40 )

    self.directors = {}
    self.pools = {}
    self.__checkSubmitPools()

    self.callBackLock = threading.Lock()

    return S_OK()

  def execute(self):
    """Main Agent code:
      1.- Query TaskQueueDB for existing TQs
      2.- Add their Priorities
      3.- Submit pilots
    """

    self.__checkSubmitPools()

    result = taskQueueDB.retrieveTaskQueues()
    if not result['OK']:
      self.log.error( 'Could not retrieve TaskQueues from TaskQueueDB', result['Message'] )
      return result
    taskQueueDict = result['Value']

    print taskQueueDict

    self.log.info( 'Found %s TaskQueues' % len(taskQueueDict) )

    if not taskQueueDict:
      self.log.info( 'No TaskQueue to Process' )
      return S_OK()

    prioritySum = 0
    for taskQueueID in taskQueueDict:
      taskQueueDict[taskQueueID]['TaskQueueID'] = taskQueueID
      prioritySum += taskQueueDict[taskQueueID]['Priority']

    self.log.info( 'Sum of Priorities %s' % prioritySum )

    if prioritySum <= 0:
      return S_ERROR('Wrong TaskQueue Priorities')

    pilotsPerPriority  = self.am_getOption('pilotsPerIteration') / prioritySum

    self.callBackLock.acquire()
    self.submittedPilots = 0
    self.callBackLock.release()
    self.toSubmitPilots = 0
    waitingStatusList = ['Submitted','Ready','Scheduled','Waiting']

    for taskQueueID in taskQueueDict:
      self.log.verbose( 'Processing TaskQueue', taskQueueID )
      
      result = pilotAgentsDB.countPilots( {'TaskQueueID': taskQueueID, 'Status': waitingStatusList} )
      if not result['OK']:
        self.log.error('Fail to get Number of Waiting pilots',result['Message'])
        waitingPilots = 0
      else:
        waitingPilots = result['Value']
        self.log.verbose( 'Waiting Pilots for TaskQueue %s:' % taskQueueID, waitingPilots )
      
      result = self.submitPilotsForTaskQueue( taskQueueDict[taskQueueID], waitingPilots, pilotsPerPriority )

      if result['OK']:
        self.toSubmitPilots += result['Value']

    self.log.info( 'Number of pilots to be Submitted %s' % self.toSubmitPilots )
    # Now wait until all Jobs in the Default ThreadPool are proccessed
    if 'Default' in self.pools:
      # only for those in "Default' thread Pool
      # for pool in self.pools:
      self.pools['Default'].processAllResults()

    self.log.info( 'Number of pilots Submitted %s' % self.submittedPilots )

    return S_OK()

  def submitPilotsForTaskQueue(self, taskQueueDict, waitingPilots, pilotsPerPriority ):

    from numpy.random import poisson

    taskQueueID = taskQueueDict['TaskQueueID']
    maxCPU = taskQueueDB.maxCPUSegments[-1]
    extraPilotFraction = self.am_getOption('extraPilotFraction')
    extraPilots = self.am_getOption('extraPilots')

    taskQueuePriority = taskQueueDict['Priority']
    self.log.verbose( 'Priority for TaskQueue %s:' % taskQueueID, taskQueuePriority )
    taskQueueCPU      = max( taskQueueDict['CPUTime'], self.am_getOption('lowestCPUBoost') )
    self.log.verbose( 'CPUTime  for TaskQueue %s:' % taskQueueID, taskQueueCPU )
    taskQueueJobs     = taskQueueDict['Jobs']
    self.log.verbose( 'Jobs in TaskQueue %s:' % taskQueueID, taskQueueJobs )

    # Determine number of pilots to submit, boosting TaskQueues with low CPU requirements
    pilotsToSubmit = poisson( pilotsPerPriority * taskQueuePriority * maxCPU / taskQueueCPU )
    # limit the number of pilots according to the number of waiting job in the TaskQueue 
    # and the number of already submitted pilots for that TaskQueue
    pilotsToSubmit = min( pilotsToSubmit, int( ( 1 + extraPilotFraction ) * taskQueueJobs ) + extraPilots - waitingPilots )
    if pilotsToSubmit <= 0: return S_OK( 0 ) 
    self.log.verbose( 'Submitting %s pilots for TaskQueue %s' % ( pilotsToSubmit,  taskQueueID ) )

    return self.__submitPilots( taskQueueDict, pilotsToSubmit )

  def __submitPilots(self, taskQueueDict, pilotsToSubmit ):
    """
      Try to insert the submission in the corresponding Thread Pool, disable the Thread Pool
      until next itration once it becomes full
    """
    # Check if an specific MiddleWare is required
    if 'GridMiddlewares' in taskQueueDict:
      submitPools = taskQueueDict['GridMiddlewares']
    else:
      submitPools = List.randomize( self.am_getOption('DefaultSubmitPools') )

    for submitPool in submitPools:
      self.log.verbose( 'Trying SubmitPool:',submitPool )

      if not submitPool in self.directors or not self.directors[submitPool]['isEnabled']:
        self.log.verbose( 'Not Enabled' )
        continue

      pool = self.pools[self.directors[submitPool]['pool']]
      director = self.directors[submitPool]['director']
      ret = pool.generateJobAndQueueIt( director.submitPilots,
                                        args=(taskQueueDict, pilotsToSubmit, self.workDir ),
                                        oCallback=self.callBack,
                                        oExceptionCallback=director.exceptionCallBack,
                                        blocking=False )
      if not ret['OK']:
        # Disable submission until next iteration
        self.directors[submitPool]['isEnabled'] = False
      else:
        time.sleep( self.am_getOption('ThreadStartDelay') )
        break

    return S_OK( pilotsToSubmit )

  def __checkSubmitPools(self):
    # this method is called at initalization and at the begining of each execution cycle
    # in this way running parameters can be dynamically changed via the remote
    # configuration.

    # First update common Configuration for all Directors
    self.__configureDirector()

    # Now we need to initialize one thread for each Director in the List,
    # and check its configuration:
    for submitPool in self.am_getOption( 'SubmitPools' ):
      # check if the Director is initialized, then reconfigure
      if submitPool in self.directors:
        self.__configureDirector(submitPool)
      else:
        # instantiate a new Director
        self.__createDirector(submitPool)

      # Now enable the director for this iteration, if some RB/WMS is defined
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
    # Comprobar esto
    directorGridMiddleware = self.am_getOption( submitPool + '/GridMiddleware', '' )
    if not directorGridMiddleware:
      self.log.error( 'No Director GridMiddleware defined for SubmitPool:', submitPool )
      return

    directorName = '%sPilotDirector' % directorGridMiddleware

    try:
      self.log.info( 'Instantiating Director Object:', directorName )
      director = eval( '%s( )' %  ( directorName ) )
    except Exception,x:
      self.log.exception( )
      return

    self.log.info( 'Director Object instantiated:', directorName )

    # 2. check the requested ThreadPool (if not defined use the default one)
    directorPool = self.am_getOption( submitPool + '/Pool', 'Default' )
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
    # if submitPool == None then, 
    #     disable all Directors
    # else 
    #    Update Configuration for the PilotDirector of that SubmitPool
    if not submitPool:
      self.workDir     = self.am_getOption( 'WorkDirectory' )      
      # By default disable all directors
      for director in self.directors:
        self.directors[director]['isEnabled'] = False

    else:
      if submitPool not in self.directors:
        DIRAC.abort(-1)
      director = self.directors[submitPool]['director']

      # Pass reference to our CS section so that defaults can be taken from there
      director.configure( self.am_getModuleParam('section'), submitPool )

      # Enable director for pilot submission
      self.directors[submitPool]['isEnabled'] = True

  def __addPool(self, poolName):
    # create a new thread Pool, by default it has 2 executing threads and 40 requests
    # in the Queue

    if not poolName:
      return None
    if poolName in self.pools:
      return None
    pool = ThreadPool( self.am_getOption('minThreadsInPool'),
                       self.am_getOption('maxThreadsInPool'),
                       self.am_getOption('totalThreadsInPool') )
    # Daemonize except "Default" pool
    if poolName != 'Default':
      pool.daemonize()
    self.pools[poolName] = pool
    return poolName

  def callBack(self, threadedJob, submitResult):
    if not submitResult['OK']:
      self.log.error( submitResult['Message'] )
    else:
      submittedPilots = submitResult['Value']
      self.callBackLock.acquire()
      self.submittedPilots += submittedPilots
      self.callBackLock.release()


import os, time, tempfile, shutil, re

# Some reasonable Defaults
DIRAC_PILOT   = os.path.join( DIRAC.rootPath, 'DIRAC', 'WorkloadManagementSystem', 'PilotAgent', 'dirac-pilot' )
DIRAC_INSTALL = os.path.join( DIRAC.rootPath, 'scripts', 'dirac-install' )
DIRAC_VERSION = 'Production'
DIRAC_VERSION = 'HEAD'

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

ERROR_JDL        = 'Could not create GRID JDL'
ERROR_PROXY      = 'No proxy Available'
ERROR_VOMS       = 'Proxy without VOMS Extensions'
ERROR_CE         = 'No queue available for pilot'

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
    self.install            = gConfig.getValue( mySection+'/InstallScript'     , self.install )

    self.enableListMatch    = gConfig.getValue( mySection+'/EnableListMatch'   , self.enableListMatch )
    self.listMatchDelay     = gConfig.getValue( mySection+'/ListMatchDelay'    , self.listMatchDelay )
    self.gridEnv            = gConfig.getValue( mySection+'/GridEnv'           , self.gridEnv )
    self.resourceBrokers    = gConfig.getValue( mySection+'/ResourceBrokers'   , self.resourceBrokers )
    self.genericPilotDN     = gConfig.getValue( mySection+'/GenericPilotDN'    , self.genericPilotDN )
    self.genericPilotGroup  = gConfig.getValue( mySection+'/GenericPilotGroup' , self.genericPilotGroup )
    self.timePolicy         = gConfig.getValue( mySection+'/TimePolicy'        , self.timePolicy )
    self.requirements       = gConfig.getValue( mySection+'/Requirements'      , self.requirements )
    self.rank               = gConfig.getValue( mySection+'/Rank'              , self.rank )
    self.fuzzyRank          = gConfig.getValue( mySection+'/FuzzyRank'         , self.fuzzyRank )
    self.loggingServers     = gConfig.getValue( mySection+'/LoggingServers'    , self.loggingServers )

  def submitPilots(self, taskQueueDict, pilotsToSubmit, workDir=None ):
    """
      Submit pilot for the given TaskQueue, this is done from the Thread Pool job
    """
    try:

      taskQueueID = taskQueueDict['TaskQueueID']

      self.log.verbose( 'Submitting Pilot' )
      ceMask = self.__resolveCECandidates( taskQueueDict )
      if not ceMask: return S_ERROR( 'No CE available for TaskQueue' )
      workingDirectory = tempfile.mkdtemp( prefix= 'TQ_%s_' % taskQueueID, dir = workDir )
      self.log.verbose( 'Using working Directory:', workingDirectory )

      inputSandbox = []
      pilotOptions = []
      if taskQueueDict['PilotType'].lower()=='private':
        self.log.verbose('Submitting private pilots for TaskQueue %s' % taskQueueID)
        ownerDN    = taskQueueDict['OwnerDN']
        ownerGroup = taskQueueDict['OwnerGroup']
        # User Group requirement
        pilotOptions.append( '-G %s' % taskQueueDict['OwnerGroup'] )
        # check if group allows jobsharing
        ownerGroupProperties = getPropertiesForGroup( ownerGroup )
        if not 'JobSharing' in ownerGroupProperties:
          # Add Owner requirement to pilot
          pilotOptions.append( "-O '%s'" % ownerDN )
        pilotOptions.append( '-o /Resources/Computing/InProcess/PilotType=private' )
      else:
        self.log.verbose('Submitting generic pilots for TaskQueue %s' % taskQueueID)
        ownerDN    = self.genericPilotDN
        ownerGroup = self.genericPilotGroup
        pilotOptions.append( '-o /Resources/Computing/InProcess/PilotType=generic' )

      # Requested version of DIRAC
      pilotOptions.append( '-v %s' % self.diracVersion )
      # Requested CPU time
      pilotOptions.append( '-T %s' % taskQueueDict['CPUTime'] )
      # Setup. 
      pilotOptions.append( '-o /DIRAC/Setup=%s' % taskQueueDict['Setup'] )

      # Write JDL
      retDict = self._prepareJDL( taskQueueDict, workingDirectory, pilotOptions, pilotsToSubmit, ceMask )
      jdl = retDict['JDL']
      pilotRequirements = retDict['Requirements']
      if not jdl:
        try:
          shutil.rmtree( workingDirectory )
        except:
          pass
        return S_ERROR( ERROR_JDL )

      # get a valid proxy, submit with a long proxy to avoid renewal
      ret = gProxyManager.getPilotProxyFromDIRACGroup( ownerDN, ownerGroup, requiredTimeLeft = 86400 * 5 )
      if not ret['OK']:
        self.log.error( ret['Message'] )
        self.log.error( 'No proxy Available', 'User "%s", Group "%s"' % ( ownerDN, ownerGroup ) )
        try:
          shutil.rmtree( workingDirectory )
        except:
          pass
        return S_ERROR( ERROR_PROXY )
      proxy = ret['Value']
      # Need to get VOMS extension for the later interctions with WMS
      ret = gProxyManager.getVOMSAttributes(proxy)
      if not ret['OK']:
        return ret
      if not ret['Value']:
        return S_ERROR( ERROR_VOMS )
      vomsGroup = ret['Value'][0]

      # Check that there are available queues for the Job:
      if self.enableListMatch:
        availableCEs = []
        now = Time.dateTime()
        if not pilotRequirements in self.listMatch:
          availableCEs = self._listMatch( proxy, jdl, taskQueueID )
          if availableCEs != False:
            self.listMatch[pilotRequirements] = {'LastListMatch': now}
            self.listMatch[pilotRequirements]['AvailableCEs'] = availableCEs
        else:
          availableCEs = self.listMatch[pilotRequirements]['AvailableCEs']
          if not Time.timeInterval( self.listMatch[pilotRequirements]['LastListMatch'],
                                    self.listMatchDelay * Time.minute  ).includes( now ):
            availableCEs = self._listMatch( proxy, jdl, taskQueueID )
            if availableCEs != False:
              self.listMatch[pilotRequirements] = {'LastListMatch': now}
              self.listMatch[pilotRequirements]['AvailableCEs'] = availableCEs
            else:
              del self.listMatch[pilotRequirements]
          else:
            self.log.verbose( 'LastListMatch', self.listMatch[pilotRequirements]['LastListMatch'] )
            self.log.verbose( 'AvailableCEs ', availableCEs )

        if not availableCEs:
          try:
            shutil.rmtree( workingDirectory )
          except:
            pass
          return S_ERROR( ERROR_CE )

      # Now we are ready for the actual submission, so

      self.log.verbose('Submitting Pilots for TaskQueue', taskQueueID )
      submitRet = self._submitPilot( proxy, pilotsToSubmit, jdl, taskQueueID )
      try:
        shutil.rmtree( workingDirectory )
      except:
        pass
      if not submitRet:
        return S_ERROR( 'Pilot Submission Failed' )
      # pilotReference, resourceBroker = submitRet

      submittedPilots = 0

      if pilotsToSubmit != 1 and len( submitRet ) != pilotsToSubmit:
        # Parametric jobs are used
        for pilotReference, resourceBroker in submitRet:
          pilotReference = self._getChildrenReferences( proxy, pilotReference, taskQueueID )
          submittedPilots += len(pilotReference)
          pilotAgentsDB.addPilotTQReference(pilotReference, taskQueueID, ownerDN, 
                        vomsGroup, broker=resourceBroker, gridType=self.gridMiddleware, 
                        requirements=pilotRequirements )
      else:
        for pilotReference, resourceBroker in submitRet:
          pilotReference = [pilotReference]
          submittedPilots += len(pilotReference)
          pilotAgentsDB.addPilotTQReference(pilotReference, taskQueueID, ownerDN, 
                        vomsGroup, broker=resourceBroker, gridType=self.gridMiddleware, 
                        requirements=pilotRequirements )

      # add some sleep here
      time.sleep(1.0*submittedPilots)

      return S_OK( submittedPilots )

    except Exception,x:
      self.log.exception( 'Error during pilot submission' )

    return S_OK(0)

  def _JobJDL(self, taskQueueDict, pilotOptions, ceMask ):
    """
     The Job JDL is the same for LCG and GLite
    """
    pilotJDL = 'Executable     = "%s";\n' % os.path.basename( self.pilot )
    executable = self.pilot

    pilotJDL += 'Arguments     = "%s";\n' % ' '.join( pilotOptions )

    pilotJDL += 'TimeRef       = %s;\n' % taskQueueDict['CPUTime']

    pilotJDL += 'TimePolicy    = ( %s );\n' % self.timePolicy

    requirements = list(self.requirements)
    requirements.append( 'other.GlueCEPolicyMaxCPUTime > TimePolicy' )

    siteRequirements = '\n || '.join( [ 'other.GlueCEInfoHostName == "%s"' % s for s in ceMask ] )
    requirements.append( "( %s\n )" %  siteRequirements )

    pilotRequirements = '\n && '.join( requirements )

    pilotJDL += 'pilotRequirements  = %s;\n' % pilotRequirements

    pilotJDL += 'Rank          = %s;\n' % self.rank
    pilotJDL += 'FuzzyRank     = %s;\n' % self.fuzzyRank
    pilotJDL += 'StdOutput     = "std.out";\n'
    pilotJDL += 'StdError      = "std.err";\n'

    pilotJDL += 'InputSandbox  = { "%s" };\n' % '", "'.join( [ self.install, executable ] )

    pilotJDL += 'OutputSandbox = { "std.out", "std.err" };\n'

    self.log.verbose( pilotJDL )

    return (pilotJDL,pilotRequirements)

  def _gridCommand(self, proxy, cmd):
    """
     Execute cmd tuple after sourcing GridEnv
    """
    gridEnv = dict(os.environ)
    if self.gridEnv:
      self.log.verbose( 'Sourcing GridEnv script:', self.gridEnv )
      ret = Source( 10, [self.gridEnv] )
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
    self.log.exception( 'Exception in Pilot Submission' )

  def _prepareJDL(self, taskQueueDict, workingDirectory, pilotOptions, pilotsToSubmit, ceMask ):
    """
      This method should be overridden in a subclass
    """
    self.log.error( '_prepareJDL() method should be implemented in a subclass' )
    sys.exit()

  def __resolveCECandidates( self, taskQueueDict ):
    """
      Return a list of CE's
    """
    # assume user knows what they're doing and avoid site mask e.g. sam jobs
    if 'GridCEs' in taskQueueDict and taskQueueDict['GridCEs']:
      self.log.info( 'CEs requested by TaskQueue %s:' % taskQueueDict['TaskQueueID'], ', '.join( taskQueueDict['GridCEs'] ) )
      return taskQueueDict['GridCEs']

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
    if 'BannedSites' in taskQueueDict:
      for site in taskQueueDict['BannedSites']:
        if site in siteMask:
          siteMask.remove(site)
          self.log.verbose('Removing banned site %s from site Mask' % site )

    # remove from the mask if a Site is given
    siteMask = [ site for site in siteMask if 'Sites' not in taskQueueDict or site in taskQueueDict['Sites'] ]

    if not siteMask:
      # pilot can not be submitted
      self.log.info( 'No Valid Site Candidate in Mask for TaskQueue %s' % taskQueueDict['TaskQueueID'] )
      return []

    self.log.info( 'Site Candidates for TaskQueue %s:' % taskQueueDict['TaskQueueID'], ', '.join(siteMask) )

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
      self.log.info( 'No CE Candidate found for TaskQueue %s:' % taskQueueDict['TaskQueue'], ', '.join(siteMask) )

    self.log.verbose( 'CE Candidates for TaskQueue %s:' % taskQueueDict['TaskQueueID'], ', '.join(ceMask) )

    return ceMask

  def parseListMatchStdout(self, proxy, cmd, taskQueueID ):
    """
      Parse List Match stdout to return list of matched CE's
    """
    self.log.verbose( 'Executing List Match for TaskQueue', taskQueueID )

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
      self.log.info( 'List-Match failed to find CEs for TaskQueue', taskQueueID )
      self.log.info( stdout )
      self.log.info( stderr )
    else:
      self.log.debug( 'List-Match returns:', str(ret['Value'][0]) + '\n'.join( ret['Value'][1:3] ) )
      self.log.info( 'List-Match found %s CEs for TaskQueue' % len(availableCEs), taskQueueID )
      self.log.verbose( ', '.join(availableCEs) )


    return availableCEs

  def parseJobSubmitStdout(self, proxy, cmd, taskQueueID):
    """
      Parse Job Submit stdout to return pilot reference
    """
    start = time.time()
    self.log.verbose( 'Executing Job Submit for TaskQueue', taskQueueID )

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
        failed = 0
    if failed:
      self.log.error( 'Job Submit returns no Reference:', str(ret['Value'][0]) + '\n'.join( ret['Value'][1:3] ) )
      return False

    self.log.info( 'Reference %s for TaskQueue %s' % ( glite_id, taskQueueID ) )

    return glite_id,rb

  def _writeJDL(self, filename, jdlList):
    try:
      f = open(filename,'w')
      f.write( '\n'.join( jdlList) )
      f.close()
    except Exception, x:
      self.log.exception( )
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

  def _prepareJDL(self, taskQueueDict, workingDirectory, pilotOptions, pilotsToSubmit, ceMask ):
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
MyProxyServer = "no-myproxy.cern.ch";

Requirements = pilotRequirements;
WmsClient = [
Requirements = other.GlueCEStateStatus == "Production";
ErrorStorage = "%s/pilotError";
OutputStorage = "%s/pilotOutput";
# ListenerPort = 44000;
ListenerStorage = "%s/Storage";
VirtualOrganisation = "lhcb";
RetryCount = 0;
ShallowRetryCount = 0;
WMProxyEndPoints = { %s };
LBEndPoints = { %s };
MyProxyServer = "no-myproxy.cern.ch";
];
""" % ( workingDirectory, workingDirectory, workingDirectory, ', '.join(RBs), ', '.join(LBs) )

    if pilotsToSubmit >= 1:
      wmsClientJDL += """
JobType = "Parametric";
Parameters= %s;
ParameterStep =1;
ParameterStart = 0;
""" % pilotsToSubmit
      nPilots = pilotsToSubmit


    (pilotJDL , pilotRequirements) = self._JobJDL( taskQueueDict, pilotOptions, ceMask )

    jdl = os.path.join( workingDirectory, '%s.jdl' % taskQueueDict['TaskQueueID'] )
    jdl = self._writeJDL( jdl, [pilotJDL, wmsClientJDL] )

    return {'JDL':jdl, 'Requirements':pilotRequirements, 'Pilots':nPilots }

  def _listMatch(self, proxy, jdl, taskQueueID):
    """
     Check the number of available queues for the pilots to prevent submission
     if there are no matching resources.
    """
    cmd = [ 'glite-wms-job-list-match', '-a', '-c', '%s' % jdl, '%s' % jdl ]
    return self.parseListMatchStdout( proxy, cmd, taskQueueID )

  def _submitPilot(self, proxy, pilotsToSubmit, jdl, taskQueueID):
    """
     Submit pilot and get back the reference
    """
    result = []
    cmd = [ 'glite-wms-job-submit', '-a', '-c', '%s' % jdl, '%s' % jdl ]
    ret = self.parseJobSubmitStdout( proxy, cmd, taskQueueID )
    if ret:
      result.append(ret)

    return result

  def _getChildrenReferences(self, proxy, parentReference, taskQueueID ):
    """
     Get reference for all Children
    """
    cmd = [ 'glite-wms-job-status', parentReference ]

    start = time.time()
    self.log.verbose( 'Executing Job Status for TaskQueue', taskQueueID )

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
    self.resourceBrokers = List.randomize( self.resourceBrokers )
    self.log.info( '' )
    self.log.info( '===============================================' )

  def _prepareJDL(self, taskQueueDict, workingDirectory, pilotOptions, pilotsToSubmit, ceMask ):
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
Requirements = pilotRequirements && other.GlueCEStateStatus == "Production";
RetryCount = 0;
ErrorStorage = "%s/pilotError";
OutputStorage = "%s/pilotOutput";
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
MyProxyServer = "no-myproxy.cern.ch";
""" % (workingDirectory, workingDirectory, workingDirectory, LD, NS, LB)

    pilotJDL,pilotRequirements = self._JobJDL( taskQueueDict, pilotOptions, ceMask )

    jdl = os.path.join( workingDirectory, '%s.jdl' % taskQueueDict['TaskQueueID'] )
    jdl = self._writeJDL( jdl, [pilotJDL, rbJDL] )

    return {'JDL':jdl, 'Requirements':pilotRequirements, 'Pilots': pilotsToSubmit }

  def _listMatch(self, proxy, jdl, taskQueueID):
    """
     Check the number of available queues for the pilots to prevent submission
     if there are no matching resources.
    """
    cmd = ['edg-job-list-match','-c','%s' % jdl , '--config-vo', '%s' % jdl, '%s' % jdl]
    return self.parseListMatchStdout( proxy, cmd, taskQueueID )

  def _submitPilot(self, proxy, pilotsToSubmit, jdl, taskQueueID ):
    """
     Submit pilot and get back the reference
    """
    result = []
    for i in range(pilotsToSubmit):
      cmd = [ 'edg-job-submit', '-c', '%s' % jdl, '--config-vo', '%s' % jdl, '%s' % jdl ]
      ret = self.parseJobSubmitStdout( proxy, cmd, taskQueueID )
      if ret:
        result.append(ret)

    return result

