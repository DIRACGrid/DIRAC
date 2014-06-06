########################################################################
# File :   TaskQueueDirector.py
# Author : Stuart Paterson, Ricardo Graciani
########################################################################
"""  The TaskQueue Director Agent controls the submission of pilots via the
     PilotDirectors. These are Backend-specific PilotDirector derived classes.
     This is a simple wrapper that performs the instantiation and monitoring
     of the PilotDirector instances and add workload to them via ThreadPool
     mechanism.

     From the base Agent class it uses the following configuration Parameters
       - WorkDir:
       - PollingTime:
       - MaxCycles:

     The following parameters are searched for in WorkloadManagement/TaskQueueDirector:
       - ThreadStartDelay:
       - SubmitPools: All the Submit pools that are to be initialized
       - DefaultSubmitPools: If no specific pool is requested, use these

     It will use those Directors to submit pilots for each of the Supported SubmitPools
       - SubmitPools (see above)


     SubmitPools may refer to:
       - a full GRID infrastructure (like EGEE, OSG, NDG,...) access remotely through RBs or WMSs servers
       distributing the load over all available resources (CEs) using ad hoc middleware (gLite, LCG, ...).
       - individual GRID Computing Elements again access remotely through their corresponding GRID
       interface using ad hoc middleware.
       - classic batch systems (like LSF, BQS, PBS, Torque, Condor, ...) access locally trough
       their corresponding head nodes using their onw specific tools
       - standalone computers access by direct execution (fork or exec)

       In first two cases, the middleware takes care of properly handling the secure transfer of the
       payload to the executing node. In the last two DIRAC will take care of all relevant security
       aspects.

     For every SubmitPool category (GRID or DIRAC) and there must be a corresponding Section with the
     necessary parameters:

       - Pool: if a dedicated Threadpool is desired for this SubmitPool

     GRID:
       - GridMiddleware: <GridMiddleware>PilotDirector module from the PilotAgent directory will
               be used, currently LCG, gLite types are supported

     For every supported GridMiddleware there must be a corresponding Section with the
     necessary parameters:
       gLite:

       LCG:

     DIRAC:

     For every supported "Local backend" there must be a corresponding Section with the
     necessary parameters:
       PBS:

       Torque:

       LSF:

       BQS:

       Condor:

     (This are the parameters referring to the corresponding SubmitPool and PilotDirector classes,
      not the ones referring to the CE object that does the actual submission to the backend)

       The following parameters are taken from the TaskQueueDirector section if not
       present in the corresponding SubmitPool section
       - GenericPilotDN:
       - GenericPilotGroup:


      The pilot submission logic is as follows:

        - Determine prioritySum: sum of the Priorities for all TaskQueues in the system.

        - Determine pilotsPerPriority: result of dividing the  number of pilots to submit
          per iteration by the prioritySum.

        - select TaskQueues from the WMS system appropriated for PilotSubmission by the supported
        SubmitPools

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

        All TaskQueues are considered on every iteration, pilots are submitted
        statistically proportional to the priority and the Number of waiting tasks
        of the TaskQueue, boosted for the TaskQueues with lower CPU requirements and
        limited by the difference between the number of waiting jobs and the number of
        already waiting pilots.


      This module is prepared to work:
       - locally to the WMS DIRAC server and connect directly to the necessary DBs.
       - remotely to the WMS DIRAC server and connect via appropriated DISET methods.


      Obsolete Job JDL Option:
        GridExecutable
        SoftwareTag

"""
__RCSID__ = "$Id$"

import random, time, threading
from DIRAC                                                       import S_OK, S_ERROR, List, Time, abort
from DIRAC.Core.Utilities.ThreadPool                             import ThreadPool
from DIRAC.Core.Utilities.ObjectLoader                           import ObjectLoader
from DIRAC.Core.DISET.RPCClient                                  import RPCClient
from DIRAC.Core.Base.AgentModule                                 import AgentModule
from DIRAC.ConfigurationSystem.Client.Helpers.Resources          import getDIRACPlatforms
from DIRAC.Resources.Computing.ComputingElement                  import getResourceDict
from DIRAC.WorkloadManagementSystem.Client.ServerUtils           import pilotAgentsDB

from DIRAC.Core.Utilities                                        import List, Time
from DIRAC.Core.Utilities.ThreadPool                             import ThreadPool
from DIRAC.Core.DISET.RPCClient                                  import RPCClient
from DIRAC                                                       import S_OK, S_ERROR, gConfig

import random, time
import DIRAC

random.seed()

class TaskQueueDirector( AgentModule ):
  """
      The specific agents must provide the following methods:
      - initialize() for initial settings
      - beginExecution()
      - execute() - the main method called in the agent cycle
      - endExecution()
      - finalize() - the graceful exit of the method, this one is usually used
                 for the agent restart
  """

  def initialize( self ):
    """ Standard constructor
    """
    self.am_setOption( "PollingTime", 60.0 )

    self.am_setOption( "pilotsPerIteration", 40.0 )
    self.am_setOption( "lowestCPUBoost", 7200.0 )
    self.am_setOption( "extraPilotFraction", 0.2 )
    self.am_setOption( "extraPilots", 4 )
    self.am_setOption( "maxPilotWaitingHours", 6 )

    self.am_setOption( 'ThreadStartDelay', 1 )
    self.am_setOption( 'SubmitPools', [] )
    self.am_setOption( 'DefaultSubmitPools', [] )

    self.am_setOption( 'minThreadsInPool', 0 )
    self.am_setOption( 'maxThreadsInPool', 2 )
    self.am_setOption( 'totalThreadsInPool', 40 )

    self.directorsImportLine = self.am_getOption( 'directorsImportLine', 'DIRAC.WorkloadManagementSystem.private' )

    self.directors = {}
    self.pools = {}

    self.directorDict = {}

    self.callBackLock = threading.Lock()

    return S_OK()

  def execute( self ):
    """Main Agent code:
      1.- Query TaskQueueDB for existing TQs
      2.- Add their Priorities
      3.- Submit pilots
    """

    self.__checkSubmitPools()

    self.directorDict = getResourceDict()
    #Add all submit pools
    self.directorDict[ 'SubmitPool' ] = self.am_getOption( "SubmitPools" )
    #Add all DIRAC platforms if not specified otherwise
    if not 'Platform' in self.directorDict:
      result = getDIRACPlatforms()
      if result['OK']:
        self.directorDict['Platform'] = result['Value']

    rpcMatcher = RPCClient( "WorkloadManagement/Matcher" )
    result = rpcMatcher.getMatchingTaskQueues( self.directorDict )
    if not result['OK']:
      self.log.error( 'Could not retrieve TaskQueues from TaskQueueDB', result['Message'] )
      return result
    taskQueueDict = result['Value']

    self.log.info( 'Found %s TaskQueues' % len( taskQueueDict ) )

    if not taskQueueDict:
      self.log.info( 'No TaskQueue to Process' )
      return S_OK()

    prioritySum = 0
    waitingJobs = 0
    for taskQueueID in taskQueueDict:
      taskQueueDict[taskQueueID]['TaskQueueID'] = taskQueueID
      prioritySum += taskQueueDict[taskQueueID]['Priority']
      waitingJobs += taskQueueDict[taskQueueID]['Jobs']

    self.log.info( 'Sum of Priorities %s' % prioritySum )

    if waitingJobs == 0:
      self.log.info( 'No waiting Jobs' )
      return S_OK( 'No waiting Jobs' )
    if prioritySum <= 0:
      return S_ERROR( 'Wrong TaskQueue Priorities' )

    self.pilotsPerPriority = self.am_getOption( 'pilotsPerIteration' ) / prioritySum
    self.pilotsPerJob = self.am_getOption( 'pilotsPerIteration' ) / waitingJobs

    self.callBackLock.acquire()
    self.submittedPilots = 0
    self.callBackLock.release()
    self.toSubmitPilots = 0
    waitingStatusList = ['Submitted', 'Ready', 'Scheduled', 'Waiting']
    timeLimitToConsider = Time.toString( Time.dateTime() - Time.hour * self.am_getOption( "maxPilotWaitingHours" ) )

    for taskQueueID in taskQueueDict:
      self.log.verbose( 'Processing TaskQueue', taskQueueID )

      result = pilotAgentsDB.countPilots( { 'TaskQueueID': taskQueueID,
                                            'Status': waitingStatusList},
                                          None, timeLimitToConsider )
      if not result['OK']:
        self.log.error( 'Fail to get Number of Waiting pilots', result['Message'] )
        waitingPilots = 0
      else:
        waitingPilots = result['Value']
        self.log.verbose( 'Waiting Pilots for TaskQueue %s:' % taskQueueID, waitingPilots )

      result = self.submitPilotsForTaskQueue( taskQueueDict[taskQueueID], waitingPilots )

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

  def submitPilotsForTaskQueue( self, taskQueueDict, waitingPilots ):

    from numpy.random import poisson
    from DIRAC.WorkloadManagementSystem.private.Queues import maxCPUSegments

    taskQueueID = taskQueueDict['TaskQueueID']
    maxCPU = maxCPUSegments[-1]
    extraPilotFraction = self.am_getOption( 'extraPilotFraction' )
    extraPilots = self.am_getOption( 'extraPilots' )

    taskQueuePriority = taskQueueDict['Priority']
    self.log.verbose( 'Priority for TaskQueue %s:' % taskQueueID, taskQueuePriority )
    taskQueueCPU = max( taskQueueDict['CPUTime'], self.am_getOption( 'lowestCPUBoost' ) )
    self.log.verbose( 'CPUTime  for TaskQueue %s:' % taskQueueID, taskQueueCPU )
    taskQueueJobs = taskQueueDict['Jobs']
    self.log.verbose( 'Jobs in TaskQueue %s:' % taskQueueID, taskQueueJobs )

    # Determine number of pilots to submit, boosting TaskQueues with low CPU requirements
    pilotsToSubmit = poisson( ( self.pilotsPerPriority * taskQueuePriority +
                                self.pilotsPerJob * taskQueueJobs ) * maxCPU / taskQueueCPU )
    # limit the number of pilots according to the number of waiting job in the TaskQueue
    # and the number of already submitted pilots for that TaskQueue
    pilotsToSubmit = min( pilotsToSubmit,
                          int( ( 1 + extraPilotFraction ) * taskQueueJobs ) + extraPilots - waitingPilots )

    if pilotsToSubmit <= 0: return S_OK( 0 )
    self.log.verbose( 'Submitting %s pilots for TaskQueue %s' % ( pilotsToSubmit, taskQueueID ) )

    return self.__submitPilots( taskQueueDict, pilotsToSubmit )

  def __submitPilots( self, taskQueueDict, pilotsToSubmit ):
    """
      Try to insert the submission in the corresponding Thread Pool, disable the Thread Pool
      until next itration once it becomes full
    """
    # Check if an specific MiddleWare is required
    if 'SubmitPools' in taskQueueDict:
      submitPools = taskQueueDict[ 'SubmitPools' ]
    else:
      submitPools = self.am_getOption( 'DefaultSubmitPools' )
    submitPools = List.randomize( submitPools )

    for submitPool in submitPools:
      self.log.verbose( 'Trying SubmitPool:', submitPool )

      if not submitPool in self.directors or not self.directors[submitPool]['isEnabled']:
        self.log.verbose( 'Not Enabled' )
        continue

      pool = self.pools[self.directors[submitPool]['pool']]
      director = self.directors[submitPool]['director']
      ret = pool.generateJobAndQueueIt( director.submitPilots,
                                        args = ( taskQueueDict, pilotsToSubmit, self.workDir ),
                                        oCallback = self.callBack,
                                        oExceptionCallback = director.exceptionCallBack,
                                        blocking = False )
      if not ret['OK']:
        # Disable submission until next iteration
        self.directors[submitPool]['isEnabled'] = False
      else:
        time.sleep( self.am_getOption( 'ThreadStartDelay' ) )
        break

    return S_OK( pilotsToSubmit )

  def __checkSubmitPools( self ):
    """
      This method is called at initialization and at the beginning of each execution cycle
      in this way running parameters can be dynamically changed via the remote
      configuration.
    """

    # First update common Configuration for all Directors
    self.__configureDirector()

    # Now we need to initialize one thread for each Director in the List,
    # and check its configuration:
    for submitPool in self.am_getOption( 'SubmitPools' ):
      # check if the Director is initialized, then reconfigure
      if submitPool not in self.directors:
        # instantiate a new Director
        self.__createDirector( submitPool )

      self.__configureDirector( submitPool )

      # Now enable the director for this iteration, if some RB/WMS/CE is defined
      if submitPool in self.directors:
        if 'resourceBrokers' in dir( self.directors[submitPool]['director'] ) and \
            self.directors[submitPool]['director'].resourceBrokers:
          self.directors[submitPool]['isEnabled'] = True
        if 'computingElements' in dir( self.directors[submitPool]['director'] ) and \
            self.directors[submitPool]['director'].computingElements:
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
        pool = self.pools.pop( pool )
        # del self.pools[pool]
        del pool

  def __createDirector( self, submitPool ):
    """
     Instantiate a new PilotDirector for the given SubmitPool
    """

    self.log.info( 'Creating Director for SubmitPool:', submitPool )
    # check the GridMiddleware
    directorGridMiddleware = self.am_getOption( submitPool + '/GridMiddleware', '' )
    if not directorGridMiddleware:
      self.log.error( 'No Director GridMiddleware defined for SubmitPool:', submitPool )
      return

    directorName = '%sPilotDirector' % directorGridMiddleware

    self.log.info( 'Instantiating Director Object:', directorName )

    # loading the module
    directorM = ObjectLoader().loadModule( "WorkloadManagementSystem.private.%s" % directorName )
    if not directorM:
      return directorM
    # instantiating the director object passing submitPool
    director = getattr( directorM['Value'], directorName )( submitPool )

    # 2. check the requested ThreadPool (if not defined use the default one)
    directorPool = self.am_getOption( submitPool + '/Pool', 'Default' )
    if not directorPool in self.pools:
      self.log.info( 'Adding Thread Pool:', directorPool )
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

  def __configureDirector( self, submitPool = None ):
    """
      Update Configuration from CS
      if submitPool == None then,
          disable all Directors
      else
         Update Configuration for the PilotDirector of that SubmitPool
    """
    if not submitPool:
      self.workDir = self.am_getWorkDirectory()
      # By default disable all directors
      for director in self.directors:
        self.directors[director]['isEnabled'] = False

    else:
      if submitPool not in self.directors:
        abort( -1, "Submit Pool not available", submitPool )
      director = self.directors[submitPool]['director']

      # Pass reference to our CS section so that defaults can be taken from there
      director.configure( self.am_getModuleParam( 'section' ), submitPool )

      # Enable director for pilot submission
      self.directors[submitPool]['isEnabled'] = True

  def __addPool( self, poolName ):
    """
      create a new thread Pool, by default it has 2 executing threads and 40 requests
      in the Queue
    """

    if not poolName:
      return None
    if poolName in self.pools:
      return None
    pool = ThreadPool( self.am_getOption( 'minThreadsInPool' ),
                       self.am_getOption( 'maxThreadsInPool' ),
                       self.am_getOption( 'totalThreadsInPool' ) )
    # Daemonize except "Default" pool
    if poolName != 'Default':
      pool.daemonize()
    self.pools[poolName] = pool
    return poolName

  def callBack( self, threadedJob, submitResult ):
    """ Call with result from director.submitPilots Threaded Job
    """
    if not submitResult['OK']:
      self.log.error( 'submitPilot Failed: ', submitResult['Message'] )
      if 'Value' in submitResult:
        submittedPilots = submitResult['Value']
        self.callBackLock.acquire()
        self.submittedPilots += submittedPilots
        self.callBackLock.release()
    else:
      submittedPilots = submitResult['Value']
      self.callBackLock.acquire()
      self.submittedPilots += submittedPilots
      self.callBackLock.release()


