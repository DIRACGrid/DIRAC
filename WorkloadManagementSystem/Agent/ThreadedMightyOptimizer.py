########################################################################
# $HeadURL$
# File :   ThreadedMightyOptimizer.py
# Author : Adria Casajus
########################################################################
"""
  SuperOptimizer
  One optimizer to rule them all, one optimizer to find them,
  one optimizer to bring them all, and in the darkness bind them.
"""
__RCSID__ = "$Id$"

import time
import os
import threading
import Queue
from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.WorkloadManagementSystem.DB.JobDB         import JobDB
from DIRAC.WorkloadManagementSystem.DB.JobLoggingDB  import JobLoggingDB
from DIRAC.Core.Utilities import ThreadSafe, List
from DIRAC.Core.Utilities.Shifter import setupShifterProxyInEnv


gOptimizerLoadSync = ThreadSafe.Synchronizer()

class ThreadedMightyOptimizer( AgentModule ):
  """
      The specific agents must provide the following methods:
      - initialize() for initial settings
      - beginExecution()
      - execute() - the main method called in the agent cycle
      - endExecution()
      - finalize() - the graceful exit of the method, this one is usually used
                 for the agent restart
  """

  __jobStates = [ 'Received', 'Checking' ]
  __defaultValidOptimizers = [ 'WorkloadManagement/JobPath',
                               'WorkloadManagement/JobSanity',
                               'WorkloadManagement/JobScheduling',
                               'WorkloadManagement/TaskQueue',
                               ]

  def initialize( self ):
    """ Standard constructor
    """
    self.jobDB = JobDB()
    self.jobLoggingDB = JobLoggingDB()
    self._optimizingJobs = JobsInTheWorks()
    self._optimizers = {}
    self._threadedOptimizers = {}
    self.am_setOption( "PollingTime", 30 )
    return S_OK()

  def execute( self ):
    """ Standard Agent module execute method
    """
    #Get jobs from DB
    result = self.jobDB.selectJobs( { 'Status': self.__jobStates  } )
    if not result[ 'OK' ]:
      gLogger.error( "Cannot retrieve jobs in states %s" % self.__jobStates )
      return result
    jobsList = result[ 'Value' ]
    for i in range( len( jobsList ) ):
      jobsList[i] = int( jobsList[i] )
    jobsList.sort()
    self.log.info( "Got %s jobs for this iteration" % len( jobsList ) )
    if not jobsList: return S_OK()
    #Check jobs that are already being optimized
    newJobsList = self._optimizingJobs.addJobs( jobsList )
    if not newJobsList:
      return S_OK()
    #Get attrs of jobs to be optimized
    result = self.jobDB.getAttributesForJobList( newJobsList )
    if not result[ 'OK' ]:
      gLogger.error( "Cannot retrieve attributes for %s jobs %s" % len( newJobsList ) )
      return result
    jobsToProcess = result[ 'Value' ]
    for jobId in jobsToProcess:
      self.log.info( "== Processing job %s == " % jobId )
      jobAttrs = jobsToProcess[ jobId ]
      result = self.__dispatchJob( jobId, jobAttrs, False )
      if not result[ 'OK' ]:
        gLogger.error( "There was a problem optimizing job", "JID %s: %s" % ( jobId, result[ 'Message' ] ) )
    return S_OK()

  def __dispatchJob( self, jobId, jobAttrs, jobDef, keepOptimizing = True ):
    """ Decide what to do with the Job
    """
    returnValue = S_OK()
    if keepOptimizing:
      result = self.__sendJobToOptimizer( jobId, jobAttrs, jobDef )
      if result[ 'OK' ] and result[ 'Value' ]:
        return S_OK()
      if not result[ 'OK' ]:
        returnValue = result
        gLogger.error( "Could not send job to optimizer\n",
                       "\tJob: %s\n\Message: %s" % ( jobId,
                                                     result[ 'Message' ] ) )
    self._optimizingJobs.deleteJob( jobId )
    return returnValue

  def __sendJobToOptimizer( self, jobId, jobAttrs, jobDef ):
    """ Send Job to Optimizer queue
    """
    optimizerName = self.__getNextOptimizerName( jobAttrs )
    if not optimizerName:
      return S_OK( False )
    if optimizerName not in self.am_getOption( "ValidOptimizers", self.__defaultValidOptimizers ):
      return S_OK( False )
    if optimizerName not in self._threadedOptimizers:
      to = ThreadedOptimizer( optimizerName, self.am_getModuleParam( 'fullName' ),
                              self.__dispatchJob )
      result = to.initialize( self.jobDB, self.jobLoggingDB )
      if not result[ 'OK' ]:
        return S_OK( False )
      self._threadedOptimizers[ optimizerName ] = to
    self._threadedOptimizers[ optimizerName ].optimizeJob( jobId, jobAttrs, jobDef )
    return S_OK( True )

  def __getNextOptimizerName( self, jobAttrs ):
    """ Determine next Optimizer
    """
    if jobAttrs[ 'Status' ] == 'Received':
      optList = [ "JobPath" ]
    elif jobAttrs[ 'Status' ] == 'Checking':
      optList = List.fromChar( jobAttrs[ 'MinorStatus' ], "/" )
    else:
      return False
    if len( optList ) == 1:
      optList.insert( 0, "WorkloadManagement" )
    if len( optList ) > 2:
      optList[1] = "/".join( optList[1:] )
    return "/".join( optList )

gOptimizingJobs = ThreadSafe.Synchronizer()

class JobsInTheWorks:

  def __init__( self, maxTime = 0 ):
    self.__jobs = {}
    self.__maxTime = maxTime
    self.log = gLogger.getSubLogger( "JobsBeingOptimized" )

  @gOptimizingJobs
  def addJobs( self, jobsList ):
    now = time.time()
    self.__purgeExpiredJobs()
    addedJobs = []
    for job in jobsList:
      if job not in self.__jobs:
        self.__jobs[ job ] = now
        addedJobs.append( job )
    self.log.info( "Added %s jobs to the list" % addedJobs )
    return addedJobs

  def __purgeExpiredJobs( self ):
    if not self.__maxTime:
      return
    stillOnIt = {}
    now = time.time()
    for job in self.__jobs:
      if now - self.__jobs[ job ] < self.__maxTime:
        stillOnIt[ job ] = self.__jobs[ job ]
    self.__jobs = stillOnIt

  @gOptimizingJobs
  def deleteJob( self, job ):
    try:
      if job in self.__jobs:
        self.log.info( "Deleted job %s from the list" % job )
        del( self.__jobs[ job ] )
    except Exception, e:
      print "=" * 20
      print "EXCEPTION", e
      print "THIS SHOULDN'T HAPPEN"
      print "=" * 20


class ThreadedOptimizer( threading.Thread ):

  def __init__( self, optimizerName, containerName, dispatchFunction ):
    threading.Thread.__init__( self )
    self.setDaemon( True )
    self.optimizerName = optimizerName
    self.containerName = containerName
    self.dispatchFunction = dispatchFunction
    self.jobQueue = Queue.Queue()

  def initialize( self, jobDB, jobLoggingDB ):
    self.jobDB = jobDB
    self.jobLoggingDB = jobLoggingDB
    gLogger.info( "Loading optimizer %s" % self.optimizerName )
    result = self.__loadOptimizer()
    if not result[ 'OK' ]:
      return result
    self.optimizer = result[ 'Value' ]
    self.start()
    return S_OK()

  @gOptimizerLoadSync
  def __loadOptimizer( self ):
    #Need to load an optimizer
    gLogger.info( "Loading optimizer %s" % self.optimizerName )
    optList = List.fromChar( self.optimizerName, "/" )
    optList[1] = "/".join( optList[1:] )
    systemName = optList[0]
    agentName = "%sAgent" % optList[1]
    rootModulesToLook = gConfig.getValue( "/LocalSite/Extensions", [] ) + [ 'DIRAC' ]
    for rootModule in rootModulesToLook:
      try:
        gLogger.info( "Trying to load from root module %s" % rootModule )
        opPyPath = '%s.%sSystem.Agent.%s' % ( rootModule, systemName, agentName )
        optimizerModule = __import__( opPyPath,
                                globals(),
                                locals(), agentName )
      except ImportError, e:
        gLogger.info( "Can't load %s: %s" % ( opPyPath, str( e ) ) )
        continue
      try:
        optimizerClass = getattr( optimizerModule, agentName )
        optimizer = optimizerClass( '%sAgent' % self.optimizerName, self.containerName )
        result = optimizer.am_initialize( self.jobDB, self.jobLoggingDB )
        if not result[ 'OK' ]:
          return S_ERROR( "Can't initialize optimizer %s: %s" % ( self.optimizerName, result[ 'Message' ] ) )
        return S_OK( optimizer )
      except Exception, e:
        gLogger.exception( "Can't load optimizer %s with root module %s" % ( self.optimizerName, rootModule ) )
    return S_ERROR( "Can't load optimizer %s" % self.optimizerName )

  def optimizeJob( self, jobId, jobAttrs, jobDef ):
    self.jobQueue.put( ( jobId, jobAttrs, jobDef ), block = True )

  def run( self ):
    while True:
      jobId, jobAttrs, jobDef = self.jobQueue.get( block = True )
      #If there's no job def then get it
      if not jobDef:
        result = self.optimizer.getJobDefinition( jobId, jobDef )
        if not result['OK']:
          self.optimizer.setFailedJob( jobId, result[ 'Message' ] )
          return result
        jobDef = result[ 'Value' ]
      #Does the optimizer require a proxy?
      shifterEnv = False
      if self.optimizer.am_getModuleParam( 'shifterProxy' ):
        shifterEnv = True
        result = setupShifterProxyInEnv( self.optimizer.am_getModuleParam( 'shifterProxy' ),
                                         self.optimizer.am_getShifterProxyLocation() )
        if not result[ 'OK' ]:
          return result
      #Call the initCycle function
      result = self.optimizer.am_secureCall( self.optimizer.beginExecution, name = "beginExecution" )
      if not result[ 'OK' ]:
        return result
      #Do the work
      result = self.optimizer.optimizeJob( jobId, jobDef[ 'classad' ] )
      #If there was a shifter proxy, unset it
      if shifterEnv:
        del( os.environ[ 'X509_USER_PROXY' ] )
      if not result[ 'OK' ]:
        gLogger.error( "Job failed optimization step\n",
                       "\tJob: %s\n\tOptimizer: %s\n\tMessage: %s" % ( jobId,
                                                                       self.optimizerName,
                                                                       result[ 'Message' ] ) )
        self.dispatchFunction( jobId, jobAttrs, jobDef, False )
      else:
        #Job optimization was OK
        nextOptimizer = result[ 'Value' ]
        #Check if the JDL has changed
        newJDL = jobDef[ 'classad' ].asJDL()
        if newJDL != jobDef[ 'jdl' ]:
          jobDef[ 'jdl' ] = newJDL
        #If there's a new optimizer set it!
        if nextOptimizer:
          jobAttrs[ 'Status' ] = 'Checking'
          jobAttrs[ 'MinorStatus' ] = nextOptimizer
          gLogger.info( "Sending job %s to next optimizer: %s" % ( jobId, nextOptimizer ) )
        else:
          gLogger.info( "Finished optimizing job %s" % jobId )
        self.dispatchFunction( jobId, jobAttrs, jobDef, nextOptimizer )



