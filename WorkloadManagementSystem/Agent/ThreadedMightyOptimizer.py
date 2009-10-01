########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/Agent/ThreadedMightyOptimizer.py,v 1.2 2009/10/01 18:15:35 acasajus Exp $


"""  SuperOptimizer
 One optimizer to rule them all, one optimizer to find them, one optimizer to bring them all, and in the darkness bind them.
"""

__RCSID__ = "$Id: ThreadedMightyOptimizer.py,v 1.2 2009/10/01 18:15:35 acasajus Exp $"

import time
import os
import threading
import Queue
from DIRAC  import gLogger, gConfig, gMonitor,S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.WorkloadManagementSystem.DB.JobDB         import JobDB
from DIRAC.WorkloadManagementSystem.DB.JobLoggingDB  import JobLoggingDB
from DIRAC.Core.Utilities import ThreadSafe, List
from DIRAC.Core.Utilities.Shifter import setupShifterProxyInEnv


gOptimizerLoadSync = ThreadSafe.Synchronizer()

class ThreadedMightyOptimizer(AgentModule):

  __jobStates = [ 'Received', 'Checking' ]
  __maxOptimizationTime = 600
  __defaultValidOptimizers = [ 'WorkloadManagement/JobPath', 
                               'WorkloadManagement/JobSanity', 
                               'WorkloadManagement/JobScheduling', 
                               'WorkloadManagement/TaskQueue', 
                               ]

  def initialize(self):
    """ Standard constructor
    """
    self.jobDB = JobDB()
    self.jobLoggingDB = JobLoggingDB()
    self._optimizers = {}
    self._threadedOptimizers = {}
    self._jobsBeingOptimized = {}
    self.am_setOption( "PollingTime", 30 )
    return S_OK()

  def execute( self ):
    #Get jobs from DB
    result = self.jobDB.selectJobs(  { 'Status': self.__jobStates  } )
    if not result[ 'OK' ]:
      return result
    jobsList = result[ 'Value' ]
    for i in range( len( jobsList ) ):
      jobsList[i] = int( jobsList[i] )
    jobsList.sort()
    self.log.info( "Got %s jobs for this iteration" % len( jobsList ) )
    if not jobsList: return S_OK()
    #Check jobs that are already being optimized
    jobsStillOptimizing = {}
    now = time.time()
    for jobId in self._jobsBeingOptimized:
      if now - self._jobsBeingOptimized[ jobId ] < self.__maxOptimizationTime:
        jobsStillOptimizing[ jobId ] = self._jobsBeingOptimized[ jobId ]
    self._jobsBeingOptimized = jobsStillOptimizing
    filteredJobList = []
    for jobId in jobsList:
      if jobId not in self._jobsBeingOptimized:
        filteredJobList.append( jobId )
        self._jobsBeingOptimized[ jobId ] = now
      else:
        gLogger.info( "Skipping job %s. it's already being optimized" % jobId )
    #Get attrs of jobs to be optimized
    result = self.jobDB.getAttributesForJobList( filteredJobList )
    if not result[ 'OK' ]:
      return result
    jobsToProcess =  result[ 'Value' ]
    for jobId in jobsToProcess:
      self._jobsBeingOptimized[ jobId ] = time.time()
      self.log.info( "== Processing job %s == " % jobId  )
      jobAttrs = jobsToProcess[ jobId ]
      result = self.__dispatchJob( jobId, jobAttrs, False )
      if not result[ 'OK' ]:
        gLogger.error( "There was a problem optimizing job", "JID %s: %s" % ( jobId, result[ 'Message' ] ) )
    return S_OK()
  
  def __dispatchJob( self, jobId, jobAttrs, jobDef, keepOptimizing = True ):
    optimizerName = self.__getNextOptimizerName( jobAttrs )
    if not keepOptimizing or not optimizerName or optimizerName not in self.am_getOption( "ValidOptimizers", self.__defaultValidOptimizers ):
      del( self._jobsBeingOptimized[ jobId ] )
      return S_OK()
    if optimizerName not in self._threadedOptimizers:
      to = ThreadedOptimizer( optimizerName, self.am_getModuleParam( 'fullName' ), 
                              self.__dispatchJob )
      result = to.initialize( self.jobDB, self.jobLoggingDB )
      if not result[ 'OK' ]:
        del( self._jobsBeingOptimized[ jobId ] )
        return result
      self._threadedOptimizers[ optimizerName ] = to
    self._threadedOptimizers[ optimizerName ].optimizeJob( jobId, jobAttrs, jobDef )
    return S_OK()
    
  def __getNextOptimizerName( self, jobAttrs ):
    if jobAttrs[ 'Status' ] == 'Received':
      optList = "JobPath"
    elif jobAttrs[ 'Status' ] == 'Checking':
      optList = jobAttrs[ 'MinorStatus' ]
    else:
      return False
    optList = List.fromChar( optList, "/" )
    if len( optList ) == 1:
      optList.insert( 0, "WorkloadManagement")
    if len( optList ) > 2:
      optList[1] = "/".join( optList[1:] )
    return "/".join( optList )
  

  
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
        gLogger.info( "Can't load %s: %s" % ( opPyPath, str(e) ) )
        continue
      try:
        optimizerClass = getattr( optimizerModule, agentName )
        optimizer = optimizerClass( self.optimizerName, self.containerName )
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
                                         self.optimizer.am_getModuleParam( 'shifterProxyLocation' ) )
        if not result[ 'OK' ]:
          return result
      #Call the initCycle function
      result = self.optimizer.am_secureCall( self.optimizer.beginExecution, name = "beginExecution" )
      if not result[ 'OK' ]:
        return result
      #Do the work
      result = self.optimizer.optimizeJob( jobId, jobDef[ 'classad' ] )
      if not result[ 'OK' ]:
        return result
      nextOptimizer = result[ 'Value' ]
      #If there was a shifter proxy, unset it
      if shifterEnv:
        del( os.environ[ 'X509_USER_PROXY' ] )
      #Check if the JDL has changed
      newJDL = jobDef[ 'classad' ].asJDL()
      if newJDL != jobDef[ 'jdl' ]:
        jobDef[ 'jdl' ] = newJDL
      #If there's a new optimizer set it!
      if nextOptimizer:
        jobAttrs[ 'Status' ] = 'Checking'
        jobAttrs[ 'MinorStatus' ] = nextOptimizer
      self.dispatchFunction( jobId, jobAttrs, jobDef, nextOptimizer )



