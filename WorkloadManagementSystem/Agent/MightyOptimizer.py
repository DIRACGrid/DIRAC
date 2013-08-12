########################################################################
# $HeadURL$
# File :   MightyOptimizer.py
# Author : Adria Casajus
########################################################################

"""
  SuperOptimizer
  One optimizer to rule them all, one optimizer to find them,
  one optimizer to bring them all, and in the darkness bind them.
"""
__RCSID__ = "$Id$"

import os
from DIRAC  import gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.WorkloadManagementSystem.DB.JobDB         import JobDB
from DIRAC.WorkloadManagementSystem.DB.JobLoggingDB  import JobLoggingDB
from DIRAC.Core.Utilities import ThreadSafe
from DIRAC.Core.Utilities.Shifter import setupShifterProxyInEnv


gOptimizerLoadSync = ThreadSafe.Synchronizer()

class MightyOptimizer( AgentModule ):
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

  def initialize( self ):
    """ Standard constructor
    """
    self.jobDB = JobDB()
    self.jobLoggingDB = JobLoggingDB()
    self._optimizers = {}
    self.am_setOption( "PollingTime", 30 )
    return S_OK()

  def execute( self ):
    """ The method call by AgentModule on each iteration
    """
    jobTypeCondition = self.am_getOption( "JobTypeRestriction", [] )
    jobCond = { 'Status': self.__jobStates  }
    if jobTypeCondition:
      jobCond[ 'JobType' ] = jobTypeCondition
    result = self.jobDB.selectJobs( jobCond )
    if not result[ 'OK' ]:
      return result
    jobsList = result[ 'Value' ]
    self.log.info( "Got %s jobs for this iteration" % len( jobsList ) )
    if not jobsList:
      return S_OK()
    result = self.jobDB.getAttributesForJobList( jobsList )
    if not result[ 'OK' ]:
      return result
    jobsToProcess = result[ 'Value' ]
    for jobId in jobsToProcess:
      self.log.info( "== Processing job %s == " % jobId )
      jobAttrs = jobsToProcess[ jobId ]
      jobDef = False
      jobOptimized = False
      jobOK = True
      while not jobOptimized:
        result = self.optimizeJob( jobId, jobAttrs, jobDef )
        if not result[ 'OK' ]:
          self.log.error( "Optimizer %s error" % jobAttrs[ 'MinorStatus' ], "Job %s: %s" % ( str(jobId), result[ 'Message' ] ) )
          jobOK = False
          break
        optResult = result[ 'Value' ]
        jobOptimized = optResult[ 'done' ]
        if 'jobDef' in optResult:
          jobDef = optResult[ 'jobDef' ]
      if jobOK:
        self.log.info( "Finished optimizing job %s" % jobId )
    return S_OK()


  def optimizeJob( self, jobId, jobAttrs, jobDef ):
    """ The method call for each Job to be optimized
    """
    #Get the next optimizer
    result = self._getNextOptimizer( jobAttrs )
    if not result[ 'OK' ]:
      return result
    optimizer = result[ 'Value' ]
    if not optimizer:
      return S_OK( { 'done' : True } )
    #If there's no job def then get it
    if not jobDef:
      result = optimizer.getJobDefinition( jobId, jobDef )
      if not result['OK']:
        optimizer.setFailedJob( jobId, result[ 'Message' ] )
        return result
      jobDef = result[ 'Value' ]
    #Does the optimizer require a proxy?
    shifterEnv = False
    if optimizer.am_getModuleParam( 'shifterProxy' ):
      shifterEnv = True
      result = setupShifterProxyInEnv( optimizer.am_getModuleParam( 'shifterProxy' ),
                                       optimizer.am_getShifterProxyLocation() )
      if not result[ 'OK' ]:
        return result
    #Call the initCycle function
    result = self.am_secureCall( optimizer.beginExecution, name = "beginExecution" )
    if not result[ 'OK' ]:
      return result
    #Do the work
    result = optimizer.optimizeJob( jobId, jobDef[ 'classad' ] )
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
      return S_OK( { 'done' : False, 'jobDef' : jobDef } )
    return S_OK( { 'done' : True, 'jobDef' : jobDef } )

  def _getNextOptimizer( self, jobAttrs ):
    """ Determine next Optimizer in the Path
    """
    if jobAttrs[ 'Status' ] == 'Received':
      nextOptimizer = "JobPath"
    else:
      nextOptimizer = jobAttrs[ 'MinorStatus' ]
    if nextOptimizer in self.am_getOption( "FilteredOptimizers", "InputData, BKInputData" ):
      return S_OK( False )
    gLogger.info( "Next optimizer for job %s is %s" % ( jobAttrs['JobID'], nextOptimizer ) )
    if nextOptimizer not in self._optimizers:
      result = self.__loadOptimizer( nextOptimizer )
      if not result[ 'OK' ]:
        return result
      self._optimizers[ nextOptimizer ] = result[ 'Value' ]
    return S_OK( self._optimizers[ nextOptimizer ] )

  @gOptimizerLoadSync
  def __loadOptimizer( self, optimizerName ):
    """Need to load an optimizer
    """
    gLogger.info( "Loading optimizer %s" % optimizerName )
    try:
      agentName = "%sAgent" % optimizerName
      optimizerModule = __import__( 'DIRAC.WorkloadManagementSystem.Agent.%s' % agentName,
                              globals(),
                              locals(), agentName )
      optimizerClass = getattr( optimizerModule, agentName )
      optimizer = optimizerClass( "WorkloadManagement/%s" % agentName, self.am_getModuleParam( 'fullName' ) )
      result = optimizer.am_initialize( self.jobDB, self.jobLoggingDB )
      if not result[ 'OK' ]:
        return S_ERROR( "Can't initialize optimizer %s: %s" % ( optimizerName, result[ 'Message' ] ) )
    except Exception, e:
      gLogger.exception( "LOADERROR" )
      return S_ERROR( "Can't load optimizer %s: %s" % ( optimizerName, str( e ) ) )
    return S_OK( optimizer )




