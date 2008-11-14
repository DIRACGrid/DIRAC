########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/Agent/MightyOptimizer.py,v 1.1 2008/11/14 16:20:50 acasajus Exp $


"""  SuperOptimizer
 One optimizer to rule them all, one optimizer to find them, one optimizer to bring them all, and in the darkness bind them.
"""
import time
import os
import threading
from DIRAC  import gLogger, gConfig, gMonitor,S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.WorkloadManagementSystem.DB.JobDB         import JobDB
from DIRAC.WorkloadManagementSystem.DB.JobLoggingDB  import JobLoggingDB
from DIRAC.Core.Utilities import Time, ThreadSafe


gOptimizerLoadSync = ThreadSafe.Synchronizer()

class MightyOptimizer(AgentModule):

  __jobStates = [ 'Received', 'Checking' ]

  def initialize(self):
    """ Standard constructor
    """
    self.jobDB = JobDB()
    self.jobLoggingDB = JobLoggingDB()
    self._optimizers = {}

  def execute( self ):
    result = self.jobDB.selectJobs(  { 'Status': self.__jobStates  } )
    if not result[ 'OK' ]:
      return result
    jobsList = result[ 'Value' ]
    gLogger.info( "Got %s jobs for this iteration" % len( jobsList ) )
    result = self.jobDB.getAttributesForJobList( jobsList )
    if not result[ 'OK' ]:
      return result
    jobsToProcess =  result[ 'Value' ]
    for job in jobsToProcess:
      jobData = jobsToProcess[ job ]
      result = self._getNextOptimizer( jobData )
      print result


  def _getNextOptimizer( self, jobData ):
    if jobData[ 'Status' ] == 'Received':
      nextOptimizer = "JobPath"
    else:
      nextOptimizer = jobData[ 'MinorStatus' ]
    gLogger.info( "Next optimizer for job %s is %s" % ( jobData['JobID'], nextOptimizer ) )
    if nextOptimizer in self._optimizers:
      return S_OK( self._optimizers[ nextOptimizer ] )
    return self.__loadOptimizer( nextOptimizer )

  @gOptimizerLoadSync
  def __loadOptimizer( self, optimizerName ):
    #Need to load an optimizer
    gLogger.info( "Loading optimizer %s" % optimizerName )
    try:
      agentName = "%sAgent" % optimizerName
      optimizerModule = __import__( 'DIRAC.WorkloadManagementSystem.Agent.%s' % agentName,
                              globals(),
                              locals(), agentName )
      optimizerClass = getattr( optimizerModule, agentName )
      optimizer = optimizerClass()
      result = optimizer.initialize()
      if not result[ 'OK' ]:
        return S_ERROR( errorMsg = "Can't initialize optimizer %s: %s" % ( optimizerName, result[ 'Message' ] ) )
    except Exception, e:
      gLogger.exception( "LOADERROR" )
      return S_ERROR( "Can't load optimizer %s: %s" % ( optimizerName, str(e) ) )
    self._optimizers[ optimizerName ] = { 'lock' : threading.Lock() , 'optimizer' : optimizer }
    return S_OK( optimizer )




