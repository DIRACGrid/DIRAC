########################################################################
# $HeadURL$
# File :    JobRequirements.py
########################################################################

'''
Created on Nov 4, 2013

@author: sposs
'''
__RCSID__ = "$Id$"

from DIRAC.WorkloadManagementSystem.Executor.Base.OptimizerExecutor  import OptimizerExecutor
from DIRAC                                                           import S_OK, S_ERROR

class JobRequirements ( OptimizerExecutor ):
  """ Evaluate the job requirements if any, and store them in the relevant DBs
  """
  @classmethod
  def initializeOptimizer( cls ):
    """Initialize specific parameters
    """

    return S_OK()
  
  def optimizeJob( self, jid, jobState ):
    """ Do the magic
    """
    result = jobState.getManifest()
    if not result[ 'OK' ]:
      self.jobLog.notice( "Can't retrieve manifest. %s" % result[ 'Message' ] )
      return result
    jobManifest = result[ 'Value' ]
    
    requirements = jobManifest.getOption("Requirements", {})
    if not requirements:
      self.jobLog.notice( "No Requirements. Skipping." )
      return self.setNextOptimizer()

    result = jobState.getRequirements()
    if result[ 'OK' ] and result[ 'Value' ]:
      self.jobLog.notice( "Already resolved Requirements, skipping" )
      return self.setNextOptimizer()
    
    
    #Now get the Queues that match those requirements
    #Insert first in the SubmitPools table to get a hash
    result = jobState.insertSubmitPool( requirements )
    if not result["OK"]:
      return result
    requ_hash = result["Value"]
    
    queues = []
    ##This is where there should be a Registry lookup
    result  = jobState.insertQueueRequirementRelation(requ_hash, queues)
    if not result['OK']:
      return result
    
    #Now define the SubmitPool so that it's added to the TQ DB properly, needed for matching
    jobManifest.setOption("SubmitPool", requ_hash)
    
    #Finally, add those requirements to the JobDB, means all went fine
    result = jobState.setRequirements( requirements )
    if not result[ 'OK' ]:
      return result
    
    
    return self.setNextOptimizer()
