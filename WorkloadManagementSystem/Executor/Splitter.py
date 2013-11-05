########################################################################
# $HeadURL$
# File :    JobPathAgent.py
########################################################################
"""
  The Job Path Agent determines the chain of Optimizing Agents that must
  work on the job prior to the scheduling decision.

  Initially this takes jobs in the received state and starts the jobs on the
  optimizer chain.  The next development will be to explicitly specify the
  path through the optimizers.

"""
__RCSID__ = "$Id$"
import types
from DIRAC import S_OK, S_ERROR
from DIRAC.WorkloadManagementSystem.Executor.Base.OptimizerExecutor  import OptimizerExecutor
from DIRAC.WorkloadManagementSystem.Splitters.BaseSplitter import BaseSplitter
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader

class Splitter( OptimizerExecutor ):
  """
      The specific Optimizer must provide the following methods:
      - checkJob() - the main method called for each job
      and it can provide:
      - initializeOptimizer() before each execution cycle
  """

  @classmethod
  def initializeOptimizer( cls ):
    objLoader = ObjectLoader()
    result = objLoader.getObjects( "WorkloadManagementSystem.Splitters",
                                   reFilter = ".*Splitter",
                                   parentClass = BaseSplitter )
    if not result[ 'OK' ]:
      return result
    data = result[ 'Value' ]
    cls.__splitters = {}
    for k in data:
      spClass = data[k]
      spName = k.split(".")[-1][:-8]
      cls.__splitters[ spName ] = spClass
      cls.log.notice( "Found %s splitter" % spName )
    cls.ex_setOption( "FailedStatus", "Invalid split" )
    return S_OK()

  def optimizeJob( self, jid, jobState ):
    #TODO: Debug error and exception
    result = jobState.getManifest()
    if not result[ 'OK' ]:
      return result
    jobManifest = result[ 'Value' ]
    splitter = jobManifest.getOption( "Splitter", "" )
    if not splitter:
      self.jobLog.info( "Job doesn't have any splitter defined. Ooops! Sending to next optimzer..." )
      return self.setNextOptimizer()
    if splitter not in self.__splitters:
      self.jobLog.error( "Unknown splitter %s" % splitter )
      return S_ERROR( "Unknown splitter %s" % splitter )
    spObj = self.__splitters[ splitter ]( self.jobLog )
    try:
      result = spObj.splitJob( jobState )
    except Exception, excp:
      self.jobLog.exception( "Splitter %s raised exception: %s" % ( splitter, excp ) )
      return S_ERROR( "Splitter %s raised exception: %s" % ( splitter, excp ) )
    if not result[ 'OK' ]:
      return result
    manifests = result[ 'Value' ]
    maxJobs = self.ex_getOption( "MaxParametricJobs", 100 )
    if len( manifests ) > maxJobs:
      self.jobLog.error( "Splitter went beyond maxJobs" )
      return S_ERROR( "Can't generate more than %s jobs" % maxJobs )
    numManifests = len( manifests )
    self.jobLog.notice( "Generated %s manifests" % numManifests )
    for iM in range( numManifests ):
      manifest = manifests[ iM ]
      manifest.remove( "Splitter" )
      manifest.setOption( "SplitID", str( iM ).zfill( len( str( numManifests ) ) ) )
      manifest.setOption( "SplitSourceJob", jid )
      manifest.expand()
    return self.splitJob( manifests )
