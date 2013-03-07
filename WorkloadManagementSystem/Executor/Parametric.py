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
from DIRAC import S_OK, S_ERROR, List
from DIRAC.WorkloadManagementSystem.Executor.Base.OptimizerExecutor  import OptimizerExecutor
from DIRAC.Core.Utilities.ModuleFactory import ModuleFactory

class Parametric( OptimizerExecutor ):
  """
      The specific Optimizer must provide the following methods:
      - checkJob() - the main method called for each job
      and it can provide:
      - initializeOptimizer() before each execution cycle
  """

  @classmethod
  def initializeOptimizer( cls ):
    cls.__voPlugins = {}
    return S_OK()

  def optimizeJob( self, jid, jobState ):
    result = jobState.getManifest()
    if not result[ 'OK' ]:
      return result
    manifest = result[ 'Value' ]
    maxJobs = self.ex_getOption( "MaxParametricJobs", 100 )
    params = manifest.getOption( "Parameters", [] )
    if len( params ) > maxJobs:
      return S_ERROR( "Exceeded maximum number of parametric jobs allowed (%d)" % maxJobs )
    if len( params ) > 1:
      return S_OK( params )
    try:
      numParams = int( params[0] )
    except ValueError:
      return S_ERROR( "Illegal value for Parameters option in the manifest" )
    if numParams > maxJobs:
      return S_ERROR( "Exceeded maximum number of parametric jobs allowed (%d)" % maxJobs )
    pStart = manifest.getOption( 'ParameterStart', 1.0 )
    pStep = manifest.getOption( 'ParameterStep', 0.0 )
    pFactor = manifest.getOption( 'ParameterFactor', 1.0 )

    params = [ pStart ]
    for i in range( numParams -1 ):
      params.append( params[-1] * pFactor + pStep )

    for opName in ( 'Parameters', 'ParameterStart', 'ParameterStep', 'ParameterFactor' ):
      manifest.removeOption( "Parameters" )

    manifestList = []
    sourceManifest = str( manifest.getAsCFG() )
    fillLen = len( str( numParams ) )
    for iP in range( len( params ) ):
      param = params[ iP ]
      manifestList.append( sourceManifest.replace( '%s', param ).replace( '%n', str( iP ).zfill( fillLen ) ) )

    self.setManifestsFromParametric( manifestList )
