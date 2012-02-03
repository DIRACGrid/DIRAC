########################################################################
# $HeadURL$
# File :    JobPathAgent.py
# Author :  Stuart Paterson
########################################################################
""" 
  The Job Path Agent determines the chain of Optimizing Agents that must
  work on the job prior to the scheduling decision.

  Initially this takes jobs in the received state and starts the jobs on the
  optimizer chain.  The next development will be to explicitly specify the
  path through the optimizers.

"""
__RCSID__ = "$Id$"

from DIRAC.WorkloadManagementSystem.Agent.OptimizerExecutor  import OptimizerExecutor
from DIRAC                                                 import S_OK, S_ERROR, List


from DIRAC.ConfigurationSystem.Client.Config               import gConfig
from DIRAC.Core.Utilities.ClassAd.ClassAdLight             import ClassAd
from DIRAC.Core.Utilities.ModuleFactory                    import ModuleFactory
#TODO: ADRI JobDescription -> JobState
from DIRAC.WorkloadManagementSystem.Client.JobDescription  import JobDescription

class JobPathAgent( OptimizerExecutor ):
  """
      The specific Optimizer must provide the following methods:
      - checkJob() - the main method called for each job
      and it can provide:
      - initializeOptimizer() before each execution cycle      
  """

  def initializeOptimizer( self ):
    self.__voModules = {}
    return S_OK()

  def __setOptimizerChain( self, jobState, opChain ):
    if type( opChain ) not in types.StringTypes:
      opChain = ",".join( opChain )
    result = jobState.setOpParameter( "OptimizationChain", opChain )
    if not result[ 'OK' ]:
      return result
    return jobState.setParameter( "JobPath", opChain )

  def __executeVOPlugin( self, voPlugin ):
    if voPlugin not in self.__voModules:
      modName = List.fromChar( voPlugin, "." )[-1]
      try:
        self.__voModules[ voPlugin ] = __import__( voPlugin, globals(), locals(), [ modName ] )
      except ImportError, excp:
        return S_ERROR( "Could not import VO plugin %s: %s" % ( voPlugin, excp ) )

    argsDict = { 'JobID':job,
                 'JobState' : jobState,
                 'ConfigPath':self.am_getModuleParam( "section" ) }
    try:
      modInstance = self.__voModules[ voPlugin ]( argsDict )
      result = modInstance.execute()
    except Exception, excp:
      self.log.exception( "Excp while executing %s" % voPlugin )
      return S_ERROR( "Could not execute VO plugin %s: %s" % ( voPlugin, excp ) )

    if not result['OK']:
      return result
    extraPath = result[ 'Value' ]
    if type( extraPath ) in types.StringTypes:
      extraPath = List.fromChar( result['Value'] )
    return S_OK( extraPath )


  def optimizeJob( self, jid, jobState ):
    jobManifest = jobState.getManifest()
    opChain = jobManifest.getOption( "JobPath", [] )
    if opChain:
      self.log.info( 'Job %s defines its own optimizer chain %s' % ( job, jobPath ) )
      return self.__setOptimizerChain( jobState, opChain )
    #Construct path
    opPath = self.am_getOption( 'BasePath', ['JobPath', 'JobSanity'] )
    voPlugin = self.am_getOption( 'VOPlugin', '' )
    #Specific VO path
    if voPlugin:
      result = self.__executeVOPlugin( voPlugin )
      if not result[ 'OK' ]:
        return result
      extraPath = result[ 'Value' ]
      if extraPath:
        opPath.extend( extraPath )
        self.log.verbose( 'Adding extra VO specific optimizers to path: %s' % ( extraPath ) )
    else:
      #Generic path: Should only rely on an input data setting in absence of VO plugin
      self.log.verbose( 'No VO specific plugin module specified' )
      result = jobState.getInputData()
      if not result['OK']:
        return result
      if result['Value']:
        # if the returned tuple is not empty it will evaluate true
        self.log.info( 'Job %s has an input data requirement' % ( job ) )
        opPath.extend( self.am_getOption( 'InputData', ['InputData'] ) )
      else:
        self.log.info( 'Job %s has no input data requirement' % ( job ) )
    #End of path
    opPath.extend( self.am_getOption( 'EndPath', ['JobScheduling', 'TaskQueue'] ) )
    self.log.info( 'Constructed path for job %s is: %s' % ( jid, opPath ) )
    return self.__setOptimizerChain( jobState, opPath )
