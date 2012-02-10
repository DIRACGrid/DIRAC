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
from DIRAC.WorkloadManagementSystem.Agent.Base.OptimizerExecutor  import OptimizerExecutor
from DIRAC.Core.Utilities.ModuleFactory import ModuleFactory

class JobPathAgent( OptimizerExecutor ):
  """
      The specific Optimizer must provide the following methods:
      - checkJob() - the main method called for each job
      and it can provide:
      - initializeOptimizer() before each execution cycle      
  """

  def initializeOptimizer( self ):
    self.__voPlugins = {}
    return S_OK()

  def __setOptimizerChain( self, jobState, opChain ):
    if type( opChain ) not in types.StringTypes:
      opChain = ",".join( opChain )
    result = jobState.setOptParameter( "OptimizerChain", opChain )
    if not result[ 'OK' ]:
      return result
    return jobState.setParameter( "JobPath", opChain )

  def __executeVOPlugin( self, voPlugin, jobState ):
    if voPlugin not in self.__voPlugins:
      modName = List.fromChar( voPlugin, "." )[-1]
      try:
        module = __import__( voPlugin, globals(), locals(), [ modName ] )
      except ImportError, excp:
        self.log.exception( "Could not import VO plugin %s" % voPlugin )
        return S_ERROR( "Could not import VO plugin %s: %s" % ( voPlugin, excp ) )

      try:
        self.__voPlugins[ voPlugin ] = getattr( module, modName )
      except AttributeError, excp:
        return S_ERROR( "Could not get plugin %s from module %s: %s" % ( modName, voPlugin, str( excp ) ) )

    argsDict = { 'JobID': jobState.jid,
                 'JobState' : jobState,
                 'ConfigPath':self.am_getModuleParam( "section" ) }
    try:
      modInstance = self.__voPlugins[ voPlugin ]( argsDict )
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
    result = jobState.getManifest()
    if not result[ 'OK' ]:
      return result
    jobManifest = result[ 'Value' ]
    opChain = jobManifest.getOption( "JobPath", [] )
    if opChain:
      self.log.info( 'Job %s defines its own optimizer chain %s' % ( jid, jobPath ) )
      return self.__setOptimizerChain( jobState, opChain )
    #Construct path
    opPath = self.am_getOption( 'BasePath', ['JobPath', 'JobSanity'] )
    voPlugin = self.am_getOption( 'VOPlugin', '' )
    #Specific VO path
    if voPlugin:
      result = self.__executeVOPlugin( voPlugin, jobState )
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
        self.log.info( 'Job %s has an input data requirement' % ( jid ) )
        opPath.extend( self.am_getOption( 'InputData', ['InputData'] ) )
      else:
        self.log.info( 'Job %s has no input data requirement' % ( jid ) )
    #End of path
    opPath.extend( self.am_getOption( 'EndPath', ['JobScheduling', 'TaskQueue'] ) )
    self.log.info( 'Constructed path for job %s is: %s' % ( jid, "->".join( opPath ) ) )
    result = self.__setOptimizerChain( jobState, opPath )
    if not result['OK']:
      return result
    return self.setNextOptimizer( jobState )
