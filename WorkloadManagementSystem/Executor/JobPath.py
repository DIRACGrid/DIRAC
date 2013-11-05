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
from DIRAC.Core.Utilities import List
from DIRAC.WorkloadManagementSystem.Executor.Base.OptimizerExecutor  import OptimizerExecutor
from DIRAC.WorkloadManagementSystem.Splitters.BaseSplitter import BaseSplitter
from DIRAC.Core.Utilities.ModuleFactory import ModuleFactory
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader

class JobPath( OptimizerExecutor ):
  """
      The specific Optimizer must provide the following methods:
      - checkJob() - the main method called for each job
      and it can provide:
      - initializeOptimizer() before each execution cycle
  """

  @classmethod
  def initializeOptimizer( cls ):
    cls.__voPlugins = {}
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
      cls.__splitters[ spName ] = spClass.AFTER_OPTIMIZER
      cls.log.notice( "Found %s splitter that goes after %s" % ( spName, spClass.AFTER_OPTIMIZER ) )
    cls.ex_setOption( "FailedStatus", "Cannot generate optimization path" )
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
        self.jobLog.exception( "Could not import VO plugin %s" % voPlugin )
        return S_ERROR( "Could not import VO plugin %s: %s" % ( voPlugin, excp ) )

      try:
        self.__voPlugins[ voPlugin ] = getattr( module, modName )
      except AttributeError, excp:
        return S_ERROR( "Could not get plugin %s from module %s: %s" % ( modName, voPlugin, str( excp ) ) )

    argsDict = { 'JobID': jobState.jid,
                 'JobState' : jobState,
                 'ConfigPath':self.ex_getProperty( "section" ) }
    try:
      modInstance = self.__voPlugins[ voPlugin ]( argsDict )
      result = modInstance.execute()
    except Exception, excp:
      self.jobLog.exception( "Excp while executing %s" % voPlugin )
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
      self.jobLog.info( 'Job defines its own optimizer chain %s' % opChain )
      return self.__setOptimizerChain( jobState, opChain )
    #Construct path
    #Add JobPath
    opPath = [ 'JobSanity' ]
    if jobManifest.getOption( "InputData", "" ):
      self.jobLog.info( 'Input data requirement found' )
      opPath.extend( self.ex_getOption( "InputData", [ 'InputDataResolution', 'InputDataValidation' ] ) )
    else:
      self.jobLog.info( 'No input data requirement' )
    opPath.extend( self.ex_getOption( 'EndPath', ['JobScheduling'] ) )

    voPlugin = self.ex_getOption( 'VOPlugin', '' )
    #Specific VO path
    if voPlugin:
      result = self.__executeVOPlugin( voPlugin, jobState, opPath )
      if not result[ 'OK' ]:
        return result
      voPath = result[ 'Value' ]
      if voPath:
        self.jobLog.verbose( 'Setting specific VO path' % ( voPath ) )
        opPath = voPath
    else:
      self.jobLog.verbose( 'No VO specific plugin module specified' )
    #Make sure there are no duplicates
    finalPath = [ 'JobPath' ]
    for opN in opPath:
      if opN not in finalPath:
        finalPath.append( opN )
      else:
        self.jobLog.notice( "Duplicate optimizer %s in path: %s" % ( opN, opPath ) )
    #Parametric magic
    splitter = jobManifest.getOption( "Splitter", "" )
    if splitter:
      if splitter not in self.__splitters:
        return S_ERROR( "Unknown splitter %s" % splitter )
      prevOpt = self.__splitters[ splitter ]
      try:
        opIndex = finalPath.index( prevOpt )
      except ValueError:
        return S_ERROR( "Cannot use %s splitter. Job won't go through required optimizer %s" % ( splitter, prevOpt ) )
      finalPath.insert( opIndex + 1, "Splitter" )
      self.jobLog.notice( "Added Splitter %s after %s" % ( splitter, prevOpt ) )

    self.jobLog.info( 'Constructed path is: %s' % "->".join( finalPath ) )
    result = self.__setOptimizerChain( jobState, finalPath )
    if not result['OK']:
      return result
    return self.setNextOptimizer( jobState )
