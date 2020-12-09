"""
  The Job Path Agent determines the chain of Optimizing Agents that must
  work on the job prior to the scheduling decision.

  Initially this takes jobs in the received state and starts the jobs on the
  optimizer chain.  The next development will be to explicitly specify the
  path through the optimizers.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
__RCSID__ = "$Id$"

import six
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities import List
from DIRAC.WorkloadManagementSystem.Executor.Base.OptimizerExecutor import OptimizerExecutor


class JobPath(OptimizerExecutor):
  """
      The specific Optimizer must provide the following methods:
      - optimizeJob() - the main method called for each job
      and it can provide:
      - initializeOptimizer() before each execution cycle
  """

  @classmethod
  def initializeOptimizer(cls):
    cls.__voPlugins = {}
    return S_OK()

  def __setOptimizerChain(self, jobState, opChain):
    if not isinstance(opChain, six.string_types):
      opChain = ",".join(opChain)
    return jobState.setOptParameter("OptimizerChain", opChain)

  def __executeVOPlugin(self, voPlugin, jobState):
    if voPlugin not in self.__voPlugins:
      modName = List.fromChar(voPlugin, ".")[-1]
      try:
        module = __import__(voPlugin, globals(), locals(), [modName])
      except ImportError as excp:
        self.jobLog.exception("Could not import VO plugin", voPlugin)
        return S_ERROR("Could not import VO plugin")

      try:
        self.__voPlugins[voPlugin] = getattr(module, modName)
      except AttributeError as excp:
        return S_ERROR("Could not get plugin %s from module %s: %s" % (modName, voPlugin, str(excp)))

    argsDict = {'JobID': jobState.jid,
                'JobState': jobState,
                'ConfigPath': self.ex_getProperty("section")}
    try:
      modInstance = self.__voPlugins[voPlugin](argsDict)
      result = modInstance.execute()
    except Exception as excp:
      self.jobLog.exception("Excp while executing", voPlugin)
      return S_ERROR("Could not execute VO plugin")

    if not result['OK']:
      return result
    extraPath = result['Value']
    if isinstance(extraPath, six.string_types):
      extraPath = List.fromChar(result['Value'])
    return S_OK(extraPath)

  def optimizeJob(self, jid, jobState):
    result = jobState.getManifest()
    if not result['OK']:
      self.jobLog.error("Failed to get job manifest", result['Message'])
      return result
    jobManifest = result['Value']
    opChain = jobManifest.getOption("JobPath", [])
    if opChain:
      self.jobLog.info('Job defines its own optimizer chain', opChain)
      return self.__setOptimizerChain(jobState, opChain)
    # Construct path
    opPath = self.ex_getOption('BasePath', ['JobPath', 'JobSanity'])
    voPlugin = self.ex_getOption('VOPlugin', '')
    # Specific VO path
    if voPlugin:
      result = self.__executeVOPlugin(voPlugin, jobState)
      if not result['OK']:
        return result
      extraPath = result['Value']
      if extraPath:
        opPath.extend(extraPath)
        self.jobLog.verbose('Adding extra VO specific optimizers to path', extraPath)
    else:
      # Generic path: Should only rely on an input data setting in absence of VO plugin
      self.jobLog.verbose('No VO specific plugin module specified')
      result = jobState.getInputData()
      if not result['OK']:
        self.jobLog.error("Failed to get input data", result['Message'])
        return result
      if result['Value']:
        # if the returned tuple is not empty it will evaluate true
        self.jobLog.info('Input data requirement found')
        opPath.extend(self.ex_getOption('InputData', ['InputData']))
      else:
        self.jobLog.info('No input data requirement')
    # End of path
    opPath.extend(self.ex_getOption('EndPath', ['JobScheduling']))
    uPath = []
    for opN in opPath:
      if opN not in uPath:
        uPath.append(opN)
    opPath = uPath
    self.jobLog.info('Constructed path is', '%s' % "->".join(opPath))
    result = self.__setOptimizerChain(jobState, opPath)
    if not result['OK']:
      self.jobLog.error("Failed to set optimizer chain", result['Message'])
      return result
    return self.setNextOptimizer(jobState)
