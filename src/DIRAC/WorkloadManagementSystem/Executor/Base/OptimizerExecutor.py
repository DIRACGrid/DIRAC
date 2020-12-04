""" Base class for all the executor modules for Jobs Optimization
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import threading
import datetime  # Because eval(valenc) might require it
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities import DEncode, List
from DIRAC.Core.Base.ExecutorModule import ExecutorModule
from DIRAC.WorkloadManagementSystem.Client.JobState.CachedJobState import CachedJobState
from DIRAC.WorkloadManagementSystem.Client import JobStatus


class OptimizerExecutor(ExecutorModule):

  class JobLog:

    class LogWrap:

      def __init__(self, log, jid, funcName):
        self.__log = log
        self.__jid = jid
        self.__funcName = funcName

      def __call__(self, msg, varMsg=""):
        try:
          funcObj = getattr(self.__log, self.__funcName)
        except AttributeError:
          raise AttributeError("Logger does not have %s method" % self.__funcName)
        msg = "\n".join("[JID %s] %s" % (self.__jid, line) for line in msg.split("\n"))
        funcObj(msg, varMsg)

    def __init__(self, log, jid):
      self.__jid = jid
      self.__log = log

    def __bool__(self):
      return True

    # For Python 2 compatibility
    __nonzero__ = __bool__

    def __getattr__(self, name):
      return self.LogWrap(self.__log, self.__jid, name)

  @classmethod
  def initialize(cls):
    opName = cls.ex_getProperty('fullName')
    opName = "/".join(opName.split("/")[1:])
    if opName.find("Agent") == len(opName) - 5:
      opName = opName[:-5]
    cls.__optimizerName = opName
    maxTasks = cls.ex_getOption('Tasks', 1)
    cls.__jobData = threading.local()

    cls.__jobData.jobState = None
    cls.__jobData.jobLog = None
    cls.ex_setProperty('optimizerName', cls.__optimizerName)
    try:
      result = cls.initializeOptimizer()
      if not result['OK']:
        return result
    except Exception as excp:
      cls.log.exception("Error while initializing optimizer")
      return S_ERROR("Error initializing: %s" % str(excp))
    cls.ex_setMind("WorkloadManagement/OptimizationMind")

    return S_OK()

  @classmethod
  def ex_optimizerName(cls):
    return cls.__optimizerName

  @classmethod
  def initializeOptimizer(cls):
    return S_OK()

  def processTask(self, jid, jobState):
    self.__jobData.jobState = jobState
    self.__jobData.jobLog = self.JobLog(self.log, jid)
    try:
      self.jobLog.info("Processing")
      optResult = self.optimizeJob(jid, jobState)
      # If the manifest is dirty, update it!
      result = jobState.getManifest()
      if not result['OK']:
        self.jobLog.error("Failed to get job manifest", result['Message'])
        return result
      manifest = result['Value']
      if manifest.isDirty():
        jobState.setManifest(manifest)
      # Did it go as expected? If not Failed!
      if not optResult['OK']:
        self.jobLog.info("Set to Failed/%s" % optResult['Message'])
        minorStatus = "%s optimizer" % self.ex_optimizerName()
        return jobState.setStatus("Failed", minorStatus, optResult['Message'], source=self.ex_optimizerName())

      return S_OK()
    finally:
      self.__jobData.jobState = None
      self.__jobData.jobLog = None

  def optimizeJob(self, jid, jobState):
    raise Exception("You need to overwrite this method to optimize the job!")

  def setNextOptimizer(self, jobState=None):
    if not jobState:
      jobState = self.__jobData.jobState
    result = jobState.getOptParameter('OptimizerChain')
    if not result['OK']:
      return result
    opChain = List.fromChar(result['Value'], ",")
    opName = self.__optimizerName
    try:
      opIndex = opChain.index(opName)
    except ValueError:
      return S_ERROR("Optimizer %s is not in the chain!" % opName)
    chainLength = len(opChain)
    if chainLength - 1 == opIndex:
      # This is the last optimizer in the chain!
      result = jobState.setStatus(self.ex_getOption('WaitingStatus', 'Waiting'),
                                  minorStatus=self.ex_getOption('WaitingMinorStatus', 'Pilot Agent Submission'),
                                  appStatus="Unknown",
                                  source=opName)
      if not result['OK']:
        return result

      result = jobState.insertIntoTQ()
      if not result['OK']:
        return result

      return S_OK()
    # Keep optimizing!
    nextOp = opChain[opIndex + 1]
    self.jobLog.info("Set to Checking/%s" % nextOp)
    return jobState.setStatus(JobStatus.CHECKING, nextOp, source=opName)

  def storeOptimizerParam(self, name, value):
    if not self.__jobData.jobState:
      return S_ERROR("This function can only be called inside the optimizeJob function")
    valenc = DEncode.encode(value)
    return self.__jobData.jobState.setOptParameter(name, valenc)

  def retrieveOptimizerParam(self, name):
    if not self.__jobData.jobState:
      return S_ERROR("This function can only be called inside the optimizeJob function")
    result = self.__jobData.jobState.getOptParameter(name)
    if not result['OK']:
      return result
    valenc = result['Value']
    try:
      value, encLength = DEncode.decode(valenc)
      if encLength == len(valenc):
        return S_OK(value)
    except Exception:
      self.jobLog.warn("Opt param %s doesn't seem to be dencoded %s" % (name, valenc))
    return S_OK(eval(valenc))

  @property
  def jobLog(self):
    if not self.__jobData.jobLog:
      raise RuntimeError("jobLog can only be invoked inside the optimizeJob function")
    return self.__jobData.jobLog

  def deserializeTask(self, taskStub):
    return CachedJobState.deserialize(taskStub)

  def serializeTask(self, cjs):
    return S_OK(cjs.serialize())

  def fastTrackDispatch(self, jid, jobState):
    result = jobState.getStatus()
    if not result['OK']:
      return S_ERROR("Could not retrieve job status for %s: %s" % (jid, result['Message']))
    status, minorStatus = result['Value']
    if status != JobStatus.CHECKING:
      self.log.info("[JID %s] Not in checking state. Avoid fast track" % jid)
      return S_OK()
    result = jobState.getOptParameter("OptimizerChain")
    if not result['OK']:
      return S_ERROR("Could not retrieve OptimizerChain for job %s: %s" % (jid, result['Message']))
    optChain = result['Value']
    if minorStatus not in optChain:
      self.log.info("[JID %s] End of chain for job" % jid)
      return S_OK()
    self.log.info("[JID %s] Fast track possible to %s" % (jid, minorStatus))
    return S_OK("WorkloadManagement/%s" % minorStatus)
