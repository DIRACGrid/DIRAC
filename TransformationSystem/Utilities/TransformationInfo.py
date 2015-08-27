"""TransformationInfo class to be used by ILCTransformation System"""
__RCSID__ = "$Id$"

from DIRAC import gLogger, S_OK

from DIRAC.Core.Workflow.Workflow import fromXMLString
from ILCDIRAC.Core.Utilities.ProductionData import constructProductionLFNs


class TransformationInfo(object):
  """ hold information about transformations """

  def __init__(self, transformationID, transName, tClient, jobDB, logDB):
    self.log = gLogger.getSubLogger("TInfo")
    self.tID = transformationID
    self.transName = transName
    self.transClient = tClient
    self.jobDB = jobDB
    self.logDB = logDB
    self.olist = self.__getOutputList()

  def __getTransformationWorkflow(self):
    """return the workflow for the transformation"""
    res = self.transClient.getTransformationParameters(self.tID, ['Body'])
    if not res['OK']:
      self.log.error('Could not get Body from TransformationDB')
      return res
    body = res['Value']
    workflow = fromXMLString(body)
    workflow.resolveGlobalVars()
    return S_OK(workflow)

  def __getOutputList(self):
    """Get list of outputfiles"""
    resWorkflow = self.__getTransformationWorkflow()
    if not resWorkflow['OK']:
      self.log.error("Failed to get Transformation Workflow")
      raise RuntimeError("Failed to get outputlist")

    workflow = resWorkflow['Value']
    olist = []
    for step in workflow.step_instances:
      param = step.findParameter('listoutput')
      if not param:
        continue
      olist.extend(param.value)
    return olist

  def getOutputFiles(self, taskID):
    """returns list of expected lfns for given task"""
    commons = {'outputList': self.olist,
               'PRODUCTION_ID': int(self.tID),
               'JOB_ID': int(taskID),
               }
    resFiles = constructProductionLFNs(commons)
    if not resFiles['OK']:
      raise RuntimeError("Failed to create productionLFNs")
    expectedlfns = resFiles['Value']['ProductionOutputData']
    return expectedlfns
