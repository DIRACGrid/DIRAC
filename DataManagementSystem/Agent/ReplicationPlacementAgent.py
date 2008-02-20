"""  ReplicationPlacementAgent determines the replications to be performed based on operations defined in the operations database
"""

from DIRAC  import gLogger,gMonitor, gConfig, S_OK, S_ERROR
from DIRAC.Core.Base.Agent import Agent
from DIRAC.RequestManagementSystem.Client.Request import RequestClient
from DIRAC.RequestManagementSystem.Client.DataManagementRequest import DataManagementRequest
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from DIRAC.DataManagementSystem.Client.FileCatalog import FileCatalog
from DIRAC.DataManagementSystem.Client.Catalog.PlacementDBClient import PlacementDBClient
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.DISET.RPCClient import RPCClient

import time
from types import *

AGENT_NAME = 'DataManagement/ReplicationPlacementAgent'

class ReplicationPlacementAgent(Agent):

  def __init__(self):
    """ Standard constructor
    """
    Agent.__init__(self,AGENT_NAME)

  def initialize(self):
    result = Agent.initialize(self)
    self.transferDBUrl = PathFinder.getServiceURL('RequestManagement/centralURL')
    self.TransferDB = RequestClient()
    self.PlacementDB = PlacementDBClient()
    self.server = RPCClient("DataManagement/PlacementDB")
    self.DataLog = RPCClient('DataManagement/DataLogging')
    gMonitor.registerActivity("Iteration","Agent Loops","ReplicationPlacementAgent","Loops/min",gMonitor.OP_SUM)
    return result

  def execute(self):
    gMonitor.addMark('Iteration',1)

    transName = gConfig.getValue(self.section+'/Transformation')
    if transName:
      self.singleTransformation = transName
      gLogger.info("Initializing Replication Agent for transformation %s." % transName)
    else:
      self.singleTransformation = False
      gLogger.info("ReplicationPlacementAgent.execute: Initializing general purpose agent.")

    res = self.server.getAllTransformations()
    activeTransforms = []
    if not res['OK']:
      gLogger.error("ReplicationPlacementAgent.execute: Failed to get transformations.", res['Message'])

    for transDict in res['Value']:
      transName = transDict['Name']
      transStatus = transDict['Status']

      processTransformation = True
      if self.singleTransformation:
        if not self.singleTransformation == transName:
          gLogger.info("ReplicationPlacementAgent.execute: Skipping %s (not selected)." % transName)
          processTransformation = False

      if processTransformation:
        startTime = time.time()
        # process the active transformations
        if transStatus == 'Active':
          gLogger.info("ReplicationPlacementAgent.execute: Processing transformation '%s'." % transName)
          res = self.processTransformation(transDict, False)
          gLogger.info("ReplicationPlacementAgent.execute: Transformation '%s' processed in %s seconds." % (transName,time.time()-startTime))

        # flush transformations
        elif transStatus == 'Flush':
          gLogger.info("ReplicationPlacementAgent.execute: Flushing transformation '%s'." % transName)
          res = self.processTransformation(transDict, True)
          if not res['OK']:
            gLogger.error("ReplicationPlacementAgent.execute: Failed to flush transformation '%s'." % transName, res['Message'])
          else:
            gLogger.info("ReplicationPlacementAgent.execute: Transformation '%s' flushed in %s seconds." % (transName,time.time()-startTime))
            res = self.server.setTransformationStatus(transName, 'Stopped')
            if not res['OK']:
              gLogger.error("ReplicationPlacementAgent.execute: Failed to update transformation status to 'Stopped'.", res['Message'])
            else:
              gLogger.info("ReplicationPlacementAgent.execute: Updated transformation status to 'Stopped'.")

        # skip the transformations of other statuses
        else:
          gLogger.info("ReplicationPlacementAgent.execute: Skipping transformation '%s' with status %s." % (transName,transStatus))

    return S_OK()

  def processTransformation(self,transDict,flush=False):
    """ Process a single transformation
    """
    transName = transDict['Name']
    res = self.server.getInputData(transName,'AprioriGood')
    if not res['OK']:
      errStr = "ReplicationPlacementAgent.processTransformation: Failed to obtain input data."
      gLogger.error(errStr,res['Message'])
      return S_ERROR(errStr)
    data = res['Value']
    gLogger.info("ReplicationPlacementAgent.processTransformation: %s files found for transformation '%s'." % (len(data),transName))

    if not transDict.has_key('Plugin'):
      errStr = "ReplicationPlacementAgent.processTransformation: No plugin defined."
      gLogger.error(errStr,transName)
      return S_ERROR(errStr)
    plugin = transDict['Plugin']
    gLogger.info("ReplicationPlacementAgent.processTransformation: Processing transformation '%s' with '%s' plugin." % (transName,plugin))

    res = self._generatePluginObject(plugin)
    if not res['OK']:
      errStr = "ReplicationPlacementAgent.processTransformation: Failed to instatiate plugin."
      gLogger.error(errStr,plugin)
      return S_ERROR(errStr)
    oPlugin = res['Value']

    sourceSE = ''
    if transDict.has_key('Additional'):
      oPlugin.setParameters(transDict['Additional'])
      if transDict['Additional'].has_key('SourceSE'):
        sourceSE = transDict['Additional']['SourceSE']
    oPlugin.setInputData(data)

    res = oPlugin.generateTask()
    if not res['OK']:
      errStr = "ReplicationPlacementAgent.processTransformation: Failed to generate task for transformation."
      gLogger.error(errStr,res['Message'])
      return S_ERROR(errStr)
    seFiles = res['Value']
    if not seFiles:
      gLogger.info("ReplicationPlacementAgent.processTransformation: Sufficient number of files not found for %s." % transName)
      return S_OK()
		
    res = self.submitRequest(sourceSE,seFiles,transName)
    if not res['OK']:
      errStr = "ReplicationPlacementAgent.processTransformation: Failed to process task for transformation."
      gLogger.error(errStr,res['Message'])
    else:
      for targetSE,lfns in seFiles.items():
        for lfn in lfns:
          self.DataLog.addFileRecord(lfn,'Tier1Assigned',targetSE,'','ReplicationPlacementAgent')
        res = self.server.setFileStatusForTransformation(transName,'Assigned',lfns)
        if not res['OK']:
          errStr = "ReplicationPlacementAgent.processTransformation: Failed to update file status."
          gLogger.error(errStr,res['Message'])
        res = self.server.setFileSEForTransformation(transName,targetSE,lfns)
        if not res['OK']:
          errStr = "ReplicationPlacementAgent.processTransformation: Failed to update file status."
          gLogger.error(errStr,res['Message'])
    return S_OK()

  def submitRequest(self,sourceSE,targetSEFiles,transName):
    oRequest = DataManagementRequest()
    for targetSE,lfns in targetSEFiles.items():
      subRequestIndex = oRequest.initiateSubRequest('transfer')['Value']
      attributeDict = {'Operation':'replicateAndRegister','TargetSE':targetSE,'SourceSE':sourceSE}
      oRequest.setSubRequestAttributes(subRequestIndex,'transfer',attributeDict)
      files = []
      for lfn in lfns:
        files.append({'LFN':lfn})
      oRequest.setSubRequestFiles(subRequestIndex,'transfer',files)
    requestString = oRequest.toXML()['Value']
    requestName = '%s_transfer_%s.xml' % (transName,time.time())
    res = self.TransferDB.setRequest(requestName,requestString,self.transferDBUrl)
    if not res['OK']:
      gLogger.error("ReplicationPlacementAgent.processTransformation: Failed to set request to TransferDB.", res['Message'])
    else:
      gLogger.info("ReplicationPlacementAgent.processTransformation: Successfully put %s to TransferDB." %  requestName)
    return res

  def _generatePluginObject(self,plugin):
    """ This simply instantiates the TransformationPlugin class with the relevant plugin name
    """
    try:
      plugModule = __import__('DIRAC.Core.Transformation.TransformationPlugin',globals(),locals(),['TransformationPlugin'])
    except Exception, x:
      errStr = "ReplicationPlacementAgent._generatePluginObject: Failed to import 'TransformationPlugin': %s" % (x)
      gLogger.exception(errStr)
      return S_ERROR(errStr)
    try:
      evalString = "plugModule.TransformationPlugin('%s')" % plugin
      oPlugin = eval(evalString)
      if not oPlugin.isOK():
        errStr = "ReplicationPlacementAgent._generatePluginObject: Failed to instatiate plug in."
        gLogger.error(errStr,plugin)
        return S_ERROR(errStr)
    except Exception, x:
      errStr = "ReplicationPlacementAgent._generatePluginObject: Failed to instatiate  %s()." % plugin
      gLogger.exception(errStr,str(x))
      return S_ERROR(errStr)
    return S_OK(oPlugin)
