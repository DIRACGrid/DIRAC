""" Class that contains client access to the transformation DB handler.
"""
########################################################################
# $Id$
########################################################################

from DIRAC                                          import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Resources.Catalog.FileCatalogueBase      import FileCatalogueBase
from DIRAC.Core.Utilities.List                      import breakListIntoChunks
from DIRAC.Core.DISET.RPCClient                     import RPCClient
import types

class TransformationDBClient(FileCatalogueBase):
  """ Exposing the functionality of the replica tables for the TransformationDB
  """
  def setServer(self,url):
    self.server = url

  ###########################################################################
  #
  # These methods are for adding new tasks to the transformation database
  #

  def addTaskForTransformation(self,transID,lfns=[],se='Unknown'):
    server = RPCClient(self.server,timeout=120)
    return addTaskForTransformation(transID,lfns,se)

  #####################################################################
  #
  # These are transformation management methods
  #

  def publishTransformation(self,transName,description,longDescription,fileMask='',groupsize=0,update=False,bkQuery = {},plugin='',transGroup='',transType=''):
    server = RPCClient(self.server,timeout=120)
    return server.publishTransformation(transName,description,longDescription,fileMask,groupsize,update,bkQuery,plugin,transGroup,transType)
  
  def getAllTransformations(self):
    server = RPCClient(self.server,timeout=120)
    return server.getAllTransformations()

  def getTransformationWithStatus(self,status):
    server = RPCClient(self.server,timeout=120)
    return server.getTransformationWithStatus(status)

  def removeTransformation(self,transID):
    server = RPCClient(self.server,timeout=120)
    return server.removeTransformation(transID)

  def setTransformationStatus(self,transID,status):
    server = RPCClient(self.server,timeout=120)
    return server.setTransformationStatus(transID,status)

  def setTransformationAgentType(self,transID,type):
    server = RPCClient(self.server,timeout=120)
    return server.setTransformationAgentType(transID,type)

  def setTransformationType(self,transID,type):
    server = RPCClient(self.server,timeout=120)    
    return server.setTransformationType(transID,type)

  def setTransformationPlugin(self,transID,plugin):
    server = RPCClient(self.server,timeout=120)
    return server.setTransformationPlugin(transID,plugin)

  def setTransformationMask(self,transID,mask):
    server = RPCClient(self.server,timeout=120)
    return server.setTransformationMask(transID,mask)
  
  def updateTransformation(self,transID):
    server = RPCClient(self.server,timeout=120)
    return server.updateTransformation(transID)

  def addTransformationParameter(self,transID,paramname,paramvalue):
    server = RPCClient(self.server,timeout=120)
    return server.addTransformationParameter(transID,paramname,paramvalue)

  def addTransformationParameters(self,transID,paramDict):
    server = RPCClient(self.server,timeout=120)
    return server.addTransformationParameters(transID,paramDict)

  def changeTransformationName(self,transID,name):
    server = RPCClient(self.server,timeout=120)
    return server.changeTransformationName(transID,name)

  def getTransformation(self,transID):
    server = RPCClient(self.server,timeout=120)
    return server.getTransformation(transID)

  def getTransformationLastUpdate(self,transID):
    server = RPCClient(self.server,timeout=120)
    return server.getTransformationLastUpdate(transID)

  def getTransformationStats(self,transID):
    server = RPCClient(self.server,timeout=120)
    return server.getTransformationStats(transID)

  def getTransformationLogging(self,transID):
    server = RPCClient(self.server,timeout=120)
    return server.getTransformationLogging(transID)

  def getInputData(self,transID,status):
    server = RPCClient(self.server,timeout=120)
    return server.getInputData(transID,status)

  def getTransformationLFNs(self,transID,status='Unused'):
    server = RPCClient(self.server,timeout=120)
    return server.getTransformationLFNs(transID,status)

  def getFilesForTransformation(self,transID,orderByJobs=False):
    server = RPCClient(self.server,timeout=120)
    return server.getFilesForTransformation(transID,orderByJobs)

  def addLFNsToTransformation(self,lfns,transID):
    server = RPCClient(self.server,timeout=120)
    return server.addLFNsToTransformation(lfns,transID)

  def setFileStatusForTransformation(self,transID,status,lfns):
    server = RPCClient(self.server,timeout=120)
    return server.setFileStatusForTransformation(transID,status,lfns)

  def setFileSEForTransformation(self,transID,se,lfns):
    server = RPCClient(self.server,timeout=120)
    return server.setFileSEForTransformation(transID,se,lfns)

  def resetFileStatusForTransformation(self,transID,lfns):
    server = RPCClient(self.server,timeout=120)
    return server.resetFileStatusForTransformation(transID,lfns)  

  def setFileJobID(self,transID,jobID,lfns):
    server = RPCClient(self.server,timeout=120)
    return server.setFileJobID(transID,jobID,lfns)

  def getFileSummary(self,transID,lfns):
    server = RPCClient(self.server,timeout=120)
    return server.getFileSummary(lfns,transID)
  
  #####################################################################
  #
  # These are the bk query manipulation methods
  #

  def addBookkeepingQuery(self,queryDict):
    server = RPCClient(self.server,timeout=120)
    return server.addBookkeepingQuery(queryDict)

  def getBookkeepingQuery(self,bkQueryID):
    server = RPCClient(self.server,timeout=120)
    return server.getBookkeepingQuery(bkQueryID)

  def setTransformationQuery(self,transID,queryID):
    server = RPCClient(self.server,timeout=120)
    return server.setTransformationQuery(transID,queryID)

  def getBookkeepingQueryForTransformation(self,transID):
    server = RPCClient(self.server,timeout=120)
    return server.getBookkeepingQueryForTransformation(transID)

  def deleteBookkeepingQuery(self,bkQueryID):
    server = RPCClient(self.server,timeout=120)
    return server.deleteBookkeepingQuery(bkQueryID)

  #####################################################################
  #
  # These are the file catalog interface methods
  #

  def isOK(self):
    return self.valid

  def getName(self,DN=''):
    """ Get the file catalog type name
    """
    return self.name

  def getReplicas(self,lfn):
    res = self.__checkArgumentFormat(lfn)
    if not res['OK']:
      return res
    lfns = res['Value'].keys()
    server = RPCClient(self.server,timeout=120)
    return server.getReplicas(lfns)

  def addFile(self,lfn,force=False):
    res = self.__checkArgumentFormat(lfn)
    if not res['OK']:
      return res
    tuples = []
    for lfn,info in res['Value'].items():
      tuples.append((lfn,info['PFN'],info['Size'],info['SE'],info['GUID'],info['Checksum']))     
    server = RPCClient(self.server,timeout=120)
    return server.addFile(tuples,force)

  def addReplica(self,lfn,force=False):
    res = self.__checkArgumentFormat(lfn)
    if not res['OK']:
      return res
    tuples = []
    for lfn,info in res['Value'].items():
      tuples.append((lfn,info['PFN'],info['SE'],False))
    server = RPCClient(self.server,timeout=120)
    return server.addReplica(tuples,force)

  def removeFile(self,lfn):
    res = self.__checkArgumentFormat(lfn)
    if not res['OK']:
      return res
    lfns = res['Value'].keys()
    server = RPCClient(self.server,timeout=120)
    successful = {}
    failed = {}
    listOfLists = breakListIntoChunks(lfns,100)
    for list in listOfLists:
      res = server.removeFile(list)
      if not res['OK']:
        return res
      successful.update(res['Value']['Successful'])
      failed.update(res['Value']['Failed'])
    resDict = {'Successful': successful, 'Failed':failed}
    return S_OK(resDict) 

  def removeReplica(self,lfn):
    res = self.__checkArgumentFormat(lfn)
    if not res['OK']:
      return res
    tuples = []
    for lfn,info in res['Value'].items():
      tuples.append((lfn,info['PFN'],info['SE']))
    server = RPCClient(self.server,timeout=120)
    successful = {}
    failed = {}
    listOfLists = breakListIntoChunks(tuples,100)
    for list in listOfLists:
      res = server.removeReplica(list)
      if not res['OK']:
        return res
      successful.update(res['Value']['Successful'])
      failed.update(res['Value']['Failed'])
    resDict = {'Successful': successful, 'Failed':failed}
    return S_OK(resDict)

  def getReplicaStatus(self,lfn):
    res = self.__checkArgumentFormat(lfn)
    if not res['OK']:
      return res
    tuples = []
    for lfn,info in res['Value'].items():
      tuples.append((lfn,info['SE']))
    server = RPCClient(self.server,timeout=120)
    return server.getReplicaStatus(tuples)

  def setReplicaStatus(self,lfn):
    res = self.__checkArgumentFormat(lfn)
    if not res['OK']:
      return res
    tuples = []
    for lfn,info in res['Value'].items():
      tuples.append((lfn,info['PFN'],info['SE'],info['Status']))
    server = RPCClient(self.server,timeout=120)
    return server.setReplicaStatus(tuples)

  def setReplicaHost(self,lfn):
    res = self.__checkArgumentFormat(lfn)
    if not res['OK']:
      return res
    tuples = []
    for lfn,info in res['Value'].items():
      tuples.append((lfn,info['PFN'],info['SE'],info['NewSE']))
    server = RPCClient(self.server,timeout=120)
    return server.setReplicaHost(tuples)

  def removeDirectory(self,lfn):
    return self.__returnOK(lfn)

  def createDirectory(self,lfn):
    return self.__returnOK(lfn)

  def createLink(self,lfn):
    return self.__returnOK(lfn)

  def removeLink(self,lfn):
    return self.__returnOK(lfn)

  def __returnOK(self,lfn):
    res = self.__checkArgumentFormat(lfn)
    if not res['OK']:
      return res
    successful = {}
    for lfn in res['Value'].keys():
      successful[lfn] = True     
    resDict = {'Successful':successful,'Failed':{}}
    return S_OK(resDict)

  def __checkArgumentFormat(self,path):
    if type(path) in types.StringTypes:
      urls = {path:False}
    elif type(path) == types.ListType:
      urls = {}
      for url in path:
        urls[url] = False
    elif type(path) == types.DictType:
     urls = path
    else:
      return S_ERROR("TransformationDBClient.__checkArgumentFormat: Supplied path is not of the correct format.")
    return S_OK(urls)
