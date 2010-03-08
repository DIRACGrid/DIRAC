""" Class that contains client access to the transformation DB handler. """
########################################################################
# $Id$
# $HeadURL$
########################################################################
__RCSID__ = "$Id$"

from DIRAC                                          import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Base.Client                         import Client
from DIRAC.Core.DISET.RPCClient                     import RPCClient
from DIRAC.Core.Utilities.List                      import breakListIntoChunks
from DIRAC.Resources.Catalog.FileCatalogueBase      import FileCatalogueBase
import types
    
class TransformationDBClient(Client,FileCatalogueBase):
  
  """ Exposes the functionality available in the DIRAC/TransformationHandler

      This inherits the DIRAC base Client for direct execution of server functionality.
      The following methods are available (although not visible here).
      
      Transformation (table) manipulation

          deleteTransformation(transName)
          cleanTransformation(transName)
          getTransformationParameters(transName,paramNames)
          getTransformationWithStatus(status)
          setTransformationParameter(transName,paramName,paramValue)
          deleteTransformationParameter(transName,paramName)

      TransformationFiles table manipulation
      
          addFilesToTransformation(transName,lfns)
          addTaskForTransformation(transName,lfns=[],se='Unknown')
          setFileStatusForTransformation(transName,status,lfns)
          setFileUsedSEForTransformation(transName,usedSE,lfns)  
          getTransformationStats(transName)
          
      TransformationTasks table manipulation 
          
          setTaskStatus(transName, taskID, status) 
          setTaskStatusAndWmsID(transName, taskID, status, taskWmsID) 
          getTransformationTaskStats(transName) 
          deleteTasks(transName, taskMin, taskMax) 
          extendTransformation( transName, nTasks) 
          getTasksToSubmit(transName,numTasks,site='') 
          
      TransformationLogging table manipulation
          
          getTransformationLogging(transName) 
      
      File/directory manipulation methods (the remainder of the interface can be found below)
      
          getFileSummary(lfns,transName)
          exists(lfns) 
          
      Web monitoring tools    
          
          getDistinctAttributeValues(attribute, selectDict) 
          getTransformationStatusCounters() 
          getTransformationSummary() 
          getTransformationSummaryWeb(selectDict, sortList, startItem, maxItems) 
  """

  def __init__(self,name='TransformationDBClient'):
    self.setServer('Transformation/TransformationManager')

  def setServer(self,url):
    self.serverURL = url

  def getCounters(self, table, attrList, condDict, older=None, newer=None, timeStamp=None,rpc='',url='',timeout=120):
    rpcClient = self._getRPC(rpc=rpc,url=url,timeout=timeout)
    return rpcClient. getCounters(table,attrList,condDict,older,newer,timeStamp)   

  def addTransformation(self, transName,description,longDescription,type,plugin,agentType,fileMask,
                            transformationGroup = 'General',
                            groupSize           = 1,
                            inheritedFrom       = 0,
                            body                = '', 
                            maxTasks            = 0,
                            eventsPerTask        = 0,
                            addFiles            = True,
                            rpc='',url='',timeout=120):
    rpcClient = self._getRPC(rpc=rpc,url=url,timeout=timeout)
    return rpcClient.addTransformation(transName,description,longDescription,type,plugin,agentType,fileMask,transformationGroup,groupSize,inheritedFrom,body,maxTasks,eventsPerTask,addFiles)    

  def getTransformations(self,condDict={},older=None, newer=None, timeStamp='CreationDate', orderAttribute=None, limit=None, extraParams=False,rpc='',url='',timeout=120):
    rpcClient = self._getRPC(rpc=rpc,url=url,timeout=timeout)
    return rpcClient.getTransformations(condDict,older,newer,timeStamp,orderAttribute,limit,extraParams)

  def getTransformation(self,transName,extraParams=False,rpc='',url='',timeout=120):
    rpcClient = self._getRPC(rpc=rpc,url=url,timeout=timeout)
    return rpcClient.getTransformation(transName,extraParams)

  def getTransformationFiles(self,condDict={},older=None, newer=None, timeStamp='LastUpdate', orderAttribute=None, limit=None, rpc='',url='',timeout=120):
    rpcClient = self._getRPC(rpc=rpc,url=url,timeout=timeout) 
    return rpcClient.getTransformationFiles(condDict,older,newer,timeStamp,orderAttribute,limit)

  def getTransformationTasks(self,condDict={},older=None, newer=None, timeStamp='CreationTime', orderAttribute=None, limit=None, inputVector=False,rpc='',url='',timeout=120):
    rpcClient = self._getRPC(rpc=rpc,url=url,timeout=timeout) 
    return rpcClient.getTransformationTasks(condDict,older, newer, timeStamp, orderAttribute, limit, inputVector)

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

  def addDirectory(self,path,force=False,rpc='',url='',timeout=120):
    rpcClient = self._getRPC(rpc=rpc,url=url,timeout=timeout)
    return rpcClient.addDirectory(path,force)

  def getReplicas(self,lfn,rpc='',url='',timeout=120):
    res = self.__checkArgumentFormat(lfn)
    if not res['OK']:
      return res
    lfns = res['Value'].keys()
    rpcClient = self._getRPC(rpc=rpc,url=url,timeout=timeout)
    return rpcClient.getReplicas(lfns)

  def addFile(self,lfn,force=False,rpc='',url='',timeout=120):
    res = self.__checkArgumentFormat(lfn)
    if not res['OK']:
      return res
    tuples = []
    for lfn,info in res['Value'].items():
      tuples.append((lfn,info['PFN'],info['Size'],info['SE'],info['GUID'],info['Checksum']))
    rpcClient = self._getRPC(rpc=rpc,url=url,timeout=timeout)
    return rpcClient.addFile(tuples,force)

  def addReplica(self,lfn,force=False,rpc='',url='',timeout=120):
    res = self.__checkArgumentFormat(lfn)
    if not res['OK']:
      return res
    tuples = []
    for lfn,info in res['Value'].items():
      tuples.append((lfn,info['PFN'],info['SE'],False))
    rpcClient = self._getRPC(rpc=rpc,url=url,timeout=timeout)
    return rpcClient.addReplica(tuples,force)

  def removeFile(self,lfn,rpc='',url='',timeout=120):
    res = self.__checkArgumentFormat(lfn)
    if not res['OK']:
      return res
    lfns = res['Value'].keys()
    rpcClient = self._getRPC(rpc=rpc,url=url,timeout=timeout)
    successful = {}
    failed = {}
    listOfLists = breakListIntoChunks(lfns,100)
    for list in listOfLists:
      res = rpcClient.removeFile(list)
      if not res['OK']:
        return res
      successful.update(res['Value']['Successful'])
      failed.update(res['Value']['Failed'])
    resDict = {'Successful': successful, 'Failed':failed}
    return S_OK(resDict) 

  def removeReplica(self,lfn,rpc='',url='',timeout=120):
    res = self.__checkArgumentFormat(lfn)
    if not res['OK']:
      return res
    tuples = []
    for lfn,info in res['Value'].items():
      tuples.append((lfn,info['PFN'],info['SE']))
    rpcClient = self._getRPC(rpc=rpc,url=url,timeout=timeout)
    successful = {}
    failed = {}
    listOfLists = breakListIntoChunks(tuples,100)
    for list in listOfLists:
      res = rpcClient.removeReplica(list)
      if not res['OK']:
        return res
      successful.update(res['Value']['Successful'])
      failed.update(res['Value']['Failed'])
    resDict = {'Successful': successful, 'Failed':failed}
    return S_OK(resDict)

  def getReplicaStatus(self,lfn,rpc='',url='',timeout=120):
    res = self.__checkArgumentFormat(lfn)
    if not res['OK']:
      return res
    tuples = []
    for lfn,info in res['Value'].items():
      tuples.append((lfn,info['SE']))
    rpcClient = self._getRPC(rpc=rpc,url=url,timeout=timeout)
    return rpcClient.getReplicaStatus(tuples)

  def setReplicaStatus(self,lfn,rpc='',url='',timeout=120):
    res = self.__checkArgumentFormat(lfn)
    if not res['OK']:
      return res
    tuples = []
    for lfn,info in res['Value'].items():
      tuples.append((lfn,info['PFN'],info['SE'],info['Status']))
    rpcClient = self._getRPC(rpc=rpc,url=url,timeout=timeout)
    return rpcClient.setReplicaStatus(tuples)

  def setReplicaHost(self,lfn,rpc='',url='',timeout=120):
    res = self.__checkArgumentFormat(lfn)
    if not res['OK']:
      return res
    tuples = []
    for lfn,info in res['Value'].items():
      tuples.append((lfn,info['PFN'],info['SE'],info['NewSE']))
    rpcClient = self._getRPC(rpc=rpc,url=url,timeout=timeout)
    return rpcClient.setReplicaHost(tuples)

  def removeDirectory(self,lfn,rpc='',url='',timeout=120):
    return self.__returnOK(lfn)

  def createDirectory(self,lfn,rpc='',url='',timeout=120):
    return self.__returnOK(lfn)

  def createLink(self,lfn,rpc='',url='',timeout=120):
    return self.__returnOK(lfn)

  def removeLink(self,lfn,rpc='',url='',timeout=120):
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
