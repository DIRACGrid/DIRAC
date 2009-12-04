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

          addTransformation(transName,description,longDescription,type,plugin,agentType,fileMask,
                            transformationGroup = 'General',
                            groupSize           = 1,
                            inheritedFrom       = 0,
                            body                = '', 
                            maxJobs             = 0,
                            eventsPerJob        = 0,
                            addFiles            = True)    
          deleteTransformation(transName)
          cleanTransformation(transName)
          addTransformationParameter(transName,paramName,paramValue)
          setTransformationStatus(transName,status)
          setTransformationAgentType(transName,status)
          getTransformations(condDict={},older=None, newer=None, timeStamp='CreationDate', orderAttribute=None, limit=None, extraParams=False)
          getTransformationWithStatus(status)
          getTransformation(transName)
          getTransformationLastUpdate(transName)

      T_* table manipulation
      
          addTaskForTransformation(transName,lfns=[],se='Unknown')
          setFileStatusForTransformation(transName,status,lfns) 
          getTransformationLFNs(transName,status='Unused') 
          getTransformationStats(transName)
          
      Jobs table manipulation 
          
          setTaskStatus(transName, taskID, status) 
          setTaskStatusAndWmsID(transName, taskID, status, taskWmsID) 
          selectWMSTasks(transName,statusList=[],newer=0) 
          getTransformationTaskStats(transName) 
          getTaskInfo(transName, taskID) 
          getTaskStats(transName) 
          deleteTasks(transName, taskMin, taskMax) 
          extendTransformation( transName, nTasks) 
          getTasksToSubmit(transName,numTasks,site='') 
          
      TransformationLogging table manipulation
          
          getTransformationLogging(transName) 
      
      File/directory manipulation methods (the remainder of the interface can be found below)
      
          getFileSummary(lfns,transName)
          addDirectory(path,force=False) 
          exists(lfns) 
          
      Web monitoring tools    
          
          getDistinctAttributeValues(attribute, selectDict) 
          getTransformationStatusCounters() 
          getTransformationSummary() 
          getTransformationSummaryWeb(selectDict, sortList, startItem, maxItems) 
      
  """

  def setServer(self,url):
    self.serverURL = url

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
