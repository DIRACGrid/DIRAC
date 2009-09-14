""" Base class for the TransformationDBClient for access file catalog tables
"""
from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.DataManagementSystem.Client.Catalog.FileCatalogueBase import FileCatalogueBase
from DIRAC.Core.Utilities.List import breakListIntoChunks
from DIRAC.Core.DISET.RPCClient import RPCClient
import types

class TransformationDBClient(FileCatalogueBase):
  """ Exposing the functionality of the replica tables for the TransformationDB
  """
  def setServer(self,url):
    self.server = url

  #####################################################################
  #
  # These are the transformation management methods
  #
  
  def getAllTransformations(self):
    server = RPCClient(self.server,timeout=120)
    return server.getAllTransformations()

  def getBookkeepingQuery(self,bkQueryID):
    server = RPCClient(self.server,timeout=120)
    return server.getBookkeepingQuery(bkQueryID)

  def addLFNsToTransformation(self,lfns,transID):
    server = RPCClient(self.server,timeout=120)
    return server.addLFNsToTransformation(lfns,transID)

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
