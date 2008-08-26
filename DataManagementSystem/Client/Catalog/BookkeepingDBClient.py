# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/DataManagementSystem/Client/Catalog/BookkeepingDBClient.py,v 1.12 2008/08/26 18:25:02 atsareg Exp $

""" Client for BookkeepingDB file catalog
"""

__RCSID__ = "$Id: BookkeepingDBClient.py,v 1.12 2008/08/26 18:25:02 atsareg Exp $"

from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.DataManagementSystem.Client.Catalog.FileCatalogueBase import FileCatalogueBase
import types, os

class BookkeepingDBClient(FileCatalogueBase):
  """ File catalog client for bookkeeping DB
  """
  def __init__(self, url=False):
    """ Constructor of the Bookkeeping catalogue client
    """
    self.name = 'BookkeepingDB'
    self.valid = True
    try:
      if not url:
        self.server = RPCClient("Bookkeeping/BookkeepingManager")
      else:
        self.server = RPCClient(url)
    except Exception,x:
      print x
      self.valid = False

  def isOK(self):
    return self.valid

  def __setHasReplicaFlag(self,lfns):

    failed = {}
    successful = {}
    res = self.server.addFiles(lfns)
    if not res['OK']:
      for lfn in lfns:
        failed[lfn] = res['Message']
        resDict = {'Successful':{},'Failed':failed}
      return S_OK(resDict)
    else:
      for lfn in lfns:
        if res['Value'].has_key(lfn):
          failed[lfn] = res['Value'][lfn]
        else:
          successful[lfn] = True
      resDict = {'Successful':successful,'Failed':failed}
      result = S_OK(resDict)
      result['rpcStub'] = res['rpcStub']
      return result

  def __unsetHasReplicaFlag(self,lfns):

    failed = {}
    successful = {}
    res = self.server.removeFiles(lfns)
    if not res['OK']:
      for lfn in lfns:
        failed[lfn] = res['Message']
        resDict = {'Successful':{},'Failed':failed}
      return S_OK(resDict)
    else:
      for lfn in lfns:
        if res['Value'].has_key(lfn):
          failed[lfn] = res['Value'][lfn]
        else:
          successful[lfn] = True
      resDict = {'Successful':successful,'Failed':failed}
      return S_OK(resDict)

  def __exists(self,lfns):

    failed = {}
    successful = {}
    res = self.server.exists(lfns)
    if not res['OK']:
      for lfn in lfns:
        failed[lfn] = res['Message']
        resDict = {'Successful':{},'Failed':failed}
      return S_OK(resDict)
    else:
      for lfn,exists in res['Value'].items():
        successful[lfn] = exists
      resDict = {'Successful':successful,'Failed':{}}
      return S_OK(resDict)

  def __getFileMetadata(lfns):
    res = self.server.getFileMetadata(lfns)
    successful = {}
    failed = {}
    if not res['OK']:
      for lfn in lfns:
        failed[lfn] = res['Message']
        resDict = {'Successful':{},'Failed':failed}
      return S_OK(resDict)
    else:
      for lfn,result in res['Value'].items():
        if result in types.StringTypes:
          failed[lfn] = result
        else:
          successful[lfn] = result
      resDict = {'Successful':successful,'Failed':failed}
      return S_OK(resDict)

  def addFile(self,fileTuple):
    """ A tuple should be supplied to this method which contains:
        (lfn,pfn,size,se,guid,checksum)
        A list of tuples may also be supplied.
    """
    successful = {}
    failed = {}
    if type(fileTuple) == types.TupleType:
      files = [fileTuple]
    elif type(fileTuple) == types.ListType:
      files = fileTuple
    else:
      return S_ERROR('BookkeepingDBClient.addFile: Must supply a file tuple of list of tuples')
    fileList = []
    for lfn,pfn,size,se,guid,checksum in files:
      fileList.append(lfn)
    return self.__setHasReplicaFlag(fileList)

  def addReplica(self,replicaTuple):
    """ This adds a replica to the catalogue
        The tuple to be supplied is of the following form:
          (lfn,pfn,se,master)
        where master = True or False
    """
    if type(replicaTuple) == types.TupleType:
      replicas = [replicaTuple]
    elif type(replicaTuple) == types.ListType:
      replicas = replicaTuple
    else:
      return S_ERROR('BookkeepingDBClient.addReplica: Must supply a replica tuple of list of tuples')
    fileList = []
    for lfn,pfn,se,master in replicas:
      fileList.append(lfn)
    return self.__setHasReplicaFlag(fileList)

  def removeFile(self,path):
    if type(path) == types.StringType:
      lfns = [path]
    elif type(path) == types.ListType:
       lfns = path
    else:
      return S_ERROR('BookkeepingDBClient.removeFile: Must supply a path or list of paths')
    res = self.__exists(lfns)
    if not res['OK']:
      return res
    lfnsToRemove = []
    successful = {}
    for lfn,exists in res['Value']['Successful'].items():
      if exists:
        lfnsToRemove.append(lfn)
      else:
        successful[lfn] = True
    res = self.__unsetHasReplicaFlag(lfnsToRemove)
    failed = res['Value']['Failed']
    successful.update(res['Value']['Successful'])
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def removeReplica(self,replicaTuple):
    if type(replicaTuple) == types.TupleType:
      replicas = [replicaTuple]
    elif type(replicaTuple) == types.ListType:
      replicas = replicaTuple
    else:
      return S_ERROR('BookkeepingDBClient.setReplicaStatus: Must supply a file tuple or list of file typles')
    successful = {}
    for lfn,pfn,se in replicas:
      successful[lfn] = True
    resDict = {'Failed':{},'Successful':successful}
    return S_OK(resDict)

  def exists(self,path):
    if type(path) == types.StringType:
      lfns = [path]
    elif type(path) == types.ListType:
       lfns = path
    else:
      return S_ERROR('BookkeepingDBClient.exists: Must supply a path or list of paths')
    return self.__exists(lfns)

  def getFileMetadata(self,path):
    if type(path) == types.StringType:
      lfns = [path]
    elif type(path) == types.ListType:
       lfns = path
    else:
      return S_ERROR('BookkeepingDBClient.exists: Must supply a path or list of paths')
    return self.__getFileMetadata(lfns)


