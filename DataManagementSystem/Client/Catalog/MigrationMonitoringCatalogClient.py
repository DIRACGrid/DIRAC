""" Client plug-in for the Migration Monitoring DB.
"""
__RCSID__ = "$Id: MigrationMonitoringCatalogClient.py,v 1.5 2009/11/02 14:29:25 acsmith Exp $"

import DIRAC
from DIRAC                                                         import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.Core.DISET.RPCClient                                    import RPCClient
from DIRAC.ConfigurationSystem.Client                              import PathFinder
from DIRAC.DataManagementSystem.Client.Catalog.FileCatalogueBase   import FileCatalogueBase
import types

class MigrationMonitoringCatalogClient(FileCatalogueBase):

  def __init__(self):
    try:
      self.url = PathFinder.getServiceURL('StorageManagement/MigrationMonitor')
      self.valid = True
    except Exception,x:
      errStr = "MigrationMonitoringClient.__init__: Exception while generating server url."
      gLogger.exception(errStr,lException=x)
      self.valid = False

  def isOK(self):
    return self.valid

  def exists(self,lfn):
    """ LFN may be a string or list of strings
    """
    res = self.__checkArgumentFormat(lfn)
    if not res['OK']:
      return res
    lfns = res['Value']
    successful = {}
    failed = {}
    for lfn in lfns.keys():
      successful[lfn] = False
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def addFile(self, lfn):
    res = self.__checkArgumentFormat(lfn)
    if not res['OK']:
      return res
    failed = {}
    successful = {}
    fileTuples = []
    fileInfo = res['Value']
    for lfn,info in fileInfo.items():
      pfn = str(info['PFN'])
      size = int(info['Size'])
      se = str(info['SE'])
      guid = str(info['GUID'])
      checksum = str(info['Checksum'])
      fileTuples.append((lfn,pfn,size,se,guid,checksum))
    server = RPCClient(self.url,timeout=120)
    res = server.addMigratingReplicas(fileTuples)
    if not res['OK']:
      for lfn in fileInfo.keys():
        failed[lfn] = res['Message']
    else:
      for lfn in fileInfo.keys():
        successful[lfn] = True
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def removeFile(self,path):
    res = self.__checkArgumentFormat(path)
    if not res['OK']:
      return res
    lfns = res['Value'].keys()
    failed = {}
    successful = {}
    server = RPCClient(self.url,timeout=120)
    res = server.removeMigratingFiles(lfns)
    if not res['OK']:
      for lfn in lfns:
        failed[lfn] = res['Message']
    else:
      for lfn in lfns:
        successful[lfn] = True
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def addReplica(self,lfn):
    res = self.__checkArgumentFormat(lfn)
    if not res['OK']:
      return res
    replicaTuples = []
    fileInfo = res['Value']
    for lfn, info in fileInfo.items():
      pfn = str(info['PFN'])
      se = str(info['SE'])
      replicaTuples.append((lfn,pfn,0,se,'',''))
    server = RPCClient(self.url,timeout=120)
    failed = {}
    successful = {}
    res = server.addMigratingReplicas(replicaTuples)
    if not res['OK']:
      for lfn in fileInfo.keys():
        failed[lfn] = res['Message']
    else:
      for lfn in fileInfo.keys():
        successful[lfn] = True
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def removeReplica(self,lfn):
    res = self.__checkArgumentFormat(lfn)
    if not res['OK']:
      return res
    replicaTuples = []
    fileInfo = res['Value']
    for lfn, info in fileInfo.items():
      pfn = str(info['PFN'])
      se = str(info['SE'])
      replicaTuples.append((lfn,pfn,se))
    server = RPCClient(self.url,timeout=120)
    return server.removeMigratingReplicas(replicaTuples)

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
      return S_ERROR("MigrationMonitoringClient.__checkArgumentFormat: Supplied path is not of the correct format.")
    return S_OK(urls)

  ##################################################################
  #
  # These are dummy methods that are not used but should be there to complete the file catalog interface
  #

  def setReplicaStatus(self,lfn):
    return self.__dummyMethod(lfn)

  def setReplicaHost(self,lfn):
    return self.__dummyMethod(lfn)

  def removeDirectory(self,lfn):
    return self.__dummyMethod(lfn)

  def createDirectory(self,lfn):
    return self.__dummyMethod(lfn)

  def removeLink(self,lfn):
    return self.__dummyMethod(lfn)

  def createLink(self,lfn):
    return self.__dummyMethod(lfn)

  def __dummyMethod(self,lfn):
    res = self.__checkArgumentFormat(lfn)
    if not res['OK']:
      return res
    successful = {}
    for lfn, info in res['Value'].items():
      successful[lfn] = True
    resDict = {'Failed':{},'Successful':successful}
    return S_OK(resDict)
