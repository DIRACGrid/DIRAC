########################################################################
# $HeadURL$
########################################################################
""" The FileCatalogClient is a class representing the client of the DIRAC File Catalog  """ 

__RCSID__ = "$Id$"

import re, time, random, os, types

from DIRAC                              import S_OK, S_ERROR, gLogger,gConfig
from DIRAC.Core.Base.Client             import Client

class FileCatalogClient(Client):

  def __init__(self,url=None,timeout=0):
    """ Constructor function.
    """
    self.setServer('DataManagement/FileCatalog')
    if url:
      self.setServer(url)
    self.setTimeout(timeout)
    self.available = False
    res = self.isOK()
    if res['OK']:
      self.available = res['Value']

  def isOK(self,rpc='',url='',timeout=120):
    if not self.available:
      rpcClient = self._getRPC(rpc=rpc,url=url,timeout=timeout)
      res = rpcClient.isOK()
      if not res['OK']:
        self.available = False
      else:
        self.available = True
    return S_OK(self.available)
    
  def getReplicas(self, lfns, allStatus=False,rpc='',url='',timeout=120):
    rpcClient = self._getRPC(rpc=rpc,url=url,timeout=timeout)
    return rpcClient.getReplicas(lfns,allStatus)

  def listDirectory(self, lfn, verbose=False, rpc='',url='',timeout=120):
    rpcClient = self._getRPC(rpc=rpc,url=url,timeout=timeout)
    return rpcClient.listDirectory(lfn,verbose)

  def removeDirectory(self, lfn, recursive=False, rpc='',url='',timeout=120):
    rpcClient = self._getRPC(rpc=rpc,url=url,timeout=timeout)
    return rpcClient.removeDirectory(lfn)

  def getDirectoryReplicas(self,lfns,allStatus=False,rpc='',url='',timeout=120):
    rpcClient = self._getRPC(rpc=rpc,url=url,timeout=timeout)
    return rpcClient.getDirectoryReplicas(lfns,allStatus)
