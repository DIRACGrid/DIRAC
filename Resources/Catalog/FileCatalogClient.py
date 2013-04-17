########################################################################
# $HeadURL$
########################################################################
""" The FileCatalogClient is a class representing the client of the DIRAC File Catalog  """ 

__RCSID__ = "$Id$"

import re, time, random, os, types

from DIRAC                              import S_OK, S_ERROR, gLogger,gConfig
from DIRAC.Core.Base.Client             import Client

class FileCatalogClient(Client):

  def __init__( self, url=None, **kwargs ):
    """ Constructor function.
    """
    Client.__init__( self, **kwargs )
    self.setServer('DataManagement/FileCatalog')
    if url:
      self.setServer(url)
    self.available = False
#    res = self.isOK()
#    if res['OK']:
#      self.available = res['Value']

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

  def findFilesByMetadata(self,metaDict,path='/',rpc='',url='',timeout=120):
    rpcClient = self._getRPC(rpc=rpc,url=url,timeout=timeout)
    result = rpcClient.findFilesByMetadata(metaDict,path)
    if not result['OK']:
      return result
    if type(result['Value']) == types.ListType:
      return result
    elif type(result['Value']) == types.DictType:
      # Process into the lfn list
      fileList = []
      for dir,fList in result['Value'].items():
        for f in fList:
          fileList.append(dir+'/'+f)
      result['Value'] = fileList    
      return result
    else:
      return S_ERROR( 'Illegal return value type %s' % type( result['Value'] ) ) 
       
  def getFileUserMetadata(self, path, rpc='', url='', timeout=120):
    """Get the meta data attached to a file, but also to 
    the its corresponding directory
    """
    directory = "/".join(path.split("/")[:-1])
    rpcClient = self._getRPC(rpc=rpc,url=url,timeout=timeout)
    result = rpcClient.getFileUserMetadata(path)
    if not result['OK']:
      return result
    fmeta = result['Value']
    result = rpcClient.getDirectoryMetadata(directory)
    if not result['OK']:
      return result
    fmeta.update(result['Value'])
    
    return S_OK(fmeta)
    
    
  
  
  
