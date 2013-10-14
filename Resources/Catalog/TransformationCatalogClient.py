########################################################################
# $HeadURL$
########################################################################
""" The TransformationCatalogClient is a class representing the client of the DIRAC 
Transformation System Catalog, because YES it's a catalog too! """ 

__RCSID__ = "$Id$"

import types

from DIRAC                              import S_OK, S_ERROR
from DIRAC.Core.Base.Client             import Client
from DIRAC.Core.Utilities.List          import breakListIntoChunks


class TransformationCatalogClient(Client):

  def __init__( self, url=None, **kwargs ):
    """ Constructor function.
    """
    Client.__init__( self, **kwargs )
    self.setServer('Transformation/TransformationManager')
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
  
  def addFile( self, lfn, force = False, rpc = '', url = '', timeout = 120 ):
    """ Needed for the Catalog interface """
    res = self.__checkArgumentFormat( lfn )
    if not res['OK']:
      return res
    tuples = []
    for lfn, info in res['Value'].items():
      tuples.append( ( lfn, info['PFN'], info['Size'], info['SE'], info['GUID'], info['Checksum'] ) )
    rpcClient = self._getRPC( rpc = rpc, url = url, timeout = timeout )
    return rpcClient.addFile( tuples, force )

  def addReplica( self, lfn, force = False, rpc = '', url = '', timeout = 120 ):
    """ Needed for the Catalog interface """
    res = self.__checkArgumentFormat( lfn )
    if not res['OK']:
      return res
    tuples = []
    for lfn, info in res['Value'].items():
      tuples.append( ( lfn, info['PFN'], info['SE'], False ) )
    rpcClient = self._getRPC( rpc = rpc, url = url, timeout = timeout )
    return rpcClient.addReplica( tuples, force )

  def removeFile( self, lfn, rpc = '', url = '', timeout = 120 ):
    """ Needed for the Catalog interface """
    res = self.__checkArgumentFormat( lfn )
    if not res['OK']:
      return res
    lfns = res['Value'].keys()
    rpcClient = self._getRPC( rpc = rpc, url = url, timeout = timeout )
    successful = {}
    failed = {}
    listOfLists = breakListIntoChunks( lfns, 100 )
    for fList in listOfLists:
      res = rpcClient.removeFile( fList )
      if not res['OK']:
        return res
      successful.update( res['Value']['Successful'] )
      failed.update( res['Value']['Failed'] )
    resDict = {'Successful': successful, 'Failed':failed}
    return S_OK( resDict )

  def removeReplica( self, lfn, rpc = '', url = '', timeout = 120 ):
    """ Needed for the Catalog interface """
    res = self.__checkArgumentFormat( lfn )
    if not res['OK']:
      return res
    tuples = []
    for lfn, info in res['Value'].items():
      tuples.append( ( lfn, info['PFN'], info['SE'] ) )
    rpcClient = self._getRPC( rpc = rpc, url = url, timeout = timeout )
    successful = {}
    failed = {}
    listOfLists = breakListIntoChunks( tuples, 100 )
    for fList in listOfLists:
      res = rpcClient.removeReplica( fList )
      if not res['OK']:
        return res
      successful.update( res['Value']['Successful'] )
      failed.update( res['Value']['Failed'] )
    resDict = {'Successful': successful, 'Failed':failed}
    return S_OK( resDict )

  def getReplicaStatus( self, lfn, rpc = '', url = '', timeout = 120 ):
    """ Needed for the Catalog interface """
    res = self.__checkArgumentFormat( lfn )
    if not res['OK']:
      return res
    tuples = []
    for lfn, info in res['Value'].items():
      tuples.append( ( lfn, info['SE'] ) )
    rpcClient = self._getRPC( rpc = rpc, url = url, timeout = timeout )
    return rpcClient.getReplicaStatus( tuples )

  def setReplicaStatus( self, lfn, rpc = '', url = '', timeout = 120 ):
    """ Needed for the Catalog interface """
    res = self.__checkArgumentFormat( lfn )
    if not res['OK']:
      return res
    tuples = []
    for lfn, info in res['Value'].items():
      tuples.append( ( lfn, info['PFN'], info['SE'], info['Status'] ) )
    rpcClient = self._getRPC( rpc = rpc, url = url, timeout = timeout )
    return rpcClient.setReplicaStatus( tuples )

  def setReplicaHost( self, lfn, rpc = '', url = '', timeout = 120 ):
    """ Needed for the Catalog interface """
    res = self.__checkArgumentFormat( lfn )
    if not res['OK']:
      return res
    tuples = []
    for lfn, info in res['Value'].items():
      tuples.append( ( lfn, info['PFN'], info['SE'], info['NewSE'] ) )
    rpcClient = self._getRPC( rpc = rpc, url = url, timeout = timeout )
    return rpcClient.setReplicaHost( tuples )
    
  def removeDirectory( self, lfn, rpc = '', url = '', timeout = 120 ):
    """ Needed for the Catalog interface """
    return self.__returnOK( lfn )

  def createDirectory( self, lfn, rpc = '', url = '', timeout = 120 ):
    """ Needed for the Catalog interface """
    return self.__returnOK( lfn )

  def createLink( self, lfn, rpc = '', url = '', timeout = 120 ):
    """ Needed for the Catalog interface """
    return self.__returnOK( lfn )

  def removeLink( self, lfn, rpc = '', url = '', timeout = 120 ):
    """ Needed for the Catalog interface """
    return self.__returnOK( lfn )
  