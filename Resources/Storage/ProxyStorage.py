""" This is the Proxy storage element client """

__RCSID__ = "$Id$"

from DIRAC                                              import gLogger, S_OK, S_ERROR
from DIRAC.Resources.Utilities.Utils                    import checkArgumentFormat
from DIRAC.Resources.Storage.StorageBase                import StorageBase
from DIRAC.ConfigurationSystem.Client                   import PathFinder
from DIRAC.Core.DISET.RPCClient                         import RPCClient
from DIRAC.Core.DISET.TransferClient                    import TransferClient
from DIRAC.Core.Utilities.File                          import getSize
from DIRAC.Core.Utilities.Pfn                           import pfnunparse

import os

class ProxyStorage( StorageBase ):

  def __init__( self, storageName, protocol, path, host, port, spaceToken, wspath ):
    self.isok = True

    self.protocolName = 'Proxy'
    self.name = storageName
    self.protocol = protocol
    self.path = path
    self.host = host
    self.port = port
    self.wspath = wspath
    self.spaceToken = spaceToken
    self.cwd = self.path
    apply( StorageBase.__init__, ( self, self.name, self.path ) )
    self.url = PathFinder.getServiceURL( "DataManagement/StorageElementProxy" )
    if not self.url:
      self.isok = False

  ######################################
  # URL manipulation functionalities
  ######################################

  def getParameters( self ):
    """ This gets all the storage specific parameters pass when instantiating the storage
    """
    parameterDict = {}
    parameterDict['StorageName'] = self.name
    parameterDict['ProtocolName'] = self.protocolName
    parameterDict['Protocol'] = self.protocol
    parameterDict['Host'] = self.host
    parameterDict['Path'] = self.path
    parameterDict['Port'] = self.port
    parameterDict['SpaceToken'] = self.spaceToken
    parameterDict['WSUrl'] = self.wspath
    return S_OK( parameterDict )

  def getProtocolPfn( self, pfnDict, withPort ):
    """ From the pfn dict construct the SURL to be used
    """
    return pfnunparse( pfnDict )

  ######################################
  # File transfer functionalities
  ######################################

  def getFile( self, path, localPath = False ):
    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']
    failed = {}
    successful = {}
    client = RPCClient( self.url )
    transferClient = TransferClient( self.url )
    for src_url in urls.keys():
      res = client.prepareFile( self.name, src_url )
      if not res['OK']:
        gLogger.error( "ProxyStorage.getFile: Failed to prepare file on remote server.", res['Message'] )
        failed[src_url] = res['Message']
      else:
        fileName = os.path.basename( src_url )
        if localPath:
          dest_file = "%s/%s" % ( localPath, fileName )
        else:
          dest_file = "%s/%s" % ( os.getcwd(), fileName )
        res = transferClient.receiveFile( dest_file, 'getFile/%s' % fileName )
        if not res['OK']:
          gLogger.error( "ProxyStorage.getFile: Failed to recieve file from proxy server.", res['Message'] )
          failed[src_url] = res['Message']
        elif not os.path.exists( dest_file ):
          errStr = "ProxyStorage.getFile: The destination local file does not exist."
          gLogger.error( errStr, dest_file )
          failed[src_url] = errStr
        else:
          destSize = getSize( dest_file )
          if destSize == -1:
            errStr = "ProxyStorage.getFile: Failed to get the local file size."
            gLogger.error( errStr, dest_file )
            failed[src_url] = errStr
          else:
            successful[src_url] = destSize
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  def putFile( self, path, sourceSize = 0 ):

    client = RPCClient( self.url )

    if sourceSize:
      gLogger.debug( "ProxyStorage.putFile: The client has provided the source file size implying a replication is requested." )
      return client.callProxyMethod( self.name, 'putFile', [path], {'sourceSize':sourceSize} )

    gLogger.debug( "ProxyStorage.putFile: No source size was provided therefore a simple put will be performed." )
    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']
    failed = {}
    successful = {}
    client = RPCClient( self.url )
    transferClient = TransferClient( self.url )
    for dest_url, src_file in urls.items():
      fileName = os.path.basename( dest_url )
      res = transferClient.sendFile( src_file, 'putFile/%s' % fileName )
      if not res['OK']:
        gLogger.error( "ProxyStorage.putFile: Failed to send file to proxy server.", res['Message'] )
        failed[dest_url] = res['Message']
      else:
        res = client.uploadFile( self.name, dest_url )
        if not res['OK']:
          gLogger.error( "ProxyStorage.putFile: Failed to upload file to storage element from proxy server.", res['Message'] )
          failed[dest_url] = res['Message']
        else:
          res = self.__executeOperation( dest_url, 'getFileSize' )
          if not res['OK']:
            gLogger.error( "ProxyStorage.putFile: Failed to determine destination file size.", res['Message'] )
            failed[dest_url] = res['Message']
          else:
            successful[dest_url] = res['Value']
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  ######################################
  # File manipulation functionalities
  ######################################

  def exists( self, path ):
    client = RPCClient( self.url )
    return client.callProxyMethod( self.name, 'exists', [path], {} )

  def isFile( self, path ):
    client = RPCClient( self.url )
    return client.callProxyMethod( self.name, 'isFile', [path], {} )

  def getFileSize( self, path ):
    client = RPCClient( self.url )
    return client.callProxyMethod( self.name, 'getFileSize', [path], {} )

  def getFileMetadata( self, path ):
    client = RPCClient( self.url )
    return client.callProxyMethod( self.name, 'getFileMetadata', [path], {} )

  def getTransportURL( self, path, protocols = False ):
    client = RPCClient( self.url )
    return client.callProxyMethod( self.name, 'getTransportURL', [path], {'protocols':protocols} )

  def removeFile( self, path ):
    client = RPCClient( self.url )
    return client.callProxyMethod( self.name, 'removeFile', [path], {} )

  def prestageFile( self, path ):
    client = RPCClient( self.url )
    return client.callProxyMethod( self.name, 'prestageFile', [path], {} )

  def prestageFileStatus( self, path ):
    client = RPCClient( self.url )
    return client.callProxyMethod( self.name, 'prestageFileStatus', [path], {} )

  def pinFile( self, path, lifetime = 60 * 60 * 24 ):
    client = RPCClient( self.url )
    return client.callProxyMethod( self.name, 'pinFile', [path], {'lifetime':lifetime} )

  def releaseFile( self, path ):
    client = RPCClient( self.url )
    return client.callProxyMethod( self.name, 'releaseFile', [path], {} )

  ######################################
  # Directory manipulation functionalities
  ######################################

  def isDirectory( self, path ):
    client = RPCClient( self.url )
    return client.callProxyMethod( self.name, 'isDirectory', [path], {} )

  def getDirectoryMetadata( self, path ):
    client = RPCClient( self.url )
    return client.callProxyMethod( self.name, 'getDirectoryMetadata', [path], {} )

  def getDirectorySize( self, path ):
    client = RPCClient( self.url )
    return client.callProxyMethod( self.name, 'getDirectorySize', [path], {} )

  def listDirectory( self, path ):
    client = RPCClient( self.url )
    return client.callProxyMethod( self.name, 'listDirectory', [path], {} )

  def createDirectory( self, path ):
    client = RPCClient( self.url )
    return client.callProxyMethod( self.name, 'createDirectory', [path], {} )

  def removeDirectory( self, path, recursive = False ):
    client = RPCClient( self.url )
    return client.callProxyMethod( self.name, 'removeDirectory', [path], {'recursive':recursive} )

  def getPFNBase( self ):
    client = RPCClient( self.url )
    return client.callProxyMethod( self.name, 'getPFNBase', [], {} )

  def getDirectory( self, path ):
    return S_ERROR( "Not supported" )

  def putDirectory( self, path ):
    return S_ERROR( "Not supported" )

  def __executeOperation( self, url, method ):
    """ Executes the requested functionality with the supplied url
    """
    fcn = None
    if hasattr( self, method ) and callable( getattr( self, method ) ):
      fcn = getattr( self, method )
    if not fcn:
      return S_ERROR( "Unable to invoke %s, it isn't a member function of ProxyStorage" % method )
    res = fcn( [url] )
    if not res['OK']:
      return res
    elif url not in res['Value']['Successful']:
      return S_ERROR( res['Value']['Failed'][url] )
    return S_OK( res['Value']['Successful'][url] )

