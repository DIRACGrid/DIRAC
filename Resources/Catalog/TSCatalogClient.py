""" TSCatalogClient class represents the Transformation Manager service
    as a DIRAC Catalog service
"""

__RCSID__ = "$Id$"

from DIRAC                                                  import S_OK
from DIRAC.Core.Utilities.List                              import breakListIntoChunks
from DIRAC.DataManagementSystem.Utilities.CatalogUtilities  import checkCatalogArguments
from DIRAC.Core.DISET.RPCClient                             import RPCClient

# List of common File Catalog methods implemented by this client
CATALOG_METHODS = [ "addFile", "removeFile" ]

class TSCatalogClient( object ):

  """ Exposes the catalog functionality available in the DIRAC/TransformationHandler

  """
  def __init__( self, url = None, **kwargs ):

    self.__kwargs = kwargs
    self.valid = True
    self.serverURL = "Transformation/TransformationManager"
    if url is not None:
      self.serverURL = url

  def __getRPC( self, rpc = None, url = '', timeout = 600 ):
    """ Return RPCClient object to url
    """
    if not rpc:
      if not url:
        url = self.serverURL
      self.__kwargs.setdefault( 'timeout', timeout )
      rpc = RPCClient( url, **self.__kwargs )
    return rpc

  def isOK( self ):
    return self.valid

  def hasCatalogMethod( self, methodName ):
    """ Check of a method with the given name is implemented
    :param str methodName: the name of the method to check
    :return: boolean Flag
    """
    return methodName in CATALOG_METHODS

  def getInterfaceMethods( self ):
    """ Get the methods implemented by the File Catalog client

    :return tuple: ( read_methods_list, write_methods_list, nolfn_methods_list )
    """
    return ( [], CATALOG_METHODS, [] )

  @checkCatalogArguments
  def addFile( self, lfns, force = False ):
    rpcClient = self.__getRPC()
    return rpcClient.addFile( lfns, force )

  @checkCatalogArguments
  def removeFile( self, lfns ):
    rpcClient = self.__getRPC()
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

