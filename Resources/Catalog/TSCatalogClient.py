""" TSCatalogClient class represents the Transformation Manager service
    as a DIRAC Catalog service
"""

__RCSID__ = "$Id$"

from DIRAC                                                  import S_OK
from DIRAC.Core.Utilities.List                              import breakListIntoChunks
from DIRAC.DataManagementSystem.Utilities.CatalogUtilities  import checkCatalogArguments
from DIRAC.Resources.Catalog.FileCatalogClientBase          import FileCatalogClientBase

# List of common File Catalog methods implemented by this client
READ_METHODS = []
WRITE_METHODS = [ "addFile", "removeFile" ]
NO_LFN_METHODS= []

class TSCatalogClient( FileCatalogClientBase ):

  """ Exposes the catalog functionality available in the DIRAC/TransformationHandler

  """
  def __init__( self, url = None, **kwargs ):

    self.__kwargs = kwargs
    self.valid = True
    self.serverURL = "Transformation/TransformationManager"
    if url is not None:
      self.serverURL = url

  def hasCatalogMethod( self, methodName ):
    """ Check of a method with the given name is implemented
    :param str methodName: the name of the method to check
    :return: boolean Flag
    """
    return methodName in ( READ_METHODS + WRITE_METHODS + NO_LFN_METHODS )

  def getInterfaceMethods( self ):
    """ Get the methods implemented by the File Catalog client

    :return tuple: ( read_methods_list, write_methods_list, nolfn_methods_list )
    """
    return ( READ_METHODS, WRITE_METHODS, NO_LFN_METHODS )

  @checkCatalogArguments
  def addFile( self, lfns, force = False ):
    rpcClient = self._getRPC()
    return rpcClient.addFile( lfns, force )

  @checkCatalogArguments
  def removeFile( self, lfns ):
    rpcClient = self._getRPC()
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

