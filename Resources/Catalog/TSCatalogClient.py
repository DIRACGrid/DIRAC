""" TSCatalogClient class represents the Transformation Manager service
    as a DIRAC Catalog service
"""

__RCSID__ = "$Id$"

from DIRAC                                                         import S_OK
from DIRAC.Core.Base.Client                                        import Client
from DIRAC.Core.Utilities.List                                     import breakListIntoChunks
from DIRAC.DataManagementSystem.Utilities.CatalogUtilities         import checkCatalogArguments

# List of common File Catalog methods implemented by this client
CATALOG_METHODS = [ "addFile", "removeFile" ]

class TSCatalogClient( Client ):

  """ Exposes the functionality available in the DIRAC/TransformationHandler

  """

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

