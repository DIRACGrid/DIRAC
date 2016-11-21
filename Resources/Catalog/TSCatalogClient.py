""" TSCatalogClient class represents the Transformation Manager service
    as a DIRAC Catalog service
"""

__RCSID__ = "$Id$"

from DIRAC.Resources.Catalog.Utilities             import checkCatalogArguments
from DIRAC.Resources.Catalog.FileCatalogClientBase import FileCatalogClientBase
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient

class TSCatalogClient( FileCatalogClientBase ):

  """ Exposes the catalog functionality available in the DIRAC/TransformationHandler

  """

  # List of common File Catalog methods implemented by this client
  WRITE_METHODS = FileCatalogClientBase.WRITE_METHODS + [ "addFile", "removeFile", "setMetadata" ]

  NO_LFN_METHODS = [ "setMetadata" ]

  def __init__( self, url = None, **kwargs ):

    self.serverURL = 'Transformation/TransformationManager' if not url else url
    super( TSCatalogClient, self ).__init__( self.serverURL, **kwargs )

  @checkCatalogArguments
  def addFile( self, lfns, force = False ):
    """ Add file to the catalog
    """
    transClient = TransformationClient()
    res = transClient.addFile( lfns, force )
    return res

  @checkCatalogArguments
  def removeFile( self, lfns ):
    transClient = TransformationClient()
    res = transClient.removeFile( lfns )
    return res

  @checkCatalogArguments
  def setMetadata( self, path, metadatadict ):
    """ Set metadata parameter for the given path
        :return Successful/Failed dict.
    """
    transClient = TransformationClient()
    res = transClient.setMetadata( path, metadatadict )
    return res
