from DIRAC.Core.Base.Client                     import Client

__RCSID__ = "$Id$"

class MetadataCatalogClient(Client):

  def __init__( self, **kwargs ):
    Client.__init__( self, **kwargs )
    self.setServer('DataManagement/FileCatalog')
