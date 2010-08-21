from DIRAC.Core.Base.Client                     import Client
class MetadataCatalogClient(Client):

  def __init__(self):
    self.setServer('DataManagement/FileCatalog')
