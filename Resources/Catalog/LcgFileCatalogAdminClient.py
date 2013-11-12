""" This is the admin interface to the LFC. It exposes the functionality not available through the standard 
LcgFileCatalogCombinedClient """

from DIRAC.Resources.Catalog.LcgFileCatalogCombinedClient       import LcgFileCatalogCombinedClient


class LcgFileCatalogAdminClient(LcgFileCatalogCombinedClient):

  LcgFileCatalogCombinedClient.ro_methods.extend(['getUserDirectory'])
  LcgFileCatalogCombinedClient.write_methods.extend(['createUserDirectory', 'changeDirectoryOwner',
                                                     'createUserMapping', 'removeUserDirectory'])
