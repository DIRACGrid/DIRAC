""" This is the admin interface to the LFC. It exposes the functionality not available through the standard LcgFileCatalogCombinedClient """

from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.DataManagementSystem.Client.Catalog.LcgFileCatalogCombinedClient import LcgFileCatalogCombinedClient
from DIRAC.Core.Utilities.Subprocess import pythonCall
import random, time,os
import lfc

class LcgFileCatalogAdminClient(LcgFileCatalogCombinedClient):

  LcgFileCatalogCombinedClient.ro_methods.extend(['getUserDirectory'])
  LcgFileCatalogCombinedClient.write_methods.extend(['createUserDirectory','changeDirectoryOwner','createUserMapping','removeUserDirectory'])
