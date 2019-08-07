""" DIRAC FileCatalog Storage Element Manager mix-in class for SE definitions from CS"""

__RCSID__ = "$Id$"

from DIRAC import S_OK, gConfig
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.SEManager.SEManagerBase import SEManagerBase


class SEManagerCS(SEManagerBase):

  def findSE(self, se):
    return S_OK(se)

  def addSE(self, se):
    return S_OK(se)

  def getSEDefinition(self, se):
    # TODO Think about using a cache for this information
    return gConfig.getOptionsDict('/Resources/StorageElements/%s/AccessProtocol.1' % se)
