""" FileCatalogFactory class to create file catalog client objects according to the
    configuration description
"""
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getCatalogPath
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader


class FileCatalogFactory:
    """Factory of file catalog objects. Only exposes createCatalog() method"""

    def __init__(self):
        self.log = gLogger.getSubLogger(self.__class__.__name__)
        self.catalogPath = ""

    def createCatalog(self, catalogName):
        """Create a file catalog object from its name and CS description"""
        catalogPath = getCatalogPath(catalogName)
        catalogType = gConfig.getValue(catalogPath + "/CatalogType", catalogName)
        catalogURL = gConfig.getValue(catalogPath + "/CatalogURL", "DataManagement/" + catalogType)
        optionsDict = {}
        result = gConfig.getOptionsDict(catalogPath)
        if result["OK"]:
            optionsDict = result["Value"]

        return self.__createCatalog(catalogName, catalogType, catalogURL, optionsDict)

    def __getCatalogClass(self, catalogType):
        result = ObjectLoader().loadObject(f"Resources.Catalog.{catalogType}Client")
        if not result["OK"]:
            gLogger.error("Failed to load catalog object", f"{result['Message']}")
        return result

    def __createCatalog(self, catalogName, catalogType, catalogURL, optionsDict):
        self.log.debug(f"Creating {catalogName} client of type {catalogType}")

        result = self.__getCatalogClass(catalogType)
        if not result["OK"]:
            return result
        catalogClass = result["Value"]

        try:
            optionsDict["url"] = catalogURL
            catalog = catalogClass(**optionsDict)
            self.log.debug(f"Loaded module {catalogType}Client")
            return S_OK(catalog)
        except Exception as x:
            errStr = f"Failed to instantiate {catalogType}()"
            gLogger.exception(errStr, lException=x)
            return S_ERROR(errStr)

        # Catalog module was not loaded
        return S_ERROR(f"No suitable client found for {catalogName}")
