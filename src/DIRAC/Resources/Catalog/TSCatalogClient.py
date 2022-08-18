""" TSCatalogClient class represents the Transformation Manager service
    as a DIRAC Catalog service
"""
from DIRAC import S_OK
from DIRAC.Core.Utilities.List import breakListIntoChunks
from DIRAC.Resources.Catalog.Utilities import checkCatalogArguments
from DIRAC.Resources.Catalog.FileCatalogClientBase import FileCatalogClientBase


class TSCatalogClient(FileCatalogClientBase):

    """Exposes the catalog functionality available in the DIRAC/TransformationHandler"""

    # List of common File Catalog methods implemented by this client
    WRITE_METHODS = FileCatalogClientBase.WRITE_METHODS + ["addFile", "removeFile", "setMetadata"]

    NO_LFN_METHODS = ["setMetadata"]

    def __init__(self, url=None, **kwargs):

        self.serverURL = "Transformation/TransformationManager" if not url else url
        super().__init__(self.serverURL, **kwargs)

    @checkCatalogArguments
    def addFile(self, lfns, force=False):
        rpcClient = self._getRPC()
        return rpcClient.addFile(lfns, force)

    @checkCatalogArguments
    def removeFile(self, lfns):
        rpcClient = self._getRPC()
        successful = {}
        failed = {}
        listOfLists = breakListIntoChunks(lfns, 100)
        for fList in listOfLists:
            res = rpcClient.removeFile(fList)
            if not res["OK"]:
                return res
            successful.update(res["Value"]["Successful"])
            failed.update(res["Value"]["Failed"])
        resDict = {"Successful": successful, "Failed": failed}
        return S_OK(resDict)

    def setMetadata(self, path, metadatadict):
        """Set metadata parameter for the given path

        :return: Successful/Failed dict.
        """
        rpcClient = self._getRPC()
        return rpcClient.setMetadata(path, metadatadict)
