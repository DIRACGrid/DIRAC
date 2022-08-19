"""HTTPS module based on the GFAL2_StorageBase class."""
# from DIRAC
from DIRAC.Resources.Storage.GFAL2_StorageBase import GFAL2_StorageBase
from DIRAC import gLogger


class GFAL2_HTTPSStorage(GFAL2_StorageBase):

    """.. class:: GFAL2_HTTPSStorage

    HTTP interface to StorageElement using gfal2
    """

    # davs is for https with direct access + third party
    _INPUT_PROTOCOLS = ["file", "http", "https", "dav", "davs"]
    _OUTPUT_PROTOCOLS = ["http", "https", "dav", "davs"]

    def __init__(self, storageName, parameters):
        """c'tor"""
        # # init base class
        super().__init__(storageName, parameters)
        self.srmSpecificParse = False

        self.log = gLogger.getSubLogger(self.__class__.__name__)

        self.pluginName = "GFAL2_HTTPS"

        # We don't need extended attributes for metadata
        self._defaultExtendedAttributes = None
