""" The POOL XML Slice class provides a simple plugin module to create
    an XML file for applications to translate LFNs to TURLs. The input
    dictionary has LFNs as keys with all associated metadata as key,
    value pairs.
"""
import os

from DIRAC import S_ERROR, S_OK, gLogger
from DIRAC.Resources.Catalog.PoolXMLCatalog import PoolXMLCatalog


class PoolXMLSlice:
    def __init__(self, catalogName, jobID_path: os.PathLike):
        """Standard constructor"""
        self.fileName = catalogName
        self.jobID_path = jobID_path
        self.log = gLogger.getSubLogger(self.__class__.__name__)

    def execute(self, dataDict):
        """Given a dictionary of resolved input data, this will creates a POOL XML slice."""
        try:
            poolXMLCat = PoolXMLCatalog()
            self.log.verbose("Creating POOL XML slice")

            for lfn, mdataList in dataDict.items():
                # lfn,pfn,se,guid tuple taken by POOL XML Catalogue
                if not isinstance(mdataList, list):
                    mdataList = [mdataList]
                # As a file may have several replicas, set first the file, then the replicas
                poolXMLCat.addFile((lfn, None, None, mdataList[0]["guid"], None))
                for mdata in mdataList:
                    path = ""
                    if "path" in mdata:
                        path = mdata["path"]
                    elif os.path.exists(os.path.basename(mdata["pfn"])):
                        path = os.path.abspath(os.path.basename(mdata["pfn"]))
                    else:
                        path = mdata["turl"]
                    poolXMLCat.addReplica((lfn, path, mdata["se"], False))

            xmlSlice = poolXMLCat.toXML()
            self.log.verbose("POOL XML Slice is: ")
            self.log.verbose(xmlSlice)
            with open(self.jobID_path / self.fileName, "w") as poolSlice:
                poolSlice.write(xmlSlice)
            self.log.info(f"POOL XML Catalogue slice written to {self.jobID_path / self.fileName}")
            try:
                # Temporary solution to the problem of storing the SE in the Pool XML slice
                with open(self.jobID_path / (self.fileName + ".temp"), "w") as poolSlice_temp:
                    xmlSlice = poolXMLCat.toXML(True)
                    poolSlice_temp.write(xmlSlice)
            except Exception:
                self.log.warn(f"Attempted to write catalog also to {self.fileName}.temp but this failed")
                self.log.exception()
        except Exception:
            self.log.exception()
            return S_ERROR("Exception during construction of POOL XML slice")

        return S_OK("POOL XML Slice created")
