""" Handler for CAs + CRLs bundles
"""
from base64 import b64encode

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Security import Utilities
from DIRAC.Core.Tornado.Server.TornadoService import TornadoService
from DIRAC.FrameworkSystem.Service.BundleDeliveryHandler import BundleDeliveryHandlerMixin


class TornadoBundleDeliveryHandler(BundleDeliveryHandlerMixin, TornadoService):

    types_streamToClient = []

    def export_streamToClient(self, fileId):
        version = ""
        if isinstance(fileId, str):
            if fileId in ["CAs", "CRLs"]:
                retVal = Utilities.generateCAFile() if fileId == "CAs" else Utilities.generateRevokedCertsFile()
                if not retVal["OK"]:
                    return retVal
                with open(retVal["Value"]) as fd:
                    return S_OK(b64encode(fd.read()).decode())
            bId = fileId

        elif isinstance(fileId, (list, tuple)):
            if len(fileId) == 0:
                return S_ERROR("No bundle specified!")
            bId = fileId[0]
            if len(fileId) != 1:
                version = fileId[1]

        if not self.bundleManager.bundleExists(bId):
            return S_ERROR("Unknown bundle %s" % bId)

        bundleVersion = self.bundleManager.getBundleVersion(bId)
        if bundleVersion is None:
            return S_ERROR("Empty bundle %s" % bId)

        if version == bundleVersion:
            return S_OK(bundleVersion)

        return S_OK(b64encode(self.bundleManager.getBundleData(bId)).decode())
