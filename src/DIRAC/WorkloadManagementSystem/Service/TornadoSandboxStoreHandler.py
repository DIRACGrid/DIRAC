""" Tornado-based HTTPs SandboxStore service.

.. literalinclude:: ../ConfigTemplate.cfg
  :start-after: ##BEGIN TornadoSandboxStore
  :end-before: ##END
  :dedent: 2
  :caption: SandboxStore options

"""
from DIRAC.Core.Tornado.Server.TornadoService import TornadoService
from DIRAC.WorkloadManagementSystem.Service.SandboxStoreHandler import SandboxStoreHandlerMixin


class TornadoSandboxStoreHandler(SandboxStoreHandlerMixin, TornadoService):
    def initializeRequest(self):
        # Ugly, but makes DIPS and HTTPS handlers compatible, TBD properly
        self.serviceInfoDict = self._serviceInfoDict
        return SandboxStoreHandlerMixin.initializeRequest(self)

    def export_streamFromClient(self, fileId, token, fileSize, data):
        return self._getFromClient(fileId, token, fileSize, data=data)

    def export_streamToClient(self, fileId, token=""):
        return self._sendToClient(fileId, token, raw=True)
