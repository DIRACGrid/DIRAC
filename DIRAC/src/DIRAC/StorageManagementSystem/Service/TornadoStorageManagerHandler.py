""" TornadoStorageManager is the implementation of the StorageManager service in HTTPS

    .. literalinclude:: ../ConfigTemplate.cfg
      :start-after: ##BEGIN TornadoStorageManager
      :end-before: ##END
      :dedent: 2
      :caption: TornadoStorageManager options
"""
from DIRAC.Core.Tornado.Server.TornadoService import TornadoService
from DIRAC.StorageManagementSystem.Service.StorageManagerHandler import StorageManagerHandlerMixin


class TornadoStorageManagerHandler(StorageManagerHandlerMixin, TornadoService):
    pass
