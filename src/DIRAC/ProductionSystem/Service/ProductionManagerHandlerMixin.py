""" TornadoProductionManager is the implementation of the StorageManager service in HTTPS

    .. literalinclude:: ../ConfigTemplate.cfg
      :start-after: ##BEGIN TornadoProductionManager
      :end-before: ##END
      :dedent: 2
      :caption: TornadoProductionManager options
"""
from DIRAC.Core.Tornado.Server.TornadoService import TornadoService
from DIRAC.ProductionSystem.Service.ProductionManagerHandler import ProductionManagerHandlerMixin


class TornadoProductionManagerHandler(ProductionManagerHandlerMixin, TornadoService):
    pass
