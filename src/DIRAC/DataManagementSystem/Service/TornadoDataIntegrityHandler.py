"""
Service handler for DataIntegrity using https

.. literalinclude:: ../ConfigTemplate.cfg
  :start-after: ##BEGIN TornadoDataIntegrity
  :end-before: ##END
  :dedent: 2
  :caption: TornadoDataIntegrity options

"""

from DIRAC.Core.Tornado.Server.TornadoService import TornadoService
from DIRAC.DataManagementSystem.Service.DataIntegrityHandler import DataIntegrityHandlerMixin


class TornadoDataIntegrityHandler(DataIntegrityHandlerMixin, TornadoService):
    """Tornado handler for the DataIntegrityHandler"""

    pass
