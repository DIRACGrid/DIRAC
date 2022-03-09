"""
Service handler for DataIntegrity using https

.. literalinclude:: ../ConfigTemplate.cfg
  :start-after: ##BEGIN TornadoDataIntegrity
  :end-before: ##END
  :dedent: 2
  :caption: TornadoDataIntegrity options

"""

from DIRAC import gLogger
from DIRAC.Core.Tornado.Server.TornadoService import TornadoService
from DIRAC.DataManagementSystem.Service.DataIntegrityHandler import DataIntegrityHandlerMixin

sLog = gLogger.getSubLogger(__name__)


class TornadoDataIntegrityHandler(DataIntegrityHandlerMixin, TornadoService):
    """Tornado handler for the DataIntegrityHandler"""

    log = sLog
