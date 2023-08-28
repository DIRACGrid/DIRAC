"""
Service handler for FTS3DB using Tornado

.. literalinclude:: ../ConfigTemplate.cfg
  :start-after: ##BEGIN TornadoFTS3Manager
  :end-before: ##END
  :dedent: 2
  :caption: TornadoFTS3Manager options

"""

from DIRAC.Core.Tornado.Server.TornadoService import TornadoService
from DIRAC.DataManagementSystem.Service.FTS3ManagerHandler import FTS3ManagerHandlerMixin


class TornadoFTS3ManagerHandler(FTS3ManagerHandlerMixin, TornadoService):
    """Tornado handler for the FTS3Manager"""
