from DIRAC.Core.Tornado.Server.TornadoService import TornadoService
from DIRAC.DataManagementSystem.Service.FTS3ManagerHandler import FTS3ManagerHandlerMixin


class TornadoFTS3ManagerHandler(FTS3ManagerHandlerMixin, TornadoService):
    """Tornado handler for the FTS3Manager"""

    pass
