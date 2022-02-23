from DIRAC import gLogger
from DIRAC.Core.Tornado.Server.TornadoService import TornadoService
from DIRAC.RequestManagementSystem.Service.ReqManagerHandler import ReqManagerHandlerMixin

sLog = gLogger.getSubLogger(__name__)


class TornadoReqManagerHandler(ReqManagerHandlerMixin, TornadoService):
    """Tornado handler for the ReqManager"""

    log = sLog
