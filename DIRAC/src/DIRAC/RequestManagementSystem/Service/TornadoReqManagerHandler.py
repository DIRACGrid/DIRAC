""" Tornado handler for the ReqManager
"""

from DIRAC.Core.Tornado.Server.TornadoService import TornadoService
from DIRAC.RequestManagementSystem.Service.ReqManagerHandler import ReqManagerHandlerMixin


class TornadoReqManagerHandler(ReqManagerHandlerMixin, TornadoService):
    """Tornado handler for the ReqManager"""
