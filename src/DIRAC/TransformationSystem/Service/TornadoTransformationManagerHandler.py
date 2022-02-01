""" Tornado-based HTTPs TransformationManager service.
"""
from DIRAC import gLogger
from DIRAC.Core.Tornado.Server.TornadoService import TornadoService
from DIRAC.TransformationSystem.Service.TransformationManagerHandler import TransformationManagerHandlerMixin


sLog = gLogger.getSubLogger(__name__)


class TornadoTransformationManagerHandler(TransformationManagerHandlerMixin, TornadoService):
    log = sLog
