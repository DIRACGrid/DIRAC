""" Tornado-based HTTPs ResourceStatus service.
"""
from DIRAC import gLogger
from DIRAC.Core.Tornado.Server.TornadoService import TornadoService
from DIRAC.ResourceStatusSystem.Service.ResourceStatusHandler import ResourceStatusHandlerMixin


sLog = gLogger.getSubLogger(__name__)


class TornadoResourceStatusHandler(ResourceStatusHandlerMixin, TornadoService):
    log = sLog
