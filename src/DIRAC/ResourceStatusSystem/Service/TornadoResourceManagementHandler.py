""" Tornado-based HTTPs ResourceManagement service.
"""
from DIRAC import gLogger
from DIRAC.Core.Tornado.Server.TornadoService import TornadoService
from DIRAC.ResourceStatusSystem.Service.ResourceManagementHandler import ResourceManagementHandlerMixin


sLog = gLogger.getSubLogger(__name__)


class TornadoResourceManagementHandler(ResourceManagementHandlerMixin, TornadoService):
    log = sLog
