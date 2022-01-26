""" Tornado-based HTTPs Publisher service.
"""
from DIRAC import gLogger
from DIRAC.Core.Tornado.Server.TornadoService import TornadoService
from DIRAC.ResourceStatusSystem.Service.PublisherHandler import PublisherHandlerMixin


sLog = gLogger.getSubLogger(__name__)


class TornadoPublisherHandler(PublisherHandlerMixin, TornadoService):
    log = sLog
