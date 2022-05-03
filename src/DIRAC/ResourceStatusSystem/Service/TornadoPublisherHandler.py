""" Tornado-based HTTPs Publisher service.
"""
from DIRAC.Core.Tornado.Server.TornadoService import TornadoService
from DIRAC.ResourceStatusSystem.Service.PublisherHandler import PublisherHandlerMixin


class TornadoPublisherHandler(PublisherHandlerMixin, TornadoService):
    pass
