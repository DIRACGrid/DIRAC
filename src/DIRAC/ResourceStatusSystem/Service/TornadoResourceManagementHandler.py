""" Tornado-based HTTPs ResourceManagement service.
"""
from DIRAC.Core.Tornado.Server.TornadoService import TornadoService
from DIRAC.ResourceStatusSystem.Service.ResourceManagementHandler import ResourceManagementHandlerMixin


class TornadoResourceManagementHandler(ResourceManagementHandlerMixin, TornadoService):
    pass
