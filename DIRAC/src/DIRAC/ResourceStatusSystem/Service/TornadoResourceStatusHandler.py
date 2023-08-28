""" Tornado-based HTTPs ResourceStatus service.
"""

from DIRAC.Core.Tornado.Server.TornadoService import TornadoService
from DIRAC.ResourceStatusSystem.Service.ResourceStatusHandler import ResourceStatusHandlerMixin


class TornadoResourceStatusHandler(ResourceStatusHandlerMixin, TornadoService):
    pass
