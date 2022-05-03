""" Tornado-based HTTPs TransformationManager service.
"""
from DIRAC.Core.Tornado.Server.TornadoService import TornadoService
from DIRAC.TransformationSystem.Service.TransformationManagerHandler import TransformationManagerHandlerMixin


class TornadoTransformationManagerHandler(TransformationManagerHandlerMixin, TornadoService):
    pass
