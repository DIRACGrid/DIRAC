""" Tornado-based Matcher service.
"""

from DIRAC.Core.Tornado.Server.TornadoService import TornadoService
from DIRAC.WorkloadManagementSystem.Service.MatcherHandler import MatcherHandlerMixin


class TornadoMatcherHandler(MatcherHandlerMixin, TornadoService):
  pass
