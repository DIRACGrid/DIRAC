""" TornadoProxyManager is the implementation of the ProxyManagement service in HTTPS

    .. literalinclude:: ../ConfigTemplate.cfg
      :start-after: ##BEGIN TornadoProxyManager:
      :end-before: ##END
      :dedent: 2
      :caption: TornadoProxyManager options
"""
from DIRAC import gLogger
from DIRAC.Core.Tornado.Server.TornadoService import TornadoService
from DIRAC.FrameworkSystem.Service.ProxyManagerHandler import ProxyManagerHandlerMixin


sLog = gLogger.getSubLogger(__name__)


class TornadoProxyManagerHandler(ProxyManagerHandlerMixin, TornadoService):
    log = sLog
