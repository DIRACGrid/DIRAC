""" TornadoComponentMonitoring is the implementation of the ComponentMonitoringent service in HTTPS

    .. literalinclude:: ../ConfigTemplate.cfg
      :start-after: ##BEGIN TornadoComponentMonitoring:
      :end-before: ##END
      :dedent: 2
      :caption: TornadoComponentMonitoring options
"""
from DIRAC.Core.Tornado.Server.TornadoService import TornadoService
from DIRAC.FrameworkSystem.Service.ComponentMonitoringHandler import ComponentMonitoringHandlerMixin


class TornadoComponentMonitoringHandler(ComponentMonitoringHandlerMixin, TornadoService):
    pass
