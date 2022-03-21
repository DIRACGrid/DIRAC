""" Tornado-based HTTPs Monitoring service.

.. literalinclude:: ../ConfigTemplate.cfg
  :start-after: ##BEGIN TornadoMonitoring
  :end-before: ##END
  :dedent: 2
  :caption: Monitoring options

"""
from DIRAC import gLogger
from DIRAC.Core.Tornado.Server.TornadoService import TornadoService
from DIRAC.MonitoringSystem.Service.MonitoringHandler import MonitoringHandlerMixin


sLog = gLogger.getSubLogger(__name__)


class TornadoMonitoringHandler(MonitoringHandlerMixin, TornadoService):
    log = sLog
