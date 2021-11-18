""" Tornado-based HTTPs JobMonitoring service.
"""
from DIRAC import gLogger
from DIRAC.Core.Tornado.Server.TornadoService import TornadoService
from DIRAC.WorkloadManagementSystem.Service.JobMonitoringHandler import JobMonitoringHandlerMixin


sLog = gLogger.getSubLogger(__name__)


class TornadoJobMonitoringHandler(JobMonitoringHandlerMixin, TornadoService):
    log = sLog
