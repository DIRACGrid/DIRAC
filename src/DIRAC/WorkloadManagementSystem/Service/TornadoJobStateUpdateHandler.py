""" Tornado-based HTTPs JobStateUpdate service.
"""
from DIRAC import gLogger
from DIRAC.Core.Tornado.Server.TornadoService import TornadoService
from DIRAC.WorkloadManagementSystem.Service.JobStateUpdateHandler import JobStateUpdateHandlerMixin


sLog = gLogger.getSubLogger(__name__)


class TornadoJobStateUpdateHandler(JobStateUpdateHandlerMixin, TornadoService):
    log = sLog
