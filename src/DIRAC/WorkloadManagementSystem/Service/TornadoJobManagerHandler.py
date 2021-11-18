""" Tornado-based HTTPs JobManager service.
"""
from DIRAC import gLogger
from DIRAC.Core.Tornado.Server.TornadoService import TornadoService
from DIRAC.WorkloadManagementSystem.Service.JobManagerHandler import JobManagerHandlerMixin


sLog = gLogger.getSubLogger(__name__)


class TornadoJobManagerHandler(JobManagerHandlerMixin, TornadoService):
    log = sLog

    def initializeRequest(self):
        self.diracSetup = self.get_argument("clientSetup")
        return JobManagerHandlerMixin.initializeRequest(self)
