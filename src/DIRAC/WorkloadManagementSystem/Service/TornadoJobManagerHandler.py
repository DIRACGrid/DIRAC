""" Tornado-based HTTPs JobManager service.

.. literalinclude:: ../ConfigTemplate.cfg
  :start-after: ##BEGIN TornadoJobManager
  :end-before: ##END
  :dedent: 2
  :caption: JobManager options

"""
from DIRAC.Core.Tornado.Server.TornadoService import TornadoService
from DIRAC.WorkloadManagementSystem.Service.JobManagerHandler import JobManagerHandlerMixin


class TornadoJobManagerHandler(JobManagerHandlerMixin, TornadoService):
    def initializeRequest(self):
        return JobManagerHandlerMixin.initializeRequest(self)
