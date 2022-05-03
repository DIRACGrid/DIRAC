""" Tornado-based HTTPs JobMonitoring service.

.. literalinclude:: ../ConfigTemplate.cfg
  :start-after: ##BEGIN TornadoJobMonitoring
  :end-before: ##END
  :dedent: 2
  :caption: JobMonitoring options

"""
from DIRAC.Core.Tornado.Server.TornadoService import TornadoService
from DIRAC.WorkloadManagementSystem.Service.JobMonitoringHandler import JobMonitoringHandlerMixin


class TornadoJobMonitoringHandler(JobMonitoringHandlerMixin, TornadoService):
    pass
