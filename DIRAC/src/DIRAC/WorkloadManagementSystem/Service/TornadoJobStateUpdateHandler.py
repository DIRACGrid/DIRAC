""" Tornado-based HTTPs JobStateUpdate service.

.. literalinclude:: ../ConfigTemplate.cfg
  :start-after: ##BEGIN TornadoJobStateUpdate
  :end-before: ##END
  :dedent: 2
  :caption: JobStateUpdate options

"""
from DIRAC.Core.Tornado.Server.TornadoService import TornadoService
from DIRAC.WorkloadManagementSystem.Service.JobStateUpdateHandler import JobStateUpdateHandlerMixin


class TornadoJobStateUpdateHandler(JobStateUpdateHandlerMixin, TornadoService):
    pass
