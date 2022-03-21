""" Tornado-based HTTPs WMSAdministrator service.

.. literalinclude:: ../ConfigTemplate.cfg
  :start-after: ##BEGIN TornadoWMSAdministrator
  :end-before: ##END
  :dedent: 2
  :caption: WMSAdministrator options

"""
from DIRAC import gLogger
from DIRAC.Core.Tornado.Server.TornadoService import TornadoService
from DIRAC.WorkloadManagementSystem.Service.WMSAdministratorHandler import WMSAdministratorHandlerMixin


sLog = gLogger.getSubLogger(__name__)


class TornadoWMSAdministratorHandler(WMSAdministratorHandlerMixin, TornadoService):
    log = sLog
