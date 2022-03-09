""" Tornado-based HTTPs WMSAdministrator service.
"""
from DIRAC import gLogger
from DIRAC.Core.Tornado.Server.TornadoService import TornadoService
from DIRAC.WorkloadManagementSystem.Service.WMSAdministratorHandler import WMSAdministratorHandlerMixin


sLog = gLogger.getSubLogger(__name__)


class TornadoWMSAdministratorHandler(WMSAdministratorHandlerMixin, TornadoService):
    log = sLog
