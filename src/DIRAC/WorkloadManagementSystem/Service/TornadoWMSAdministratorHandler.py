""" Tornado-based HTTPs WMSAdministrator service.

.. literalinclude:: ../ConfigTemplate.cfg
  :start-after: ##BEGIN TornadoWMSAdministrator
  :end-before: ##END
  :dedent: 2
  :caption: WMSAdministrator options

"""
from DIRAC.Core.Tornado.Server.TornadoService import TornadoService
from DIRAC.WorkloadManagementSystem.Service.WMSAdministratorHandler import WMSAdministratorHandlerMixin


class TornadoWMSAdministratorHandler(WMSAdministratorHandlerMixin, TornadoService):
    pass
