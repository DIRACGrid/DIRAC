"""Class for making requests to a ComponentMonitoring Service."""
__RCSID__ = "$Id$"

from six import add_metaclass

from DIRAC.Core.Base.Client import Client, ClientCreator


@add_metaclass(ClientCreator)
class ComponentMonitoringClient(Client):
  handlerModuleName = 'DIRAC.FrameworkSystem.Service.ComponentMonitoringHandler'
  handlerClassName = 'ComponentMonitoringHandler'

  def __init__(self, **kwargs):
    """
    Constructor function
    """

    Client.__init__(self, **kwargs)
    self.setServer('Framework/ComponentMonitoring')
