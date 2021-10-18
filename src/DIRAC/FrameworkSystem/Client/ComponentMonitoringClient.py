"""Class for making requests to a ComponentMonitoring Service."""

__RCSID__ = "$Id$"

from DIRAC.Core.Base.Client import Client, createClient


@createClient("Framework/ComponentMonitoring")
class ComponentMonitoringClient(Client):
    def __init__(self, **kwargs):
        """
        Constructor function
        """

        super(ComponentMonitoringClient, self).__init__(**kwargs)
        self.setServer("Framework/ComponentMonitoring")
