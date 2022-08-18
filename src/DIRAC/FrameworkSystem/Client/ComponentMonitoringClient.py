"""Class for making requests to a ComponentMonitoring Service."""
from DIRAC.Core.Base.Client import Client, createClient


@createClient("Framework/ComponentMonitoring")
class ComponentMonitoringClient(Client):
    def __init__(self, **kwargs):
        """
        Constructor function
        """

        super().__init__(**kwargs)
        self.setServer("Framework/ComponentMonitoring")
