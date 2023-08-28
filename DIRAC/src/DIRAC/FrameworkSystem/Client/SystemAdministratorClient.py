""" The SystemAdministratorClient is a class representing the client of the DIRAC
    SystemAdministrator service. It has also methods to update the Configuration
    Service with the DIRAC components options
"""
from DIRAC.Core.Base.Client import Client, createClient

SYSADMIN_PORT = 9162


@createClient("Framework/SystemAdministrator")
class SystemAdministratorClient(Client):
    def __init__(self, host, port=None, **kwargs):
        """Constructor function. Takes a mandatory host parameter"""
        super().__init__(**kwargs)
        if not port:
            port = SYSADMIN_PORT
        self.setServer(f"dips://{host}:{port}/Framework/SystemAdministrator")
