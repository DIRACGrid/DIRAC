""" Module that contains simple client access to Bundler service
"""

from DIRAC.Core.Base.Client import Client, createClient


@createClient("WorkloadManagement/Bundler")
class BundlerClient(Client):
    """Exposes the functionality available in the WorkloadManagement/BundlerHandler

    This inherits the DIRAC base Client for direct execution of server functionality.
    The following methods are available (although not visible here).
    """

    def __init__(self, url=None, **kwargs):
        super().__init__(**kwargs)

        if not url:
            self.serverURL = "WorkloadManagement/Bundler"

        else:
            self.serverURL = url
