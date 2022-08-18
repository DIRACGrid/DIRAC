"""
   CloudEndpoint is a base class for the clients used to connect to different
   cloud providers
"""
import os

from DIRAC import S_ERROR, S_OK
from DIRAC.Resources.Cloud.Utilities import createUserDataScript, createPilotDataScript, createCloudInitScript


class Endpoint:
    """Endpoint base class"""

    def __init__(self, parameters=None, bootstrapParameters=None):
        # logger
        if not parameters:
            parameters = {}
        self.parameters = parameters
        if not bootstrapParameters:
            bootstrapParameters = {}
        self.bootstrapParameters = bootstrapParameters
        self.valid = False
        self.proxy = None

    def isValid(self):
        return self.valid

    def setParameters(self, parameters):
        self.parameters.update(parameters)

    def setBootstrapParameters(self, bootstrapParameters):
        self.bootstrapParameters.update(bootstrapParameters)

    def getParameterDict(self):
        return self.parameters

    def setProxy(self, proxy):
        self.proxy = proxy

    def getProxyFileLocation(self):
        if not self.proxy:
            self.proxy = self.parameters.get("Proxy", os.environ.get("X509_USER_PROXY"))
            if not self.proxy:
                return S_ERROR("Can not find proxy")
        return S_OK(self.proxy)

    def _createUserDataScript(self):

        bootType = self.bootstrapParameters.get("BootType", "pilot")
        if bootType.lower() == "pilot":
            return createPilotDataScript(self.parameters, self.bootstrapParameters)
        elif bootType.lower() == "user":
            return createUserDataScript(self.parameters)
        elif bootType.lower() == "cloudinit":
            return createCloudInitScript(self.parameters, self.bootstrapParameters)
