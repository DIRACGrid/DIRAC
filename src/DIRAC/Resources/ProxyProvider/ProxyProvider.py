""" ProxyProvider base class for various proxy providers
"""
from DIRAC import S_OK, S_ERROR


class ProxyProvider:
    def __init__(self, parameters=None):

        self.parameters = parameters
        self.name = None
        if parameters:
            self.name = parameters.get("ProviderName")

    def setParameters(self, parameters):
        self.parameters = parameters
        self.name = parameters.get("ProviderName")

    def checkStatus(self, userDN):
        """Read ready to work status of proxy provider

        :param str userDN: user DN

        :return: S_OK()/S_ERROR()
        """
        return S_OK()

    def generateDN(self, **kwargs):
        """Generate new DN

        :param dict kwargs: user description dictionary

        :return: S_OK(str)/S_ERROR() -- contain DN
        """
        return S_ERROR("Not implemented in %s", self.name)
