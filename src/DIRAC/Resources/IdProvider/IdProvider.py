""" IdProvider base class for various identity providers
"""
from DIRAC import gLogger


class IdProvider:

    DEFAULT_METADATA = {}

    def __init__(self, **kwargs):
        """C'or"""
        self.log = gLogger.getSubLogger(self.__class__.__name__)
        meta = self.DEFAULT_METADATA
        meta.update(kwargs)
        self.setParameters(meta)
        self._initialization(**meta)

    def _initialization(self, **kwargs):
        """Initialization"""
        pass

    def setParameters(self, parameters):
        """Set parameters

        :param dict parameters: parameters of the identity Provider
        """
        self.parameters = parameters
        self.name = parameters.get("ProviderName")
