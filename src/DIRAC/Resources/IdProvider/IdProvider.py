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

    def getGroupScopes(self, group: str) -> list:
        """Get group scopes

        :param group: DIRAC group
        """
        idPScope = getGroupOption(group, "IdPRole")
        return scope_to_list(idPScope) if idPScope else []

    def getScopeGroups(self, scope: str) -> list:
        """Get DIRAC groups related to scope"""
        groups = []
        for group in getAllGroups():
            if (g_scope := self.getGroupScopes(group)) and set(g_scope).issubset(scope_to_list(scope)):
                groups.append(group)
        return groups
