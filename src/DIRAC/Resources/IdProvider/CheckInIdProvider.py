""" IdProvider based on OAuth2 protocol
"""
from authlib.oauth2.rfc6749.util import scope_to_list

from DIRAC.Resources.IdProvider.OAuth2IdProvider import OAuth2IdProvider
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOForGroup, getGroupOption


class CheckInIdProvider(OAuth2IdProvider):
    def getGroupScopes(self, group):
        """Get group scopes

        :param str group: DIRAC group

        :return: list
        """
        idPScope = getGroupOption(group, "IdPRole")
        if not idPScope:
            vo = getVOForGroup(group)
            if not vo:
                return []

            groupElements = group.split("_")
            if len(groupElements) < 2:
                return []

            idPScope = f"eduperson_entitlement?value=urn:mace:egi.eu:group:{vo}:role={groupElements[1]}#aai.egi.eu"
        return scope_to_list(idPScope)

    def fetchToken(self, **kwargs):
        """Fetch token

        :param kwargs:
        :return: dict
        """

        if "audience" in kwargs:
            kwargs["resource"] = kwargs["audience"]
            kwargs.pop("audience")

        return super().fetchToken(**kwargs)
