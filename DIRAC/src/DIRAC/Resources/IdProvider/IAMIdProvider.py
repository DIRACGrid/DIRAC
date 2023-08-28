""" IdProvider based on OAuth2 protocol
"""
from authlib.oauth2.rfc6749.util import scope_to_list

from DIRAC.Resources.IdProvider.OAuth2IdProvider import OAuth2IdProvider
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOForGroup, getGroupOption


class IAMIdProvider(OAuth2IdProvider):
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

            idPScope = f"wlcg.groups:/{vo}/{groupElements[1]}"
        return scope_to_list(idPScope)
