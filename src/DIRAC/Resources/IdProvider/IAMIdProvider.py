""" IdProvider based on OAuth2 protocol
"""
from authlib.oauth2.rfc6749.util import scope_to_list

from DIRAC import S_OK
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
            idPScope = "wlcg.groups:/{}/{}".format(getVOForGroup(group), group.split("_")[1])
        return S_OK(scope_to_list(idPScope))
