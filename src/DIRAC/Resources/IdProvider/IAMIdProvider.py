""" IdProvider based on OAuth2 protocol
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from authlib.oauth2.rfc6749.util import scope_to_list

from DIRAC import S_OK
from DIRAC.Resources.IdProvider.OAuth2IdProvider import OAuth2IdProvider
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOForGroup, getGroupOption

__RCSID__ = "$Id$"


class IAMIdProvider(OAuth2IdProvider):
    def getGroupScopes(self, group):
        """Get group scopes

        :param str group: DIRAC group

        :return: list
        """
        idPScope = getGroupOption(group, "IdPRole")
        if not idPScope:
            idPScope = "wlcg.groups:/%s/%s" % (getVOForGroup(group), group.split("_")[1])
        return S_OK(scope_to_list(idPScope))
