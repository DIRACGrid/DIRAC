""" IdProvider based on OAuth2 protocol
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from DIRAC import S_OK
from DIRAC.Resources.IdProvider.OAuth2IdProvider import OAuth2IdProvider

__RCSID__ = "$Id$"


class CheckInIdProvider(OAuth2IdProvider):

  def researchGroup(self, payload=None, token=None):
    """ Research group

        :param str payload: token payload
        :param str token: access token

        :return: S_OK(dict)/S_ERROR()
    """
    if token:
      self.token = {'access_token': token}

    result = self.getUserProfile()
    if not result['OK']:
      return result
    payload = result['Value']

    credDict = self.parseBasic(payload)
    if not credDict.get('DIRACGroups'):
      credDict.update(self.parseEduperson(payload))
    credDict['group'] = credDict.get('DIRACGroups', [None])[0]
    return S_OK(credDict)
