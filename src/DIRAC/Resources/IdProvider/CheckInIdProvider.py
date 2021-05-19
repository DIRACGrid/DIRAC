""" IdProvider based on OAuth2 protocol
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from DIRAC.Resources.IdProvider.OAuth2IdProvider import OAuth2IdProvider

__RCSID__ = "$Id$"


class CheckInIdProvider(OAuth2IdProvider):

  # urn:mace:egi.eu:group:registry:training.egi.eu:role=member#aai.egi.eu'
  NAMESPACE = 'urn:mace:egi.eu:group:registry'
  SIGN = '#aai.egi.eu'
  PARAM_SCOPE = 'eduperson_entitlement?value='

  def researchGroup(self, payload, token=None):
    """ Research group
    """
    if token:
      self.token = token
    claims = self.getUserProfile()
    credDict = self.parseBasic(claims)
    credDict.update(self.parseEduperson(claims))
    cerdDict = self.userDiscover(credDict)
    credDict['provider'] = self.name

    return credDict
