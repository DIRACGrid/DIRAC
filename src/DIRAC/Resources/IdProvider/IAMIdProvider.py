""" IdProvider based on OAuth2 protocol
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from DIRAC.Resources.IdProvider.OAuth2IdProvider import OAuth2IdProvider

__RCSID__ = "$Id$"


class IAMIdProvider(OAuth2IdProvider):

  def researchGroup(self, payload, token):
    """ Research group
    """
    pass
