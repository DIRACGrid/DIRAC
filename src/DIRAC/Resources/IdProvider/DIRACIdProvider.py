""" IdProvider based on OAuth2 protocol
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from DIRAC.Resources.IdProvider.OAuth2IdProvider import OAuth2IdProvider
from DIRAC.FrameworkSystem.private.authorization.AuthServer import collectMetadata

__RCSID__ = "$Id$"


class DIRACIdProvider(OAuth2IdProvider):

  def fetch_metadata(self, url=None):
    """ Fetch metada
    """
    self.metadata.update(collectMetadata(self.metadata['issuer']))
    if url:
      return self.get(url, withhold_token=True).json()
    
