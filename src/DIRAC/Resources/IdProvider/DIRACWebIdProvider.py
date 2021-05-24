""" IdProvider based on OAuth2 protocol
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from DIRAC.Resources.IdProvider.OAuth2IdProvider import OAuth2IdProvider
from DIRAC.FrameworkSystem.private.authorization.AuthServer import collectMetadata
from DIRAC.FrameworkSystem.private.authorization.utils.Clients import DEFAULT_CLIENTS

__RCSID__ = "$Id$"


class DIRACWebIdProvider(OAuth2IdProvider):

  DEFAULT_METADATA = DEFAULT_CLIENTS['DIRACWeb']

  def fetch_metadata(self):
    """ Fetch metada
    """
    self.metadata.update(collectMetadata(self.metadata['issuer']))
