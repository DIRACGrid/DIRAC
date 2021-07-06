from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

from authlib.oauth2.rfc8414 import AuthorizationServerMetadata

from DIRAC.ConfigurationSystem.Client.Utilities import getAuthorizationServerMetadata


def collectMetadata(issuer=None):
  """ Collect metadata for DIRAC Authorization Server(DAS), a metadata format defines by IETF specification:
      https://datatracker.ietf.org/doc/html/rfc8414#section-2

      :param str issuer: issuer to set

      :return: dict -- dictionary is the AuthorizationServerMetadata object in the same time
  """
  result = getAuthorizationServerMetadata(issuer)
  if not result['OK']:
    raise Exception('Cannot prepare authorization server metadata. %s' % result['Message'])
  metadata = result['Value']
  for name, endpoint in [('jwks_uri', 'jwk'),
                         ('token_endpoint', 'token'),
                         ('userinfo_endpoint', 'userinfo'),
                         ('revocation_endpoint', 'revoke'),
                         ('redirect_uri', 'redirect'),
                         ('authorization_endpoint', 'authorization'),
                         ('device_authorization_endpoint', 'device')]:
    metadata[name] = metadata['issuer'].strip('/') + '/' + endpoint
  metadata['scopes_supported'] = ['g:', 'proxy', 'lifetime:']
  metadata['grant_types_supported'] = ['code', 'authorization_code', 'refresh_token',
                                       'urn:ietf:params:oauth:grant-type:device_code']
  metadata['response_types_supported'] = ['code', 'device', 'token']
  metadata['code_challenge_methods_supported'] = ['S256']
  return AuthorizationServerMetadata(metadata)