from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

from dominate import document, tags as dom
from authlib.oauth2.rfc8414 import AuthorizationServerMetadata

from DIRAC.ConfigurationSystem.Client.Utilities import getAuthorizationServerMetadata


def collectMetadata(issuer=None, ignoreErrors=False):
  """ Collect metadata for DIRAC Authorization Server(DAS), a metadata format defines by IETF specification:
      https://datatracker.ietf.org/doc/html/rfc8414#section-2

      :param str issuer: issuer to set

      :return: dict -- dictionary is the AuthorizationServerMetadata object in the same time
  """
  result = getAuthorizationServerMetadata(issuer, ignoreErrors=ignoreErrors)
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


def getHTML(title, style=None):
  """ Provide HTML object

      :param str title: browser tab title
      :param str style: css as string

      :return: HTML object
  """
  html = document("DIRAC - %s" % title)
  with html.head:
    dom.script(src="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.15.4/js/all.min.js")
    # Enable bootstrap 5
    dom.link(rel='stylesheet', integrity="sha384-EVSTQN3/azprG1Anm3QDgpJLIm9Nao0Yz1ztcQTwFspd3yD65VohhpuuCOmLASjC",
             href="https://cdn.jsdelivr.net/npm/bootstrap@5.0.2/dist/css/bootstrap.min.css", crossorigin="anonymous")
    dom.script(src='https://cdn.jsdelivr.net/npm/bootstrap@5.0.2/dist/js/bootstrap.bundle.min.js',
               integrity="sha384-MrcW6ZMFYlzcLA8Nl+NtUVF0sA7MsXsP1UyJoMp4YLEuNSfAP+JcXn/tWtIaxVXM",
               crossorigin="anonymous")
    # Provide additional css
    if style:
      dom.style(style)
  return html
