from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import time
import pytest
import unittest

from diraccfg import CFG

import DIRAC
from DIRAC import gConfig

cfg = CFG()
cfg.loadFromBuffer("""
DIRAC
{
  Security
  {
    Authorization
    {
      issuer = https://issuer.url/
      Clients
      {
        DIRACWeb
        {
          client_id = client_identificator
          client_secret = client_secret_key
          redirect_uri = https://redirect.url/
        }
      }
    }
  }
}
Resources
{
  IdProviders
  {
    SomeIdP
    {
      ProviderType = OAuth2
      issuer = https://idp.url/
      client_id = IdP_client_id
      client_secret = IdP_client_secret
      redirect_uri = https://dirac/redirect
      jwks_uri = https://idp.url/jwk
      scope = openid+profile+offline_access+eduperson_entitlement
    }
  }
}
""")
gConfig.loadCFG(cfg)

if six.PY3:
  # DIRACOS not contain required packages
  from authlib.jose import jwt
  from DIRAC.Resources.IdProvider.IdProviderFactory import IdProviderFactory
  from DIRAC.FrameworkSystem.private.authorization.AuthServer import collectMetadata
  from DIRAC.FrameworkSystem.private.authorization.utils.Clients import DEFAULT_CLIENTS
  idps = IdProviderFactory()


@pytest.mark.skipif(six.PY2, reason="Skiped for Python 2")
def test_getDIRACClients():
  """ Try to load default DIRAC authorization client
  """
  params = collectMetadata()

  # Try to get DIRAC client authorization settings
  result = idps.getIdProvider('DIRACCLI', **params)
  assert result['OK'], result['Message']
  assert result['Value'].issuer == 'https://issuer.url/'
  assert result['Value'].client_id == DEFAULT_CLIENTS['DIRACCLI']['client_id']
  assert result['Value'].get_metadata('jwks_uri') == 'https://issuer.url/jwk'

  # Try to get DIRAC client authorization settings for Web portal
  result = idps.getIdProvider('DIRACWeb', **params)
  assert result['OK'], result['Message']
  assert result['Value'].issuer == 'https://issuer.url/'
  assert result['Value'].client_id == 'client_identificator'
  assert result['Value'].client_secret == 'client_secret_key'
  assert result['Value'].get_metadata('jwks_uri') == 'https://issuer.url/jwk'


@pytest.mark.skipif(six.PY2, reason="Skiped for Python 2")
def test_getIdPClients():
  """ Try to load external identity provider settings
  """
  # Try to get identity provider by name
  result = idps.getIdProvider('SomeIdP', jwks='my_jwks')
  assert result['OK'], result['Message']
  assert result['Value'].jwks == 'my_jwks'
  assert result['Value'].issuer == 'https://idp.url/'
  assert result['Value'].client_id == 'IdP_client_id'
  assert result['Value'].client_secret == 'IdP_client_secret'
  assert result['Value'].get_metadata('jwks_uri') == 'https://idp.url/jwk'

  # Try to get identity provider for token issued by it
  result = idps.getIdProviderForToken(jwt.encode({'alg': 'HS256'}, dict(
      sub='user',
      iss=result['Value'].issuer,
      iat=int(time.time()),
      exp=int(time.time()) + (12 * 3600),
  ), "secret").decode('utf-8'))
  assert result['OK'], result['Message']
  assert result['Value'].issuer == 'https://idp.url/'
