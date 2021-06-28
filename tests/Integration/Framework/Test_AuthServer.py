""" This is a test of the AuthServer
    It supposes that the AuthDB is present and installed in DIRAC
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import six
import time
import pytest
from mock import MagicMock

from diraccfg import CFG

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

import DIRAC
from DIRAC import S_OK, S_ERROR, gConfig
from DIRAC.FrameworkSystem.private.authorization.AuthServer import AuthServer


class Proxy(object):
  def dumpAllToString(self):
    return S_OK('proxy')


class ProxyManagerClient(object):
  def downloadProxy(self, *args, **kwargs):
    return S_OK(Proxy())


class TokenManagerClient(object):
  def getToken(self, *args, **kwargs):
    return S_OK({'access_token': 'token', 'refresh_token': 'token'})


mockgetIdPForGroup = MagicMock(return_value=S_OK('IdP'))
mockgetDNForUsername = MagicMock(return_value=S_OK('DN'))
mockgetUsernameForDN = MagicMock(return_value=S_OK('user'))
mockisDownloadablePersonalProxy = MagicMock(return_value=True)


@pytest.fixture
def server(mocker):
  cfg = CFG()
  cfg.loadFromBuffer("""
  DIRAC
  {
    Security
    {
      Authorization
      {
        issuer = https://issuer.url/
      }
    }
  }
  """)
  gConfig.loadCFG(cfg)
  mocker.patch("DIRAC.FrameworkSystem.private.authorization.AuthServer.getIdPForGroup",
              side_effect=mockgetIdPForGroup)
  mocker.patch("DIRAC.FrameworkSystem.private.authorization.AuthServer.getDNForUsername",
              side_effect=mockgetDNForUsername)
  mocker.patch("DIRAC.FrameworkSystem.private.authorization.AuthServer.getUsernameForDN",
              side_effect=mockgetUsernameForDN)
  mocker.patch("DIRAC.FrameworkSystem.private.authorization.AuthServer.ProxyManagerClient",
              side_effect=ProxyManagerClient)
  mocker.patch("DIRAC.FrameworkSystem.private.authorization.AuthServer.TokenManagerClient",
              side_effect=TokenManagerClient)
  mocker.patch("DIRAC.FrameworkSystem.private.authorization.AuthServer.isDownloadablePersonalProxy",
              side_effect=mockisDownloadablePersonalProxy)
  return DIRAC.FrameworkSystem.private.authorization.AuthServer.AuthServer()


@pytest.mark.skipif(six.PY2, reason="requires python3")
def test_metadata(server):
  """ Check metadata
  """
  assert server.metadata.get('issuer')


@pytest.mark.skipif(six.PY2, reason="requires python3")
def test_queryClient(server):
  """ Try to search some default client
  """
  assert not server.query_client('not_exist_client')
  assert server.query_client('DIRAC_CLI').client_id == 'DIRAC_CLI'


@pytest.mark.skipif(six.PY2, reason="requires python3")
@pytest.mark.parametrize("client, grant, user, scope, expires_in, refresh_token, instance, result", [
    ('DIRAC_CLI', None, 'id', 'g:my_group proxy', None, None, 'proxy', 'proxy'),
    ('DIRAC_CLI', None, 'id', 'g:my_group', None, None, 'access_token', 'token'),
])
def test_generateToken(server, client, grant, user, scope, expires_in, refresh_token, instance, result):
  """ Generate tokens
  """
  from authlib.oauth2.base import OAuth2Error
  cli = server.query_client(client)
  try:
    assert server.generate_token(cli, grant, user, scope, expires_in, refresh_token).get(instance) == result
  except OAuth2Error as e:
    assert False, str(e)


@pytest.mark.skipif(six.PY2, reason="requires python3")
def test_writeReadRefreshToken(server):
  """ Try to search some default client
  """
  result = server.registerRefreshToken({}, {'access_token': 'token', 'refresh_token': 'token'})
  assert result['OK'], result['Message']
  token = result['Value']
  assert token.get('access_token') == 'token'
  assert token.get('refresh_token') != 'token'

  result = server.readToken(token['refresh_token'])
  assert result['OK'], result['Message']
  assert result['Value'].get('jti')
  assert result['Value'].get('iat')
