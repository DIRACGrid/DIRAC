""" This is a test of the AuthDB
    It supposes that the DB is present and installed in DIRAC
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import six
import time
from authlib.jose import jwt

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC.FrameworkSystem.DB.TokenDB import TokenDB

db = TokenDB()

payload = {'sub': 'user',
           'iss': 'issuer',
           'iat': int(time.time()),
           'exp': int(time.time()) + (12 * 3600),
           'scope': 'scope',
           'setup': 'setup',
           'group': 'my_group'}

exp_payload = payload.copy()
exp_payload['iat'] = int(time.time()) - 10
exp_payload['exp'] = int(time.time()) - 10

DToken = dict(access_token=jwt.encode({'alg': 'HS256'}, payload, "secret").decode('utf-8'),
              refresh_token=jwt.encode({'alg': 'HS256'}, payload, "secret").decode('utf-8'),
              expires_at=int(time.time()) + 3600)

New_DToken = dict(access_token=jwt.encode({'alg': 'HS256'}, payload, "secret").decode('utf-8'),
                  refresh_token=jwt.encode({'alg': 'HS256'}, payload, "secret").decode('utf-8'),
                  issued_at=int(time.time()),
                  expires_in=int(time.time()) + 3600)

Exp_DToken = dict(access_token=jwt.encode({'alg': 'HS256'}, exp_payload, "secret").decode('utf-8'),
                  refresh_token=jwt.encode({'alg': 'HS256'}, exp_payload, "secret").decode('utf-8'),
                  expires_at=int(time.time()) - 10,
                  rt_expires_at=int(time.time()) - 10)


def test_Token():
  """ Try to revoke/save/get tokens
  """
  # Remove all tokens
  result = db.removeToken(user_id=123)
  assert result['OK'], result['Message']

  # Store tokens
  result = db.updateToken(DToken.copy(), userID=123, provider='DIRAC', rt_expired_in=24)
  assert result['OK'], result['Message']
  assert result['Value'] == []

  # Expired token
  result = db.updateToken(Exp_DToken.copy(), userID=123, provider='DIRAC', rt_expired_in=24)
  assert not result['OK']

  # Check token
  result = db.getTokenForUserProvider(userID=123, provider='DIRAC')
  assert result['OK'], result['Message']
  assert result['Value']['access_token'] == DToken['access_token']
  assert result['Value']['refresh_token'] == DToken['refresh_token']

  # Store new tokens
  result = db.updateToken(New_DToken.copy(), userID=123, provider='DIRAC', rt_expired_in=24)
  assert result['OK'], result['Message']
  # Must return old tokens
  assert len(result['Value']) == 1
  assert result['Value'][0]['access_token'] == DToken['access_token']
  assert result['Value'][0]['refresh_token'] == DToken['refresh_token']

  # Check token
  result = db.getTokenForUserProvider(userID=123, provider='DIRAC')
  assert result['OK'], result['Message']
  assert result['Value']['access_token'] == New_DToken['access_token']
  assert result['Value']['refresh_token'] == New_DToken['refresh_token']
