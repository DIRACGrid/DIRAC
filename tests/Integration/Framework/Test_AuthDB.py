""" This is a test of the AuthDB
    It supposes that the DB is present and installed in DIRAC
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time
from authlib.jose import JsonWebKey, JsonWebSignature, jwt, RSAKey
from authlib.common.encoding import json_b64encode, urlsafe_b64decode, json_loads

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC.FrameworkSystem.DB.AuthDB import AuthDB

db = AuthDB()

payload = {'sub': 'user',
           'iss': 'issuer',
           'iat': int(time.time()),
           'exp': int(time.time()) + (12 * 3600),
           'scope': 'scope',
           'setup': 'setup',
           'group': 'my_group'}

DToken = dict(access_token=jwt.encode({'alg': 'HS256'}, payload, "secret"),
              refresh_token=jwt.encode({'alg': 'HS256'}, payload, "secret"),
              expires_at=int(time.time()) + 3600)

New_DToken = dict(access_token=jwt.encode({'alg': 'HS256'}, payload, "secret"),
                  refresh_token=jwt.encode({'alg': 'HS256'}, payload, "secret"),
                  expires_in=int(time.time()) + 3600)


def test_RefreshToken():
  """ Try to revoke/save/get refresh tokens
  """
  preset_jti = '123'

  # Remove refresh token
  result = db.revokeRefreshToken(preset_jti)
  assert result['OK'], result['Message']

  # Store tokens
  result = db.storeRefreshToken(DToken.copy(), preset_jti)
  assert result['OK'], result['Message']
  assert result['Value']['jti'] == preset_jti
  assert result['Value']['iat'] <= int(time.time())

  result = db.storeRefreshToken(New_DToken.copy())
  assert result['OK'], result['Message']
  assert result['Value']['jti']
  assert result['Value']['iat'] <= int(time.time())

  token_id = result['Value']['jti']
  issued_at = result['Value']['iat']

  # Check token
  result = db.getCredentialByRefreshToken(preset_jti)
  assert result['OK'], result['Message']
  assert result['Value']['jti'] == preset_jti
  assert result['Value']['access_token'] == DToken['access_token']
  assert result['Value']['refresh_token'] == DToken['refresh_token']

  result = db.getCredentialByRefreshToken(token_id)
  assert result['OK'], result['Message']
  assert result['Value']['jti'] == token_id
  assert int(result['Value']['issued_at']) == issued_at
  assert result['Value']['access_token'] == New_DToken['access_token']
  assert result['Value']['refresh_token'] == New_DToken['refresh_token']

  # Check token after request
  for jti in [token_id, preset_jti]:
    result = db.getCredentialByRefreshToken(jti)
    assert result['OK'], result['Message']
    assert not result['Value']

  # Renew tokens
  result = db.storeRefreshToken(New_DToken.copy(), token_id)
  assert result['OK'], result['Message']

  # Revoke token
  result = db.revokeRefreshToken(token_id)
  assert result['OK'], result['Message']

  # Check token
  result = db.getCredentialByRefreshToken(token_id)
  assert result['OK'], result['Message']
  assert not result['Value']


def test_keys():
  """ Try to store/get/remove keys
  """
  # JWS
  jws = JsonWebSignature(algorithms=['RS256'])
  code_payload = {'user_id': 'user',
                  'scope': 'scope',
                  'redirect_uri': 'redirect_uri',
                  'client_id': 'client',
                  'code_challenge': 'code_challenge'}

  # Token metadata
  header = {'alg': 'RS256'}
  payload = {'sub': 'user',
             'iss': 'issuer',
             'scope': 'scope',
             'setup': 'setup',
             'group': 'my_group'}

  # Remove all keys
  result = db.removeKeys()
  assert result['OK'], result['Message']

  # Check active keys
  result = db.getActiveKeys()
  assert result['OK'], result['Message']
  assert result['Value'] == []

  # Create new one
  result = db.getPrivateKey()
  assert result['OK'], result['Message']
  assert type(result['Value']['rsakey']) is RSAKey
  assert type(result['Value']['strkey']) is str

  # Sign token
  header['kid'] = result['Value']['kid']
  private_key = result['Value']['rsakey']

  # Find key by KID
  result = db.getPrivateKey(header['kid'])
  assert result['OK'], result['Message']
  assert result['Value']['rsakey'] == private_key

  token = jwt.encode(header, payload, private_key)
  # Sign auth code
  code = jws.serialize_compact(header, json_b64encode(code_payload), private_key)

  # Get public key set
  result = db.getKeySet()
  assert result['OK'], result['Message']
  _payload = jwt.decode(token, JsonWebKey.import_key_set(result['Value'].as_dict()))
  assert _payload == payload
  data = jws.deserialize_compact(code, result['Value'].keys[0])
  _code_payload = json_loads(urlsafe_b64decode(data['payload']))
  assert _code_payload == code_payload

  # Get JWK
  result = db.getJWKs()
  assert result['OK'], result['Message']
  _payload = jwt.decode(token, JsonWebKey.import_key_set(result['Value']))
  assert _payload == payload, result['Value']


def test_Sessions():
  """ Try to store/get/remove Sessions
  """
  # Example of the new session metadata
  sData1 = {'client_id': 'DIRAC_CLI',
            'device_code': 'SsoGTDglu6LThpx0CigM9i9J72B5atZ24ULr6R1awm',
            'expires_in': 1800,
            'id': 'SsoGTDglu6LThpx0CigM9i9J72B5atZ24ULr6R1awm',
            'interval': 5,
            'scope': 'g:my_group',
            'uri': 'https://domain.com/DIRAC/auth/device?&response_type=device&client_id=DIRAC_CLI&scope=g:my_group',
            'user_code': 'MDKP-MXMF',
            'verification_uri': 'https://domain.com/DIRAC/auth/device',
            'verification_uri_complete': u'https://domain.com/DIRAC/auth/device?user_code=MDKP-MXMF'}

  # Example of the updated session
  sData2 = {'client_id': 'DIRAC_CLI',
            'device_code': 'SsoGTDglu6LThpx0CigM9i9J72B5atZ24ULr6R1awm',
            'expires_in': 1800,
            'id': 'SsoGTDglu6LThpx0CigM9i9J72B5atZ24ULr6R1awm',
            'interval': 5,
            'scope': 'g:my_group',
            'uri': 'https://domain.com/DIRAC/auth/device?&response_type=device&client_id=DIRAC_CLI&scope=g:my_group',
            'user_code': 'MDKP-MXMF',
            'verification_uri': 'https://domain.com/DIRAC/auth/device',
            'verification_uri_complete': u'https://domain.com/DIRAC/auth/device?user_code=MDKP-MXMF',
            'user_id': 'username'}

  # Remove old session
  db.removeSession(sData1['id'])

  # Add session
  result = db.addSession(sData1)
  assert result['OK'], result['Message']

  # Get session
  result = db.getSessionByUserCode(sData1['user_code'])
  assert result['OK'], result['Message']
  assert result['Value']['device_code'] == sData1['device_code']
  assert result['Value'].get('user_id') != sData2['user_id']

  # Update session
  result = db.updateSession(sData2, sData1['id'])
  assert result['OK'], result['Message']

  # Get session
  result = db.getSession(sData2['id'])
  assert result['OK'], result['Message']
  assert result['Value']['device_code'] == sData2['device_code']
  assert result['Value']['user_id'] == sData2['user_id']

  # Remove session
  result = db.removeSession(sData2['id'])
  assert result['OK'], result['Message']

  # Make sure that the session is absent
  result = db.getSession(sData2['id'])
  assert result['OK'], result['Message']
  assert not result['Value']