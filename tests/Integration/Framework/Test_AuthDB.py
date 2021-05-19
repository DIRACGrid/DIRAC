""" This is a test of the AuthDB
    It supposes that the DB is present and installed in DIRAC
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time
from authlib.jose import JsonWebKey, JsonWebSignature, jwt
from authlib.common.encoding import json_b64encode, urlsafe_b64decode, json_loads

from DIRAC.FrameworkSystem.DB.AuthDB import AuthDB

db = AuthDB()

payload = {'sub': 'user',
           'iss': 'issuer',
           'iat': int(time.time()),
           'exp': int(time.time()) + (12 * 3600),
           'scope': 'scope',
           'setup': 'setup',
           'group': 'my_group'}

exp_payload = {'sub': 'user',
               'iss': 'issuer',
               'iat': int(time.time()) - 10,
               'exp': int(time.time()) - 10,
               'scope': 'scope',
               'setup': 'setup',
               'group': 'my_group'}


def test_Token():
  """ Try to revoke/save/get tokens
  """
  # Remove all tokens
  result = db.removeTokens()
  assert result['OK'], result['Message']

  # Get key
  result = db.getPrivateKey()
  assert result['OK'], result['Message']
  privat_key = result['Value']['key']

  # Sign token
  token = dict(access_token=jwt.encode({'alg': 'RS256'}, payload, privat_key),
               expires_in=864000,
               token_type='Bearer',
               client_id='1hlUgttap3P9oTSXUwpIT50TVHxCflN3O98uHP217Y',
               scope='g:checkin-integration_user',
               refresh_token=jwt.encode({'alg': 'RS256'}, payload, privat_key))
  # Expired token
  exp_token = dict(access_token=jwt.encode({'alg': 'RS256'}, exp_payload, privat_key),
                   expires_in=864000,
                   token_type='Bearer',
                   client_id='1hlUgttap3P9oTSXUwpIT50TVHxCflN3O98uHP217Y',
                   scope='g:checkin-integration_user',
                   refresh_token=jwt.encode({'alg': 'RS256'}, exp_payload, privat_key))

  # Store tokens
  result = db.storeToken(token)
  assert result['OK'], result['Message']
  result = db.storeToken(token)
  assert result['OK'], result['Message']

  # Check token
  result = db.getToken(token['refresh_token'])
  assert result['OK'], result['Message']
  assert result['Value']['access_token'] == token['access_token']
  assert result['Value']['refresh_token'] == token['refresh_token']
  assert result['Value']['revoked'] == False

  # Check expired token
  result = db.getToken(exp_token['refresh_token'])
  assert not result['OK']

  # Revoke token
  result = db.revokeToken(token)
  assert result['OK'], result['Message']

  # Check if token revoked
  result = db.getToken(token['refresh_token'])
  assert result['OK'], result['Message']
  assert result['Value']['revoked'] == True


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

  # Sign token
  header['kid'] = result['Value']['kid']
  private_key = result['Value']['key']
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
