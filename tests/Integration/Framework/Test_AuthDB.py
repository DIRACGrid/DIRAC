""" This is a test of the AuthDB
    It supposes that the DB is present and installed in DIRAC
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

# pylint: disable=invalid-name,wrong-import-position,protected-access
import sys
import pytest
import pprint

from DIRAC import gConfig
from DIRAC.FrameworkSystem.DB.AuthDB import AuthDB


pprint.pprint(gConfig.getOptionsDictRecursively('/'))
db = AuthDB()


def test_Tokens():
  """ Try to store/get/remove Tokens
  """
  # Example of the new token metadata
  tData1 = {'access_token': 'eyJraWQiOiJvaWRjIiwiYWxnIjoiUlMyNTYifQ.eyJzdWIiOiI5N2ZhZGY2M2U1NWixTokH0OMjseMTQMk36sU5O',
            'client_id': '2C7823B4-4A85-A912-E5D06D955809',
            'expires_at': 1616538163,
            'expires_in': 3599,
            'id_token': 'eyJraWQiOiJvaWRjIiwiYWxnIjoiUlMyNTYifQ.eyJzdWIiOiI5N2ZhZGY2M2U1NWVhMzlkQGVnaS5ldSIsImF1ZCI61',
            'provider': 'CheckIn',
            'refresh_token': 'eyJhbGciOiJub25lIn0.eyJleHAImp0aSI6IjQwNDI5M2YwLTk4NztNDI0Yi04NDZjLWU1NDQzMWRjMmEzZSJ9.',
            'scope': 'openid offline_access profile eduperson_scoped_affiliation eduperson_unique_id',
            'token_type': 'Bearer',
            'user_id': '97fadf63e5123358a4f084e4c136475e377357c6723269f23eb9aba437fd6d9d@egi.eu'}

  # Example of updated token
  tData2 = {'access_token': 'eyJraWQiOiJvaWRjIiwi4e4c136475e377357c6723269f23eb9aba437fd6d9dk36sU5Od',
            'client_id': '2C7823B4-4A85-A912-E5D06D955809',
            'expires_at': 1616538163,
            'expires_in': 3599,
            'id_token': 'eyJraWQiOiJvaWRjIiwiYWxnIjoiUlMy4e4c136475e377357c6723269f23eb9aba4F1ZCI6d1',
            'provider': 'CheckIn',
            'refresh_token': 'eyJhbGciOiJub25lIn0.eyJleHAImp0aSI6IjQ475e377357c6723269f23eb9aba4Fd9.',
            'scope': 'openid offline_access profile eduperson_scoped_affiliation eduperson_unique_id',
            'token_type': 'Bearer',
            'user_id': '97fadf63e5123358a4f084e4c136475e377357c6723269f23eb9aba437fd6d9d@egi.eu'}

  # Remove token
  db.removeToken(tData1['access_token'])
  db.removeToken(tData2['access_token'])

  # Add token
  result = db.storeToken(tData1)
  assert result['OK'], result['Message']

  # Get token
  result = db.getTokenByUserIDAndProvider(tData1['user_id'], tData1['provider'])
  assert result['OK'], result['Message']
  assert result['Value']['refresh_token'] == tData1['refresh_token']
  assert result['Value']['access_token'] == tData1['access_token']

  # Update token
  result = db.updateToken(tData2, tData1['refresh_token'])
  assert result['OK'], result['Message']

  # Get token
  result = db.getTokenByUserIDAndProvider(tData1['user_id'], tData1['provider'])
  assert result['OK'], result['Message']
  assert result['Value']['refresh_token'] == tData2['refresh_token']
  assert result['Value']['access_token'] == tData2['access_token']

  # Get token
  result = db.getIdPTokens(tData2['provider'])
  assert result['OK'], result['Message']
  aTokens = []
  for token in result['Value']:
    aTokens.append(token['access_token'])
  assert tData2['access_token'] in aTokens
  assert tData1['access_token'] not in aTokens

  # Remove token
  result = db.removeToken(tData2['access_token'])
  assert result['OK'], result['Message']

  # Make sure that the Client is absent
  result = db.getIdPTokens(tData2['provider'])
  assert result['OK'], result['Message']
  aTokens = []
  for token in result['Value']:
    aTokens.append(token['access_token'])
  assert tData2['access_token'] not in aTokens
