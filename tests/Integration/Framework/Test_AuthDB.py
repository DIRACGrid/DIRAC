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


pprint.pprint(gConfig.getOptionsDictRecursively('/Services'))
db = AuthDB()


def test_Clients(self):
  """ Try to store/get/remove Clients
  """
  # Example of client credentials
  data = {'client_id': 'egfy1547e15s2ReUr0IsolS2O0gPcQuLSWWulBWaH6g',
          'client_id_issued_at': 1614204041,
          'client_metadata': {'grant_types': ['authorization_code',
                                              'refresh_token'],
                              'redirect_uris': ['https://marosvn32.in2p3.fr/DIRAC',
                                                'https://marosvn32.in2p3.fr/DIRAC/loginComplete'],
                              'response_types': ['token', 'id_token token', 'code'],
                              'token_endpoint_auth_method': 'client_secret_basic'},
          'client_secret': '90092079a217f7f30930b1d981a2f426ff4fa90bb4698d736',
          'client_secret_expires_at': 0}

  # Add client
  result = db.addClient(data)
  assert result['OK']
  assert result['Value'] == data

  # Get Client
  result = db.getClient(data['client_id'])
  assert result['OK']
  assert result['Value'] == data

  # Remove Client
  result = db.removeClient(data['client_id'])
  assert result['OK']

  # Make sure that the Client is absent
  result = db.getClient(data['client_id'])
  assert result['OK']


def test_Tokens(self):
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

  # Add token
  result = db.storeToken(tData1)
  assert result['OK']

  # Get token
  result = db.getTokenByUserIDAndProvider(tData1['user_id'], tData1['provider'])
  assert result['OK']
  assert result['Value'] == tData1

  # Update token
  result = db.updateToken(tData2, tData1['refresh_token'])
  assert result['OK']
  assert result['Value'] == tData2

  # Get token
  result = db.getIdPTokens(tData2['provider'])
  assert result['OK']
  assert tData2 in result['Value']
  assert tData1 not in result['Value']

  # Remove token
  result = db.removeToken(tData2['access_token'])
  assert result['OK']

  # Make sure that the Client is absent
  result = db.getIdPTokens(tData2['provider'])
  assert not result['OK']
