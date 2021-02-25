""" This is a test of the AuthDB
    It supposes that the DB is present and installed in DIRAC
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

# pylint: disable=invalid-name,wrong-import-position,protected-access
import sys
import unittest

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC.FrameworkSystem.DB.AuthDB import AuthDB


db = AuthDB()


class AuthDBTestCase(unittest.TestCase):

  @classmethod
  def setUpClass(cls):
    pass

  def setUp(self):
    pass

  def tearDown(self):
    pass

  @classmethod
  def tearDownClass(cls):
    pass

class testDB(AuthDBTestCase):

  def test_connectDB(self):
    """ Try to connect to the AuthDB
    """
    res = db._connect()
    self.assertTrue(res['OK'])

  def test_Clients(self):
    """ Try to store/get/remove Clients
    """
    # Example of client credentials
    data = {'client_id': 'egfy1547e15s2ReUr0IsolSO0gPcQuLSWWulBWaH6g',
            'client_id_issued_at': 1614204041,
            'client_metadata': {'grant_types': ['authorization_code', 'refresh_token'],
                                'redirect_uris': ['https://marosvn32.in2p3.fr/DIRAC', 'https://marosvn32.in2p3.fr/DIRAC/loginComplete'],
                                'response_types': ['token', 'id_token token', 'code'],
                                'token_endpoint_auth_method': 'client_secret_basic'},
            'client_secret': '90092079a17f7f30930b1d981a2f426ff4fa90bb4698d736',
            'client_secret_expires_at': 0}
    
    # Add client
    result = db.addClient(data)
    self.assertTrue(result['OK'], '\n' + result.get('Message', 'Error message is absent.'))
    self.assertEqual(result['Value'], data, result['Value'] + ', when expected ' + data)

    # Get Client
    result = db.getClient(data['client_id'])
    self.assertTrue(result['OK'], '\n' + result.get('Message', 'Error message is absent.'))
    self.assertEqual(result['Value'], data, result['Value'] + ', when expected ' + data)

    # Remove Client
    result = db.removeClient(data['client_id'])
    self.assertTrue(result['OK'], '\n' + result.get('Message', 'Error message is absent.'))

    # Make sure that the Client is absent
    result = db.getClient(data['client_id'])
    self.assertFalse(result['OK'], '\n' + result.get('Message', 'Error message is absent.'))

  def test_Tokens(self):
    """ Try to store/get/remove Tokens
    """
    # Example of token
    token = {'access_token': '...',
             'refresh_token': '...', 
             'provider': 'IdProvider_1',
             'client_id': 'TKMR11HGRf3O4tciFP3ReIhBIvbgUjkXCzYJmqMhxC',
             'user_id': '20db3fc892432f769f172081dd59fedbd5debe42b45bf4b1'}
    refreshToken = 'my_refresh_token_1234567890'

    # Example of new token
    newToken = {'access_token': '...',
                'provider': 'IdProvider_1',
                'client_id': 'TKMR11HGRf3O4tciFP3ReIhBIvbgUjkXCzYJmqMhxC',
                'user_id': '20db3fc892432f769f172081dd59fedbd5debe42b45bf4b1'}
    
    # Add token
    result = db.storeToken(token)
    self.assertTrue(result['OK'], '\n' + result.get('Message', 'Error message is absent.'))

    # Get token
    result = db.getTokenByUserIDAndProvider(token['user_id'], token['provider'])
    self.assertTrue(result['OK'], '\n' + result.get('Message', 'Error message is absent.'))
    self.assertEqual(result['Value'], token, result['Value'] + ', when expected ' + token)

    # Update token
    result = db.updateToken(newToken, token['refresh_token'])
    self.assertTrue(result['OK'], '\n' + result.get('Message', 'Error message is absent.'))
    self.assertEqual(result['Value'], newToken, result['Value'] + ', when expected ' + newToken)

    # Get token
    result = db.getIdPTokens(newToken['provider'])
    self.assertTrue(result['OK'], '\n' + result.get('Message', 'Error message is absent.'))
    self.assertEqual(result['Value'], [newToken], result['Value'] + ', when expected ' + [newToken])

    # Remove token
    result = db.removeToken(newToken['access_token'])
    self.assertTrue(result['OK'], '\n' + result.get('Message', 'Error message is absent.'))

    # Make sure that the Client is absent
    result = db.getIdPTokens(newToken['provider'])
    self.assertFalse(result['OK'], '\n' + result.get('Message', 'Error message is absent.'))


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(AuthDBTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(testDB))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
  sys.exit(not testResult.wasSuccessful())
