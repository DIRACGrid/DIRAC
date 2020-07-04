""" Tests for ProxyProvider modules module
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import re
import sys
import shutil
import unittest

from diraccfg import CFG

from DIRAC import gConfig, S_OK
from DIRAC.Core.Security.X509Chain import X509Chain  # pylint: disable=import-error
from DIRAC.Resources.ProxyProvider.ProxyProviderFactory import ProxyProviderFactory

certsPath = os.path.join(os.environ['DIRAC'], 'DIRAC/Core/Security/test/certs')

diracTestCACFG = """
Resources
{
  ProxyProviders
  {
    DIRAC_TEST_CA
    {
      ProviderType = DIRACCA
      CertFile = %s
      KeyFile = %s
      Match =
      Supplied = C, O, OU, CN
      Optional = emailAddress
      DNOrder = C, O, OU, CN, emailAddress
      C = FR
      O = DIRAC
      OU = DIRAC TEST
    }
  }
}
""" % (os.path.join(certsPath, 'ca/ca.cert.pem'), os.path.join(certsPath, 'ca/ca.key.pem'))

userCFG = """
Registry
{
  Users
  {
    testuser
    {
      DN = /C=FR/O=DIRAC/OU=DIRAC TEST/CN=DIRAC test user/emailAddress=testuser@diracgrid.org
    }
  }
  Groups
  {
    dirac_user
    {
      Users = testuser
    }
    dirac_no_user
    {
      Users = nouser
    }
  }
}
"""


class proxyManager(object):
  """ Fake proxyManager
  """
  def _storeProxy(self, userDN, chain):
    """ Fake store method
    """
    return S_OK()


class DIRACCAPPTest(unittest.TestCase):
  """ Base class for the Modules test cases
  """

  @classmethod
  def setUpClass(cls):
    pass

  @classmethod
  def tearDownClass(cls):
    pass

  def setUp(self):

    cfg = CFG()
    cfg.loadFromBuffer(diracTestCACFG)
    gConfig.loadCFG(cfg)
    cfg.loadFromBuffer(userCFG)
    gConfig.loadCFG(cfg)

    result = ProxyProviderFactory().getProxyProvider('DIRAC_TEST_CA', proxyManager=proxyManager())
    self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
    self.pp = result['Value']

  def tearDown(self):
    pass

  def test_getProxy(self):
    for dn, res in [('/C=FR/O=DIRAC/OU=DIRAC TEST/CN=DIRAC test user/emailAddress=testuser@diracgrid.org', True),
                    ('/C=FR/OU=DIRAC TEST/emailAddress=testuser@diracgrid.org', False),
                    ('/C=FR/OU=DIRAC/O=DIRAC TEST/emailAddress=testuser@diracgrid.org', False),
                    ('/C=FR/O=DIRAC/BADFIELD=DIRAC TEST/CN=DIRAC test user', False)]:
      result = self.pp.getProxy(dn)
      text = 'Must be ended %s%s' % ('successful' if res else 'with error',
                                     ': %s' % result.get('Message', 'Error message is absent.'))
      self.assertEqual(result['OK'], res, text)
      if res:
        chain = X509Chain()
        chain.loadChainFromString(result['Value'])
        result = chain.getCredentials()
        self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
        credDict = result['Value']
        self.assertEqual(credDict['username'], 'testuser',
                         '%s, expected %s' % (credDict['username'], 'testuser'))

  def test_generateProxyDN(self):

    userDict = {"FullName": "John Doe",
                "Email": "john.doe@nowhere.net",
                "O": 'DIRAC',
                'OU': 'DIRAC TEST',
                'C': 'FR'}
    result = self.pp.generateDN(**userDict)
    self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
    result = self.pp.getProxy(result['Value'])
    self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
    chain = X509Chain()
    chain.loadChainFromString(result['Value'])
    result = chain.getCredentials()
    self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
    issuer = result['Value']['issuer']
    self.assertEqual(issuer, '/C=FR/O=DIRAC/OU=DIRAC TEST/CN=John Doe/emailAddress=john.doe@nowhere.net')


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(DIRACCAPPTest)
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
  sys.exit(not testResult.wasSuccessful())
