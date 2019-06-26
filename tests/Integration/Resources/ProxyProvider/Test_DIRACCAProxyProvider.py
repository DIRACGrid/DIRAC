#!/bin/env python
""" Tests for ProxyProvider modules module
"""

import unittest
import os

from DIRAC import gConfig
from DIRAC.Core.Utilities.CFG import CFG
from DIRAC.Core.Security.X509Chain import X509Chain
from DIRAC.Resources.ProxyProvider.ProxyProviderFactory import ProxyProviderFactory

diracTestCACFG = """
Resources
{
  ProxyProviders
  {
    DIRAC_TEST_CA
    {
      ProxyProviderType = DIRACCA
      CAConfigFile = %s

      C = FR
      O = DIRAC
      OU = DIRAC TEST
    }
  }
}
""" % os.path.join(os.environ['DIRAC'], 'DIRAC/tests/Integration/certs/ca/openssl_config_ca.cnf')

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


class DIRACCAPPTest(unittest.TestCase):
  """ Base class for the Modules test cases
  """

  def setUp(self):

    cfg = CFG()
    cfg.loadFromBuffer(diracTestCACFG)
    gConfig.loadCFG(cfg)
    cfg.loadFromBuffer(userCFG)
    gConfig.loadCFG(cfg)

    result = ProxyProviderFactory().getProxyProvider('DIRAC_TEST_CA')
    self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
    self.pp = result['Value']

    self.userDictClean = {
        "FullName": "DIRAC test user",
        "EMail": "testuser@diracgrid.org"
    }
    self.userDictCleanDN = {
        "DN": "/C=FR/O=DIRAC/OU=DIRAC Consortium/CN=DIRAC test user/emailAddress=testuser@diracgrid.org",
        "EMail": "testuser@diracgrid.org"
    }
    self.userDictGroup = {
        "FullName": "DIRAC test user",
        "EMail": "testuser@diracgrid.org",
        "DiracGroup": "dirac_user"
    }
    self.userDictNoGroup = {
        "FullName": "DIRAC test user",
        "EMail": "testuser@diracgrid.org",
        "DiracGroup": "dirac_no_user"
    }

  def tearDown(self):
    pass

  def test_getProxy(self):

    result = self.pp.getProxy(self.userDictClean)
    self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
    chain = X509Chain()
    chain.loadChainFromString(result['Value'])
    result = chain.getCredentials()
    self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
    credDict = result['Value']
    self.assertEqual(credDict['username'], 'testuser',
                     '%s, expected %s' % (credDict['username'], 'testuser'))
    self.assertEqual(credDict['group'], 'dirac_user',
                     '%s, expected %s' % (credDict['group'], 'dirac_user'))

  def test_getProxyDN(self):

    result = self.pp.getProxy(self.userDictCleanDN)
    self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
    chain = X509Chain()
    chain.loadChainFromString(result['Value'])
    result = chain.getCredentials()
    self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
    credDict = result['Value']
    self.assertEqual(credDict['username'], 'testuser',
                     '%s, expected %s' % (credDict['username'], 'testuser'))
    self.assertEqual(credDict['group'], 'dirac_user',
                     '%s, expected %s' % (credDict['group'], 'dirac_user'))

  def test_getProxyGroup(self):

    result = self.pp.getProxy(self.userDictGroup)
    self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
    chain = X509Chain()
    chain.loadChainFromString(result['Value'])
    result = chain.getCredentials()
    self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
    credDict = result['Value']
    self.assertEqual(credDict['username'], 'testuser',
                     '%s, expected %s' % (credDict['username'], 'testuser'))
    self.assertEqual(credDict['group'], 'dirac_user',
                     '%s, expected %s' % (credDict['group'], 'dirac_user'))

  def test_getProxyNoGroup(self):

    result = self.pp.getProxy(self.userDictNoGroup)
    self.assertFalse(result['OK'], 'Must be fail.')

  def test_getUserDN(self):

    goodDN = {"DN": '/C=FR/O=DIRAC/OU=DIRAC TEST/CN=DIRAC test user/emailAddress=testuser@diracgrid.org'}
    badDN_1 = {"DN": '/C=FR/OU=DIRAC TEST/CN=DIRAC test user/emailAddress=testuser@diracgrid.org'}
    badDN_2 = {"DN": '/C=FR/O=DIRAC/OU=DIRAC TEST/emailAddress=testuser@diracgrid.org'}
    badDN_3 = {"DN": '/C=FR/O=DIRAC/OU=DIRAC TEST/CN=DIRAC test user'}
    result = self.pp.getUserDN(goodDN)
    self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
    result = self.pp.getUserDN(badDN_1)
    self.assertFalse(result['OK'], 'Must be fail.')
    result = self.pp.getUserDN(badDN_2)
    self.assertFalse(result['OK'], 'Must be fail.')
    result = self.pp.getUserDN(badDN_3)
    self.assertFalse(result['OK'], 'Must be fail.')
    userDict = {"FullName": "John Doe", "EMail": "john.doe@nowhere.net"}
    result = self.pp.getUserDN(userDict)
    self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
    self.assertEqual(result['Value'], '/C=FR/O=DIRAC/OU=DIRAC TEST/CN=John Doe/emailAddress=john.doe@nowhere.net')


if __name__ == '__main__':
  SUITE = unittest.defaultTestLoader.loadTestsFromTestCase(DIRACCAPPTest)
  unittest.TextTestRunner(verbosity=2).run(SUITE)
