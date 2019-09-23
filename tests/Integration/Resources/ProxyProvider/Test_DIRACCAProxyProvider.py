""" Tests for ProxyProvider modules module
"""

import os
import re
import sys
import shutil
import unittest

from DIRAC import gConfig
from DIRAC.Core.Utilities.CFG import CFG
from DIRAC.Core.Security.X509Chain import X509Chain  # pylint: disable=import-error
from DIRAC.Resources.ProxyProvider.ProxyProviderFactory import ProxyProviderFactory

# For Jenkins
for f in ['', 'TestCode', os.environ['DIRAC']]:
  certsPath = os.path.join(f, 'DIRAC/Core/Security/test/certs')
  if os.path.exists(certsPath):
    break

diracTestCACFG = """
Resources
{
  ProxyProviders
  {
    DIRAC_TEST_CA
    {
      ProviderType = DIRACCA
      CAConfigFile = %s
      Match =
      Supplied = C, O, OU, CN
      Optional = emailAddress
      C = FR
      O = DIRAC
      OU = DIRAC TEST
    }
  }
}
""" % os.path.join(certsPath, 'ca/openssl_config_ca.cnf')

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

  @classmethod
  def setUpClass(cls):
    __caPath = os.path.join(certsPath, 'ca')
    cls.caConfigFile = os.path.join(__caPath, 'openssl_config_ca.cnf')

    # Save original configuration file
    lines = []
    shutil.copyfile(cls.caConfigFile, cls.caConfigFile + 'bak')
    with open(cls.caConfigFile, "r") as caCFG:
      for line in caCFG:
        if re.findall('=', re.sub(r'#.*', '', line)):
          field = re.sub(r'#.*', '', line).replace(' ', '').rstrip().split('=')[0]
          line = 'dir = %s #PUT THE RIGHT DIR HERE!\n' % (__caPath) if field == 'dir' else line
        lines.append(line)
    with open(cls.caConfigFile, "w") as caCFG:
      caCFG.writelines(lines)

  @classmethod
  def tearDownClass(cls):
    shutil.move(cls.caConfigFile + 'bak', cls.caConfigFile)

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
        "Email": "testuser@diracgrid.org"
    }
    self.userDictCleanDN = {
        "DN": "/C=FR/O=DIRAC/OU=DIRAC TEST/CN=DIRAC test user/emailAddress=testuser@diracgrid.org",
        "Email": "testuser@diracgrid.org"
    }
    self.userDictGroup = {
        "FullName": "DIRAC test user",
        "Email": "testuser@diracgrid.org",
        "DiracGroup": "dirac_user"
    }

  def tearDown(self):
    pass

  def test_getProxy(self):

    result = self.pp.getProxy(self.userDictClean)
    self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
    chain = X509Chain()
    chain.loadChainFromString(result['Value']['proxy'])
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
    chain.loadChainFromString(result['Value']['proxy'])
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
    chain.loadChainFromString(result['Value']['proxy'])
    result = chain.getCredentials()
    self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
    credDict = result['Value']
    self.assertEqual(credDict['username'], 'testuser',
                     '%s, expected %s' % (credDict['username'], 'testuser'))
    self.assertEqual(credDict['group'], 'dirac_user',
                     '%s, expected %s' % (credDict['group'], 'dirac_user'))

  def test_getUserDN(self):

    goodDN = '/C=FR/O=DIRAC/OU=DIRAC TEST/CN=DIRAC test user/emailAddress=testuser@diracgrid.org'
    badDN_1 = '/C=FR/OU=DIRAC TEST/CN=DIRAC test user/emailAddress=testuser@diracgrid.org'
    badDN_2 = '/C=FR/O=DIRAC/OU=DIRAC TEST/emailAddress=testuser@diracgrid.org'
    badDN_3 = '/C=FR/O=DIRAC/OU=DIRAC TEST/CN=DIRAC test user'
    result = self.pp.getUserDN(userDN=goodDN)
    self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
    result = self.pp.getUserDN(userDN=badDN_1)
    self.assertFalse(result['OK'], 'Must be fail.')
    result = self.pp.getUserDN(userDN=badDN_2)
    self.assertFalse(result['OK'], 'Must be fail.')
    result = self.pp.getUserDN(userDN=badDN_3)
    self.assertFalse(result['OK'], 'Must be fail.')
    userDict = {"FullName": "John Doe", "Email": "john.doe@nowhere.net"}
    result = self.pp.getUserDN(userDict)
    self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
    self.assertEqual(result['Value'], '/C=FR/O=DIRAC/OU=DIRAC TEST/CN=John Doe/emailAddress=john.doe@nowhere.net')


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(DIRACCAPPTest)
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
  sys.exit(not testResult.wasSuccessful())
