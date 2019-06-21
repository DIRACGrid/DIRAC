#!/bin/env python
"""
tests for ProxyProvider modules module
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
      CertFile = CAcert.pem
      KeyFile = CAkey.pem
      # CAConfigFile = /opt/dirac/pro/tmpCA/tmp7T_olE/CA_conf_file.cnf
      # WorkingDirectory = /opt/dirac/pro/tmpCA
      # CertFile = /opt/dirac/pro/etc/grid-security/DIRACCA/CAcert.pem
      # KeyFile = /opt/dirac/pro/etc/grid-security/DIRACCA/CAkey.pem

      C = FR
      O = DIRAC
      OU = DIRAC TEST
    }
  }
}
"""

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

CAkey = """-----BEGIN RSA PRIVATE KEY-----
MIIEogIBAAKCAQEAzDiL9DJt8+51DfIyllApUMWedK5ELFkx9s/eo17XEe6jN/fu
1JCHOrwxfEvVs+QiX0z32KHfJfctzbfnjSWPbZo9avp13GTbZFJQfIjyEGyfrc5l
VNipIOMigoaTXf78LZa3QvyVEhNAzluRnwHvtuT5rKHQNeOs/B9ApcnT+P40pxbW
pOcMUD1astRWAD4U3j2ElAwPXNun2oFQqRU+fkOAS3ggmSYopN7BHOyZn6q30B4L
tBf50qK5inVu50vIiRRFjH8IR+cziduWyn3oRwEpOgzsCxBTnHsylgOsNdTCw3Qb
8dSSCcjgdyMdokpjgDfIwI6lWvktnpfTbLEWgQIDAQABAoIBAF7+qrNTxeui71Ym
ZDuhXCaTVkrmSRXsA57QQLrzwc04mTnOnYzJEe5TXh14VRbRtt9nuR5O5bMOUIMR
2abBYv9TsOATU0HKtHbtBz12okrbjEdX67DU+48tuH7IxMIDeyBlrCd/wIPg6tNS
quExEGWeCzmvJ1/54RyCGRtFT9HQ74BknBfT6A8AQt6UsetznLY2dmPctoCh2bI6
2MhkBiZtef3uQpvcsbfwcI1dDkGZtwu123PMUiyTr9MCQpBL6jqwPR3KxqCoVSJ7
pZ2MJGUM8LV1KmnjXKu0G1jwECWl2SH5LURU4TmsoUIZpJcWGE9so/9zVJST1+cv
vJIy/VUCgYEA/0UzYpsp2ggi8mOTwCZ5Vq7MxM2mMmZVSEIUeWLyH0j9iK0EEaCx
0NAYkMtAsPzDaHJJoCFU9SGeX5hPCM/oMQOIPxHFVSzpAzi+a9VwG0uhlbv2tHtk
Dm3KTYT866PPsivp5TnuHymrHDlgvDwIWO9qjlO3nll2bzKWE4be0j8CgYEAzM39
UE+j9Eqi8/zb5O3rEWxsEQ1oK15D+r1pcrx6N4yCq3divx3yc/Ursck7Jjoeb0YS
57nURovM1OW2ZUODpj72IVnzWG7m3K23PuIPhc+V6RPGSj7QiN/qTNfk7RR3vEpL
4ouTmhzXIHLoiqizxCoXZFyjZhnNJG1NlxG8Zz8CgYAv+LU7ZqVqz/ShUI1HovNS
ku7wXSVKe2izd5eZaDrQHktnD/yfw28nKrQzIb86g4XFbxTe/uSUXIkCtgDESy37
aAqGr7RB2XrAnD1MzoOO3Zu0I+qs6DNZctB92Owe7F3vwcjmxwg02wPI/g9r0GxR
Kk0ACkOLgox7QSpq6QGeyQKBgFQE9bTq1zIzJGLAC14BlPwS5MqiG2gfRfgpmIbv
d5wuUrURRztsh7i2jfRjv5ZRJYc00jCqdcFzPNbiXk9wwSOElOjdxA01ghRqV9C+
YOveW3vBFwoCdv6QDcj0kQAJ840VVchcxnLk/gRb37Zyuzzwn6QWtRn/377f8ILX
Tdl1AoGAOmomX3ZEVCWgJKGBSKZGyeWz84VmWzv3oepbM7m4LlqqS0xxQj+8H9mJ
y5X2qH3sOXwMz1EsQQk8n+27PUrkBIVhUVf8Svd5pZ6NsGp1EbgrDMODuvTMu+TY
1CujvAI/zNgLKnmYhSLD6MZ2zVOZnfpnxK2iHDcG5LfEV4EUYzo=
-----END RSA PRIVATE KEY-----
"""

CAcert = """-----BEGIN CERTIFICATE-----
MIIDhDCCAmygAwIBAgICEAowDQYJKoZIhvcNAQELBQAwVTELMAkGA1UEBhMCRlIx
GTAXBgNVBAoMEERJUkFDIENvbnNvcnRpdW0xKzApBgNVBAMMIkRJUkFDIFJvb3Qg
Q2VydGlmaWNhdGlvbiBBdXRob3JpdHkwHhcNMTkwNTExMTUzMjIyWhcNMjkwNTA4
MTUzMjIyWjBNMRkwFwYDVQQKDBBESVJBQyBDb25zb3J0aXVtMTAwLgYDVQQDDCdE
SVJBQyBDb2RlIFRlc3QgQ2VydGlmaWNhdGlvbiBBdXRob3JpdHkwggEiMA0GCSqG
SIb3DQEBAQUAA4IBDwAwggEKAoIBAQDMOIv0Mm3z7nUN8jKWUClQxZ50rkQsWTH2
z96jXtcR7qM39+7UkIc6vDF8S9Wz5CJfTPfYod8l9y3Nt+eNJY9tmj1q+nXcZNtk
UlB8iPIQbJ+tzmVU2Kkg4yKChpNd/vwtlrdC/JUSE0DOW5GfAe+25PmsodA146z8
H0ClydP4/jSnFtak5wxQPVqy1FYAPhTePYSUDA9c26fagVCpFT5+Q4BLeCCZJiik
3sEc7JmfqrfQHgu0F/nSormKdW7nS8iJFEWMfwhH5zOJ25bKfehHASk6DOwLEFOc
ezKWA6w11MLDdBvx1JIJyOB3Ix2iSmOAN8jAjqVa+S2el9NssRaBAgMBAAGjZjBk
MB0GA1UdDgQWBBRWEtHrZiybQKpJK8HKErBPuz6FsTAfBgNVHSMEGDAWgBRFC9bQ
c7eQJw1ipbVF4t5aOYTuKjASBgNVHRMBAf8ECDAGAQH/AgEAMA4GA1UdDwEB/wQE
AwIBhjANBgkqhkiG9w0BAQsFAAOCAQEAeAM0C4MhEUBMfIY+2RlFgb5SbU3hqGBx
vSsmVw7bf/2qF/HJBiI8YJ+4H0KGaA1Wc9XzpITonPS3TGNi3+jHpV+2WU77kPyJ
fpG6yDqSH9dSPYHEsG2rFmiRjHNi2rtH+hueLAhBbK1oSLn4ZGBjntF3b2SKq3Zi
b6n+NPJKQNC7W0xfd8u1qqU5K3lAuDTkFtqbjzjXcemN/2Ex09M55tAevD7bBH4u
P0JRow1JioXLH/c6XDRHrASuzdrNya0logqWh56wn11BBBz94PUMNoWNPGtpKS/S
7bP7WJHTesdJQzHgOHmwYICGGiEssqsrx9aSxg6gUiO6FF5lBL7AaQ==
-----END CERTIFICATE-----
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

    with open('CAkey.pem', 'w') as keyfile:
      keyfile.write(CAkey)

    with open('CAcert.pem', 'w') as certfile:
      certfile.write(CAcert)

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
    os.unlink('CAkey.pem')
    os.unlink('CAcert.pem')

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
