""" Tests for ProxyProvider modules module
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import sys
import pytest

from diraccfg import CFG

import DIRAC
from DIRAC.Core.Security.X509Chain import X509Chain  # pylint: disable=import-error
from DIRAC.Resources.ProxyProvider import ProxyProviderFactory
from DIRAC.ConfigurationSystem.Client.Helpers import Resources
from DIRAC.ConfigurationSystem.private import ConfigurationClient
from DIRAC.ConfigurationSystem.private.ConfigurationData import ConfigurationData

certsPath = os.path.join(os.path.dirname(DIRAC.__file__), 'Core/Security/test/certs')

localCFGData = ConfigurationData(False)
mergedCFG = CFG()
mergedCFG.loadFromBuffer("""
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
""" % (os.path.join(certsPath, 'ca/ca.cert.pem'), os.path.join(certsPath, 'ca/ca.key.pem')))
localCFGData.localCFG = mergedCFG
localCFGData.remoteCFG = mergedCFG
localCFGData.mergedCFG = mergedCFG
localCFGData.generateNewVersion()


@pytest.fixture
def ppf(monkeypatch):
  monkeypatch.setattr(ConfigurationClient, "gConfigurationData", localCFGData)
  localCFG = ConfigurationClient.ConfigurationClient()
  monkeypatch.setattr(Resources, "gConfig", localCFG)
  monkeypatch.setattr(ProxyProviderFactory, "getInfoAboutProviders", Resources.getInfoAboutProviders)
  return ProxyProviderFactory.ProxyProviderFactory()


@pytest.mark.parametrize(
    "dn, res",
    [('/C=FR/O=DIRAC/OU=DIRAC TEST/CN=DIRAC test user/emailAddress=testuser@diracgrid.org', True),
     ('/C=FR/OU=DIRAC TEST/emailAddress=testuser@diracgrid.org', False),
     ('/C=FR/OU=DIRAC/O=DIRAC TEST/emailAddress=testuser@diracgrid.org', False),
     ('/C=FR/O=DIRAC/BADFIELD=DIRAC TEST/CN=DIRAC test user', False)])
def test_getProxy(ppf, dn, res):
  result = ppf.getProxyProvider('DIRAC_TEST_CA')
  assert result['OK'], result['Message']
  pp = result['Value']

  result = pp.getProxy(dn)
  text = 'Must be ended %s%s' % ('successful' if res else 'with error',
                                 ': %s' % result.get('Message', 'Error message is absent.'))
  assert result['OK'] == res, text
  if res:
    chain = X509Chain()
    chain.loadChainFromString(result['Value'])
    result = chain.getCredentials()
    assert result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.'
    credDict = result['Value']
    assert credDict['username'] == 'testuser', '%s, expected %s' % (credDict['username'], 'testuser')


def test_generateProxyDN(ppf):
  result = ppf.getProxyProvider('DIRAC_TEST_CA')
  assert result['OK'], result['Message']
  pp = result['Value']

  userDict = {"FullName": "John Doe",
              "Email": "john.doe@nowhere.net",
              "O": 'DIRAC',
              'OU': 'DIRAC TEST',
              'C': 'FR'}
  result = pp.generateDN(**userDict)
  assert result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.'
  result = pp.getProxy(result['Value'])
  assert result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.'
  chain = X509Chain()
  chain.loadChainFromString(result['Value'])
  result = chain.getCredentials()
  assert result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.'
  issuer = result['Value']['issuer']
  assert issuer == '/C=FR/O=DIRAC/OU=DIRAC TEST/CN=John Doe/emailAddress=john.doe@nowhere.net'
