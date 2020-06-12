__RCSID__ = "$Id$"

import os
import mock
import unittest

from DIRAC import S_OK, S_ERROR, gLogger, rootPath
from DIRAC.Resources.ProxyProvider.ProxyProviderFactory import ProxyProviderFactory


certsPath = os.path.join(rootPath, 'DIRAC/Core/Security/test/certs')


def sf_getProviderInfo(provider):
  if providerName == 'MY_DIRACCA':
    return S_OK({'ProviderType': 'DIRACCA',
                 'CertFile': os.path.join(certsPath, 'ca/ca.cert.pem'),
                 'KeyFile': os.path.join(certsPath, 'ca/ca.key.pem'),
                 'Supplied': ['O', 'OU', 'CN'],
                 'Optional': ['emailAddress'],
                 'DNOrder': ['O', 'OU', 'CN', 'emailAddress'],
                 'OU': 'CA',
                 'C': 'DN',
                 'O': 'DIRACCA'})
  if providerName == 'MY_PUSP':
    return S_OK({'ProviderType': 'PUSP', 'ServiceURL': 'https://somedomain'})
  return S_ERROR('No proxy provider found')

@mock.patch('DIRAC.Resources.ProxyProvider.ProxyProviderFactory.getProviderInfo',
            new=sf_getProviderInfo)
class ProxyProviderFactoryTest(unittest.TestCase):
  """ Base class for the ProxyProviderFactory test cases
  """

  def test_standalone(self):
    """ Test loading a proxy provider element with everything defined in itself.
    """
    for provider, resultOfGenerateDN in [('MY_DIRACCA', True), ('MY_PUSP', False)]:
      result = ProxyProviderFactory().getProxyProvider(provider)
      self.assertTrue(result['OK'], '\n' + result.get('Message', 'Error message is absent.'))
      proxyProviderObj = result['Value']
      result = proxyProviderObj.generateDN(FullName='test', Email='email@test.org')
      text = 'Must be ended %s%s' % ('successful' if resultOfGenerateDN else 'with error',
                                     ': %s' % result.get('Message', 'Error message is absent.'))
      self.assertEqual(result['OK'], resultOfGenerateDN, text)
      if not resultOfGenerateDN:
        gLogger.info('Msg: %s' % (result['Message']))
      else:
        self.assertTrue(result['OK'], '\n' + result.get('Message', 'Error message is absent.'))
        userDN = result['Value']
        gLogger.info('Created DN:', userDN)
        result = proxyProviderObj.getProxy(userDN)
        self.assertTrue(result['OK'], '\n' + result.get('Message', 'Error message is absent.'))


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(ProxyProviderFactoryTest)
  testResult = unittest.TextTestRunner(verbosity=3).run(suite)
