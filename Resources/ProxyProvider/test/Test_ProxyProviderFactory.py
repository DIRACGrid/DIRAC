__RCSID__ = "$Id$"

import os
import mock
import unittest

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Resources.ProxyProvider.ProxyProviderFactory import ProxyProviderFactory


thisPath = os.path.dirname(os.path.abspath(__file__)).split('/')
rootPath = thisPath[:len(thisPath) - 3]
certsPath = os.path.join('/'.join(rootPath), 'Core/Security/test/certs')

def sf_getInfoAboutProviders(of, providerName, option, section):
  if of == 'Proxy' and providerName == 'MY_DIRACCA' and option == 'all' and section == 'all':
    return {'ProviderType': 'DIRACCA',
            'CertFile': os.path.join(certsPath, 'ca/ca.cert.pem'),
            'KeyFile': os.path.join(certsPath, 'ca/ca.key.pem'),
            'Supplied': ['O', 'OU', 'CN'],
            'Optional': ['emailAddress'],
            'DNOrder': ['O', 'OU', 'CN', 'emailAddress'],
            'OU': 'CA',
            'C': 'DN',
            'O': 'DIRACCA'}
  return S_ERROR('No proxy provider found')

@mock.patch('DIRAC.Resources.ProxyProvider.ProxyProviderFactory.getInfoAboutProviders',
            new=sf_getInfoAboutProviders())
class ProxyProviderFactoryTest(unittest.TestCase):
  """ Base class for the ProxyProviderFactory test cases
  """

  def test_standalone(self):
    """ Test loading a proxy provider element with everything defined in itself.
    """
    result = ProxyProviderFactory().getProxyProvider('MY_DIRACCA')
    self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
    proxyProviderObj = result['Value']
    result = proxyProviderObj.generateDN(FullName='test', Email='email@test.org')
    self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
    userDN = result['Value']
    gLogger.info('Created DN:', userDN)
    result = proxyProviderObj.getProxy(userDN)
    self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(ProxyProviderFactoryTest)
  testResult = unittest.TextTestRunner(verbosity = 3).run(suite)
