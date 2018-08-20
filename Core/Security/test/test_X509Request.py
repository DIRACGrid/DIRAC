""" This contains unit tests to make sure that the migration between PyGSI and M2Crypto is as smooth as possible
    The test covers only the method exposed by the PyGSI version for the time being.

    We are not testing:

    * generateProxyRequest with bitStrengsh -> I should...
    * getIssuerDN: does not make any sense here, is never used


    We are skipping:
    * init with arguments, because never used


    We are missing:


"""


# redefined-outer-name is needed because we keep bassing get_X509Chain_class as param
# pylint: disable=redefined-outer-name

from datetime import datetime, timedelta
from string import ascii_letters

from hypothesis import given, settings
from hypothesis.strategies import integers, text
settings.max_examples = 200

from pytest import mark, fixture, skip, raises, approx
parametrize = mark.parametrize

from .x509TestConfig import CERTS, CERTKEYS, CERTCONTENTS, deimportDIRAC, ENCRYPTEDKEYPASS,\
    ENCRYPTEDKEY, getCertOption, HOSTCERT, KEYCONTENTS_PKCS8, USERCERT

X509REQUESTTYPES = ('GSI_X509Request', 'M2_X509Request')

# This fixture will return a pyGSI or M2Crypto X509Request class
# https://docs.pytest.org/en/latest/fixture.html#automatic-grouping-of-tests-by-fixture-instances


@fixture(scope="function", params=X509REQUESTTYPES)
def get_X509Request(request):
  """ Fixture to return either the pyGSI or M2Crypto X509Request instance.
      It also 'de-import' DIRAC before and after
  """
  # Clean before
  deimportDIRAC()

  x509Class = request.param

  if x509Class == 'GSI_X509Request':
    from DIRAC.Core.Security.pygsi.X509Request import X509Request
  else:
    from DIRAC.Core.Security.m2crypto.X509Request import X509Request

  def _generateX509Request():
    """ Instanciate the object
        :returns: an X509Request instance
    """
    return X509Request()

  yield _generateX509Request

  # Clean after
  deimportDIRAC()


def test_dumpRequest_notInitialized(get_X509Request):
  """ Calls dumpRequest a non initlaized Request"""

  x509Req = get_X509Request()
  res = x509Req.dumpRequest()

  assert res['OK'] is False
  from DIRAC.Core.Utilities.DErrno import ENOCERT
  assert res['Errno'] == ENOCERT


def test_dumpRequest(get_X509Request):
  """" Generate an X509Request and dumps it"""
  x509Req = get_X509Request()
  x509Req.generateProxyRequest()

  res = x509Req.dumpRequest()

  assert res['OK']
  assert 'CERTIFICATE REQUEST' in res['Value']

def test_loadAllFromString_fromDumpRequest(get_X509Request):
  """ Generate a proxy Request and try loading it from incomplete dump"""
  x509Req = get_X509Request()
  x509Req.generateProxyRequest()

  proxyRequest = x509Req.dumpRequest()['Value']

  # This should fail because the proxyRequest does not contain the private key
  x509ReqLoad =  get_X509Request()
  res = x509ReqLoad.loadAllFromString(proxyRequest)

  assert res['OK'] is False
  from DIRAC.Core.Utilities.DErrno import ENOPKEY
  assert res['Errno'] == ENOPKEY

@parametrize('isLimited', (False, True))
def test_getSubjectDN(get_X509Request, isLimited):
  """ Try getting the subjectDN in case of limited and non limited request
      :param isLimited: request a limited proxy
  """

  x509Req = get_X509Request()
  x509Req.generateProxyRequest(limited = isLimited)

  res = x509Req.getSubjectDN()
  assert res['OK']

  if isLimited:
    assert res['Value'] == '/CN=limited proxy'
  else:
    assert res['Value'] == '/CN=proxy'

@parametrize('isLimited', (False, True))
def test_loadAllFromString(get_X509Request, isLimited):
  """ Generate a proxy Request, load it, and check that the subject DN are the same
      :param isLimited: request a limited proxy
"""
  x509Req = get_X509Request()
  x509Req.generateProxyRequest(limited = isLimited)

  proxyRequest = x509Req.dumpAll()['Value']

  x509ReqLoad =  get_X509Request()
  res = x509ReqLoad.loadAllFromString(proxyRequest)

  assert res['OK']
  assert x509Req.getSubjectDN() == x509ReqLoad.getSubjectDN()
