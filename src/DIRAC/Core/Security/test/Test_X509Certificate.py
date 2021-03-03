""" This contains unit tests to make sure that the migration between PyGSI and M2Crypto is as smooth as possible
    The test covers only the method exposed by the PyGSI version for the time being.

    We are not testing:

    * generateProxyRequest -> boils down to testing X509Request
    * setCertificate -> not used anywhere

    We are skipping:
    * getPublicKey -> no way to test that really
    * getSerialNumber -> buggy in PyGSI

"""

# redefined-outer-name is needed because we keep passing get_X509Certificate_class as param
# pylint: disable=redefined-outer-name

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
from DIRAC.Core.Security.test.x509TestUtilities import (
    deimportDIRAC, CERTS, CERTCONTENTS, getCertOption, HOSTCERT, VOMSPROXY, VOMS_PROXY_ATTR
)

from pytest import mark, fixture, skip
parametrize = mark.parametrize

X509CERTTYPES = ('M2_X509Certificate',)

# This fixture will return a X509Certificate class
# https://docs.pytest.org/en/latest/fixture.html#automatic-grouping-of-tests-by-fixture-instances


@fixture(scope="function", params=X509CERTTYPES)
def get_X509Certificate_class(request):
  """ Fixture to return the X509Certificate class.
      It also 'de-import' DIRAC before and after
  """
  # Clean before
  deimportDIRAC()

  x509Class = request.param

  if x509Class == 'M2_X509Certificate':
    from DIRAC.Core.Security.m2crypto.X509Certificate import X509Certificate
  else:
    raise NotImplementedError()

  yield X509Certificate

  # Clean after
  deimportDIRAC()


@parametrize('cert_file', CERTS)
def test_executeOnlyIfCertLoaded(cert_file, get_X509Certificate_class):
  """" Tests whether the executeOnlyIfCertLoaded decorator works"""
  x509Cert = get_X509Certificate_class()
  # Since we did not load the certificate, we should get S_ERROR
  res = x509Cert.getNotAfterDate()
  from DIRAC.Core.Utilities.DErrno import ENOCERT
  assert res['Errno'] == ENOCERT

  # Now load it
  x509Cert.load(cert_file)
  res = x509Cert.getNotAfterDate()
  assert res['OK']


@parametrize('cert_file', CERTS)
def test_load(cert_file, get_X509Certificate_class):
  """" Just load a certificate """
  x509Cert = get_X509Certificate_class()
  res = x509Cert.load(cert_file)
  assert res['OK']


def test_load_non_existing_file(get_X509Certificate_class):
  """" Just load a non existing file and non pem formated string """
  x509Cert = get_X509Certificate_class()
  res = x509Cert.load('/tmp/nonexistingFile.pem')
  assert not res['OK']

  from DIRAC.Core.Utilities.DErrno import ECERTREAD

  assert res['Errno'] == ECERTREAD


@parametrize('cert_file', CERTS)
def test_loadFromFile(cert_file, get_X509Certificate_class):
  """" Just load a certificate """
  x509Cert = get_X509Certificate_class()
  res = x509Cert.loadFromFile(cert_file)
  assert res['OK']


def test_loadFromFile_non_existing_file(get_X509Certificate_class):
  """" Just load a non existing file"""
  x509Cert = get_X509Certificate_class()
  res = x509Cert.loadFromFile('/tmp/nonexistingFile.pem')
  assert not res['OK']

  from DIRAC.Core.Utilities.DErrno import EOF

  assert res['Errno'] == EOF


# pylint: disable=unused-argument
@parametrize('cert_content_type', CERTCONTENTS)
def test_loadFromString(cert_content_type, get_X509Certificate_class, indirect=('hostcertcontent', 'usercertcontent')):
  """" Just load a certificate from PEM string
      :param cert_content_type: either HOSTCERTCONTENT or USERCERTCONTENT
      :param indirect: pytest trick,
            see https://docs.pytest.org/en/latest/example/parametrize.html#apply-indirect-on-particular-arguments
  """
  x509Cert = get_X509Certificate_class()
  res = x509Cert.loadFromString(CERTCONTENTS[cert_content_type])
  assert res['OK'], res


def test_loadFromString_non_pem(get_X509Certificate_class):
  """" Just load a non pem formated string """
  x509Cert = get_X509Certificate_class()
  res = x509Cert.loadFromString('THIS IS NOT PEM DATA')
  assert not res['OK']

  from DIRAC.Core.Utilities.DErrno import ECERTREAD

  assert res['Errno'] == ECERTREAD


# TODO: have a non valid certificate to try
@parametrize('cert_file', CERTS)
def test_hasExpired(cert_file, get_X509Certificate_class):
  """" Load a valid certificate and check it has not expired"""
  x509Cert = get_X509Certificate_class()
  x509Cert.load(cert_file)

  res = x509Cert.hasExpired()
  assert res['OK']
  assert not res['Value']


@parametrize('cert_file', CERTS)
def test_getNotAfterDate(cert_file, get_X509Certificate_class):
  """" Load a valid certificate and check its expiration date"""
  x509Cert = get_X509Certificate_class()
  x509Cert.load(cert_file)

  res = x509Cert.getNotAfterDate()

  assert res['OK']
  # We expect getNotAfterDate to return a datetime
  assert res['Value'].date() == getCertOption(cert_file, 'endDate')


@parametrize('cert_file', CERTS)
def test_getNotBeforeDate(cert_file, get_X509Certificate_class):
  """" Load a valid certificate and check its start validity date"""
  x509Cert = get_X509Certificate_class()
  x509Cert.load(cert_file)

  res = x509Cert.getNotBeforeDate()

  assert res['OK']
  # We expect getNotBeforeDate to return a datetime
  assert res['Value'].date() == getCertOption(cert_file, 'startDate')


@parametrize('cert_file', CERTS)
def test_getSubjectDN(cert_file, get_X509Certificate_class):
  """" Load a valid certificate and check its subject"""
  x509Cert = get_X509Certificate_class()
  x509Cert.load(cert_file)

  res = x509Cert.getSubjectDN()

  assert res['OK']
  assert res['Value'] == getCertOption(cert_file, 'subjectDN')


@parametrize('cert_file', CERTS)
def test_getIssuerDN(cert_file, get_X509Certificate_class):
  """" Load a valid certificate and check its issuer"""
  x509Cert = get_X509Certificate_class()
  x509Cert.load(cert_file)

  res = x509Cert.getIssuerDN()

  assert res['OK']
  assert res['Value'] == getCertOption(cert_file, 'issuerDN')


# # TODO: this method seems not to be used anyway
# @parametrize('cert_file', CERTS)
# def test_getSubjectNameObject(cert_file, get_X509Certificate_class):
#   """" Load a valid certificate and check its subject object. """
#   x509Cert = get_X509Certificate_class()
#   x509Cert.load(cert_file)

#   res = x509Cert.getSubjectNameObject()

#   assert res['OK']
#   # We cannot compare the objects themselves because it is a different object...
#   expectedValue = getCertOption(cert_file, 'subjectDN')
#   try:
#     # This works in the case of pyGSI
#     returnedValue = res['Value'].one_line()
#   except AttributeError:
#     # This works in the case of M2Crypto
#     returnedValue = str(res['Value'])

#   assert returnedValue == expectedValue


# # TODO: this method seems not to be used anyway
# @parametrize('cert_file', CERTS)
# def test_getIssuerNameObject(cert_file, get_X509Certificate_class):
#   """" Load a valid certificate and check its subject object. """
#   x509Cert = get_X509Certificate_class()
#   x509Cert.load(cert_file)

#   res = x509Cert.getIssuerNameObject()

#   assert res['OK']
#   # We cannot compare the objects themselves because it is a different object...
#   expectedValue = getCertOption(cert_file, 'issuerDN')
#   try:
#     # This works in the case of pyGSI
#     returnedValue = res['Value'].one_line()
#   except AttributeError:
#     # This works in the case of M2Crypto
#     returnedValue = str(res['Value'])

#   assert returnedValue == expectedValue


@mark.skip(reason="no way of currently testing this")
@parametrize('cert_file', CERTS)
def test_getPublicKey(cert_file, get_X509Certificate_class):
  """" Load a valid certificate and verify its public key (for m2crypto only)"""
  x509Cert = get_X509Certificate_class()
  x509Cert.load(cert_file)

  res = x509Cert.getPublicKey()
  assert res['OK']

  if 'm2crypto' in get_X509Certificate_class.__module__:
    print(x509Cert.verify(res['Value']))


@parametrize('cert_file', CERTS)
def test_getSerialNumber(cert_file, get_X509Certificate_class):
  """" Load a valid certificate and check its public key"""

  x509Cert = get_X509Certificate_class()

  x509Cert.load(cert_file)

  res = x509Cert.getSerialNumber()

  assert res['OK']
  assert res['Value'] == getCertOption(cert_file, 'serial')


@parametrize('cert_file', CERTS)
def test_getDIRACGroup_on_cert(cert_file, get_X509Certificate_class):
  """" Load a valid certificate and check if there is a dirac group. It should not"""

  x509Cert = get_X509Certificate_class()
  x509Cert.load(cert_file)

  # ignoreDefault is used such that there is no attempt to look for group in the CS
  res = x509Cert.getDIRACGroup(ignoreDefault=True)

  assert res['OK']
  assert res['Value'] is False


@parametrize('cert_file', CERTS)
def test_hasVOMSExtensions_on_cert(cert_file, get_X509Certificate_class):
  """" Load a valid certificate and check if it has VOMS extensions. It should not"""

  x509Cert = get_X509Certificate_class()
  x509Cert.load(cert_file)

  # ignoreDefault is used such that there is no attempt to look for group in the CS
  res = x509Cert.hasVOMSExtensions()

  assert res['OK']
  assert res['Value'] is False


@parametrize('cert_file', CERTS)
def test_getVOMSData_on_cert(cert_file, get_X509Certificate_class):
  """" Load a valid certificate and load the (non existing VOMS data)"""

  x509Cert = get_X509Certificate_class()
  x509Cert.load(cert_file)

  res = x509Cert.getVOMSData()
  from DIRAC.Core.Utilities.DErrno import EVOMS

  assert not res['OK']
  assert res['Errno'] == EVOMS


@parametrize('cert_file', CERTS)
def test_getRemainingSecs_on_cert(cert_file, get_X509Certificate_class):
  """" Load a valid certificate and check the output is a positive integer"""

  x509Cert = get_X509Certificate_class()
  x509Cert.load(cert_file)

  res = x509Cert.getRemainingSecs()

  assert res['OK']
  assert isinstance(res['Value'], int) and res['Value'] > 0


@parametrize('cert_file', CERTS)
def test_getExtensions_on_cert(cert_file, get_X509Certificate_class):
  """" Load a valid certificate and check the output is a positive integer"""

  x509Cert = get_X509Certificate_class()
  x509Cert.load(cert_file)

  res = x509Cert.getExtensions()

  assert res['OK']

  extensionDict = dict(extTuple for extTuple in res['Value'])

  assert sorted(extensionDict) == sorted(getCertOption(cert_file, 'availableExtensions'))

  # Test a few of them
  for ext in ('basicConstraints', 'extendedKeyUsage'):
    assert extensionDict[ext] == getCertOption(cert_file, ext)

  # Valid only for Host certificate:
  if cert_file == HOSTCERT:
    assert extensionDict['subjectAltName'] == getCertOption(cert_file, 'subjectAltName')


###########################################################################
# Temporary. For the time being, we need a real proxy !

def test_getVOMSData(get_X509Certificate_class):
  """" Load a valid certificate and check the output is a positive integer"""

  x509Cert = get_X509Certificate_class()
  x509Cert.load(VOMSPROXY)

  res = x509Cert.getVOMSData()
  assert res['OK']
  assert res['Value'] == VOMS_PROXY_ATTR


def test_hasVOMSExtensions(get_X509Certificate_class):
  """" Load a certificate generated with voms-proxy-fake and check hasVOMSExtension is True"""

  x509Cert = get_X509Certificate_class()
  x509Cert.load(VOMSPROXY)
  res = x509Cert.hasVOMSExtensions()
  assert res['OK']
  assert res['Value']
