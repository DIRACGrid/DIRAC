""" This contains unit tests to make sure that the migration between PyGSI and M2Crypto is as smooth as possible
    The test covers only the method exposed by the PyGSI version for the time being.

    We are not testing:
    -> setChain: simple setter
    -> setPKey: simple setter

    * generateProxyRequest
    * generateChainFromRequestString


    We are skipping:
    * init with certList as argument, because never used
    * generateProxyToString: strength (nah....)


    We are missing:
      test proxy from proxy ? Is it a usecase ?


"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


# redefined-outer-name is needed because we keep bassing get_X509Chain_class as param
# pylint: disable=redefined-outer-name

from datetime import datetime, timedelta
from string import ascii_letters, digits
import sys

from hypothesis import given, settings, HealthCheck
from hypothesis.strategies import integers, text

from pytest import mark, fixture, skip, raises, approx
parametrize = mark.parametrize

# function_scoped_fixture is only used in Python 3 compatible release of hypothesis
if sys.version_info.major >= 3:
  function_scoped = (HealthCheck.function_scoped_fixture,)
else:
  function_scoped = tuple()

from DIRAC.Core.Security.test.x509TestUtilities import (
    CERTS, CERTKEYS, CERTCONTENTS, deimportDIRAC, ENCRYPTEDKEYPASS, ENCRYPTEDKEY,
    getCertOption, HOSTCERT, KEYCONTENTS_PKCS8, USERCERT, get_X509Chain_class,
    X509CHAINTYPES, get_X509Request, get_X509Chain_from_X509Request
)


ONE_YEAR_IN_SECS = 3600 * 24 * 365
TWENTY_YEARS_IN_SEC = 20 * ONE_YEAR_IN_SECS

# Validity date wont go further than 2050, See RFC 5280 4.1.2.5 for more information.
NO_LATER_THAN_2050_IN_SEC = int((datetime.strptime("2049-12-31", "%Y-%M-%d") - datetime.now()).total_seconds())


@fixture(scope="function", params=X509CHAINTYPES)
def get_proxy(request):
  """ Fixture to return either the proxy string.
      It also 'de-import' DIRAC before and after
  """
  # Clean before

  # Oh what a dirty hack....
  # When you do the delegation, you call both Request and Proxy generation fixtures.
  # So if you do the cleaning twice, you end up in a terrible mess.
  # So, do not do the cleaning if you are in the test_delegation method
  if request.function.__name__ != 'test_delegation':
    deimportDIRAC()
  x509Class = request.param

  if x509Class == 'M2_X509Chain':
    from DIRAC.Core.Security.m2crypto.X509Chain import X509Chain
  else:
    raise NotImplementedError()

  def _generateProxy(certFile, lifetime=3600, **kwargs):
    """ Generate the proxyString and return it as an X509Chain object

        :param certFile: path to the certificate
        :param lifetime: lifetime of the proxy in seconds

        :returns:  X509Chain object
    """
    # Load the certificate and the key
    x509Chain = X509Chain()
    x509Chain.loadChainFromFile(certFile)
    x509Chain.loadKeyFromFile(getCertOption(certFile, 'keyFile'))

    # Generate the proxy string
    res = x509Chain.generateProxyToString(lifetime, rfc=True, **kwargs)

    proxyString = res['Value']
    # Load the proxy string as an X509Chain object
    proxyChain = X509Chain()
    proxyChain.loadProxyFromString(proxyString)

    return proxyChain

  yield _generateProxy

  # Clean after
  deimportDIRAC()


@parametrize('cert_file', CERTS)
def test_loadChainFromFile(cert_file, get_X509Chain_class):
  """" Just load a certificate chain"""
  x509Chain = get_X509Chain_class()
  res = x509Chain.loadChainFromFile(cert_file)
  assert res['OK']


def test_loadChainFromFile_non_existing_file(get_X509Chain_class):
  """" Just loadChain a non existing file"""
  X509Chain = get_X509Chain_class()
  res = X509Chain.loadChainFromFile('/tmp/nonexistingFile.pem')
  assert not res['OK']

  from DIRAC.Core.Utilities.DErrno import EOF

  assert res['Errno'] == EOF


# pylint: disable=unused-argument
@parametrize('cert_content_type', CERTCONTENTS)
def test_loadChainFromString(cert_content_type, get_X509Chain_class, indirect=('hostcertcontent', 'usercertcontent')):
  """" Just loadChain a certificate from PEM string
      :param cert_content_type: either HOSTCERTCONTENT or USERCERTCONTENT

  """
  X509Chain = get_X509Chain_class()
  res = X509Chain.loadChainFromString(CERTCONTENTS[cert_content_type])
  assert res['OK'], res


def test_loadChainFromString_non_pem(get_X509Chain_class):
  """" Just loadChain a non pem formated string """
  X509Chain = get_X509Chain_class()
  res = X509Chain.loadChainFromString('THIS IS NOT PEM DATA')
  assert not res['OK']

  from DIRAC.Core.Utilities.DErrno import EX509

  assert res['Errno'] == EX509


@parametrize('key_file', CERTKEYS)
def test_init_with_key(key_file, get_X509Chain_class):
  """ Test init with key object as argument and check the content"""

  chain1 = get_X509Chain_class()
  chain1.loadKeyFromFile(key_file)

  # Get the key and check the number of bits
  keyObj = chain1.getPKeyObj()['Value']

  chain2 = get_X509Chain_class(keyObj=keyObj)
  assert chain1.dumpPKeyToString() == chain2.dumpPKeyToString()
  # Careful ! The two keys are the same object
  assert chain2.getPKeyObj()['Value'] is keyObj


@parametrize('key_file', CERTKEYS)
def test_privatekey_without_password(key_file, get_X509Chain_class):
  """ Test loading a key from a file, retrieve the object and check the content"""

  X509Chain = get_X509Chain_class()
  res = X509Chain.loadKeyFromFile(key_file)
  assert res['OK']
  # Get the key and check the number of bits
  res = X509Chain.getPKeyObj()
  assert res['OK']
  assert res['Value'].size() == 512

  # Check that the content of the object is correct.
  # CAUTION ! The object is PKCS8, while the file contains PKCS1.
  # Check the comment of KEYCONTENTS_PKCS8
  res = X509Chain.dumpPKeyToString()
  assert res['Value'] == KEYCONTENTS_PKCS8[key_file]


def test_privatekey_with_password(get_X509Chain_class):
  """ Test loading a password protected key from a file and retrieve the object """
  X509Chain = get_X509Chain_class()
  res = X509Chain.loadKeyFromFile(ENCRYPTEDKEY, password=ENCRYPTEDKEYPASS)
  assert res['OK']
  # Get the key and check the number of bits
  res = X509Chain.getPKeyObj()
  assert res['OK']
  assert res['Value'].size() == 512


def test_privatekey_with_wrong_password(get_X509Chain_class):
  """ Try loading a password protected key with the wrong password"""
  X509Chain = get_X509Chain_class()
  res = X509Chain.loadKeyFromFile(ENCRYPTEDKEY, password='WRONGPASSWRD')
  assert not res['OK']

  from DIRAC.Core.Utilities.DErrno import ECERTREAD

  assert res['Errno'] == ECERTREAD


@parametrize('cert_file', CERTS)
def test_getCertInChain_on_cert(cert_file, get_X509Chain_class):
  """" Load a chain, get the first certificate, and check its name"""
  x509Chain = get_X509Chain_class()
  x509Chain.loadChainFromFile(cert_file)
  res = x509Chain.getCertInChain(0)
  assert res['OK']

  certSubject = res['Value'].getSubjectDN().get('Value')
  assert certSubject == getCertOption(cert_file, 'subjectDN')


def test_getCertInChain_too_far(get_X509Chain_class):
  """" Load a chain, get too far in the certificate chain"""
  x509Chain = get_X509Chain_class()
  x509Chain.loadChainFromFile(HOSTCERT)
  # it should raise IndexError if too far
  with raises(IndexError):
    x509Chain.getCertInChain(1)


@parametrize('cert_file', CERTS)
def test_getCertList(cert_file, get_X509Chain_class):
  """" Load a chain, and get its length."""
  x509Chain = get_X509Chain_class()
  x509Chain.loadChainFromFile(cert_file)
  # For a certificate, there should be only 1 certificate in the chain

  assert len(x509Chain.getCertList()['Value']) == 1
  assert len(x509Chain.getCertList()['Value']) == x509Chain.getNumCertsInChain()['Value']


@parametrize('cert_file', CERTS)
def test_certProperties(cert_file, get_X509Chain_class):
  """ Try on a certificate if it is a proxy, limited proxy, VOMS, valid proxy, rfc """
  x509Chain = get_X509Chain_class()
  x509Chain.loadChainFromFile(cert_file)

  # These methods should return False
  assert x509Chain.isProxy()['Value'] is False
  assert x509Chain.isLimitedProxy()['Value'] is False
  assert x509Chain.isVOMS()['Value'] is False

  assert x509Chain.isRFC()['Value'] is False

  from DIRAC.Core.Utilities.DErrno import ENOCHAIN

  # Now these methods should complain that it is not a proxy
  # After all, why would you do something logical...
  assert x509Chain.isValidProxy()['Errno'] == ENOCHAIN


@parametrize('cert_file', CERTS)
def test_getVOMSData_on_cert(cert_file, get_X509Chain_class):
  """" Load a  Chain with only a certificate and load the (non existing VOMS data)
      Of course, it will behave differently from the certificate...
  """

  x509Chain = get_X509Chain_class()
  x509Chain.loadChainFromFile(cert_file)

  res = x509Chain.getVOMSData()

  assert res['OK']

  # The VOMS data of a certificate chain composed of only a certificate is... False
  assert res['Value'] is False


@parametrize('cert_file', CERTS)
def test_getDIRACGroup_on_cert(cert_file, get_X509Chain_class):
  """" Load a  Chain with only a certificate and get the (non existing) DIRAC Group
      Of course, it will behave differently from the certificate...
  """

  x509Chain = get_X509Chain_class()
  x509Chain.loadChainFromFile(cert_file)

  # ignoreDefault is used such that there is no attempt to look for group in the CS
  res = x509Chain.getDIRACGroup(ignoreDefault=True)

  assert not res['OK']

  from DIRAC.Core.Utilities.DErrno import EX509

  assert res['Errno'] == EX509

# TODO: have a non valid certificate to try


@parametrize('cert_file', CERTS)
def test_hasExpired(cert_file, get_X509Chain_class):
  """" Load a valid certificate and check it has not expired"""
  x509Chain = get_X509Chain_class()
  x509Chain.loadChainFromFile(cert_file)

  res = x509Chain.hasExpired()
  assert res['OK']
  assert not res['Value']


@parametrize('cert_file', CERTS)
def test_getNotAfterDate(cert_file, get_X509Chain_class):
  """" Load a valid certificate and check its expiration date"""
  x509Chain = get_X509Chain_class()
  x509Chain.loadChainFromFile(cert_file)

  res = x509Chain.getNotAfterDate()

  assert res['OK']
  # We expect getNotAfterDate to return a datetime
  assert res['Value'].date() == getCertOption(cert_file, 'endDate')


@parametrize('cert_file', CERTS)
def test_getRemainingSecs_on_cert(cert_file, get_X509Chain_class):
  """" Load a valid certificate and check the output is a positive integer"""

  x509Chain = get_X509Chain_class()
  x509Chain.loadChainFromFile(cert_file)

  res = x509Chain.getRemainingSecs()

  assert res['OK']
  assert isinstance(res['Value'], int) and res['Value'] > 0


@parametrize('cert_file', CERTS)
def test_dumpChainToString_on_cert(cert_file, get_X509Chain_class):
  """" Load a valid certificate in a chain, and dump all to string"""

  x509Chain = get_X509Chain_class()
  x509Chain.loadChainFromFile(cert_file)

  res = x509Chain.dumpChainToString()

  assert res['OK']

  assert res['Value'] == getCertOption(cert_file, 'content')


@parametrize('cert_file', CERTS)
def test_isPUSP_on_cert(cert_file, get_X509Chain_class):
  """" Load a valid certificate in a chain, and check isPUSP"""

  x509Chain = get_X509Chain_class()
  x509Chain.loadChainFromFile(cert_file)

  res = x509Chain.isPUSP()

  assert res['OK']
  assert res['Value'] is False


@parametrize('cert_file', CERTS)
def test_getCredentials_on_cert(cert_file, get_X509Chain_class):
  """" Load a valid certificate in a chain, and check the information returned.
       We do not check the values, they are already checked in other tests
  """

  x509Chain = get_X509Chain_class()
  x509Chain.loadChainFromFile(cert_file)

  credentialInfo = ['DN', 'isLimitedProxy', 'isProxy', 'issuer', 'secondsLeft', 'subject', 'validDN', 'validGroup']

  res = x509Chain.getCredentials(ignoreDefault=True)

  assert res['OK']
  assert sorted(res['Value']) == sorted(credentialInfo)


@parametrize('cert_file', CERTS)
def test_hash_on_cert(cert_file, get_X509Chain_class):
  """" Load a valid certificate in a chain, and check the hash
       It is supposed to raise an exception because it is not a proxy
  """

  x509Chain = get_X509Chain_class()
  x509Chain.loadChainFromFile(cert_file)

  # Because hash expects a proxy, it will attempt to access the diracGroup attribute
  # and raise a KeyError
  with raises(KeyError):
    x509Chain.hash()


#######################################################################
# From here we start proxy tests
########################################################################


def test_generateProxyToString(get_proxy):
  """" Generate a proxy and check that the chain has two level
  """

  proxyChain = get_proxy(USERCERT)
  # We are supposed now to have two elements in the chain
  assert proxyChain.getNumCertsInChain()['Value'] == 2


def test_getCertInChain(get_proxy):
  """" retrieve the first certificate in the chain, and make sure it is the original one
  """

  proxyChain = get_proxy(USERCERT)

  chainLength = proxyChain.getNumCertsInChain()['Value']

  res = proxyChain.getCertInChain(certPos=chainLength - 1)

  assert res['OK']

  certSubject = res['Value'].getSubjectDN().get('Value')
  assert certSubject == getCertOption(USERCERT, 'subjectDN')

  # bonus: check the negative counter also works
  assert certSubject == proxyChain.getCertInChain(certPos=- 1)['Value'].getSubjectDN().get('Value')

  # Test default value
  assert proxyChain.isProxy()['Value'] is True
  assert proxyChain.isLimitedProxy()['Value'] is False
  assert proxyChain.isVOMS()['Value'] is False
  assert proxyChain.isRFC()['Value'] is True
  assert proxyChain.isValidProxy()['Value'] is True


@mark.slow
@settings(max_examples=200, suppress_health_check=function_scoped)
@given(lifetime=integers(max_value=ONE_YEAR_IN_SECS, min_value=1))
def test_proxyLifetime(get_proxy, lifetime):
  """" Generate a proxy with various lifetime, smaller than the certificate length
        :param lifetime: lifetime of the proxy in seconds
  """

  proxyChain = get_proxy(USERCERT, lifetime=lifetime)

  res = proxyChain.getNotAfterDate()
  assert res['OK']

  notAfterDate = res['Value']
  expectedValidity = datetime.utcnow() + timedelta(seconds=lifetime)

  # The two value should coincide with a margin of 2 seconds
  margin = 2
  assert (notAfterDate - expectedValidity).total_seconds() == approx(0, abs=margin)


@mark.slow
@settings(max_examples=200, suppress_health_check=function_scoped)
@given(lifetime=integers(min_value=TWENTY_YEARS_IN_SEC, max_value=NO_LATER_THAN_2050_IN_SEC))
def test_tooLong_proxyLifetime(get_proxy, lifetime):
  """" Generate a proxy with various lifetime, longer than the certificate length
        :param lifetime: lifetime of the proxy in seconds
  """

  proxyChain = get_proxy(USERCERT, lifetime=lifetime)

  res = proxyChain.getNotAfterDate()
  assert res['OK']

  notAfterDate = res['Value']

  # The expected validity is the validity of the certificate

  certObj = proxyChain.getCertInChain(-1)['Value']
  expectedEndDate = certObj.getNotAfterDate()['Value']
  # The two value should coincide with a margin of 2 seconds
  assert notAfterDate == expectedEndDate

# def generateProxyToString(self, lifeTime, diracGroup=False, strength=1024, limited=False, proxyKey=False, rfc = True):

# hypthesis successfully prove that m2crypto implementation does not work
# for an empty group name, or 0, or whatever can be evaluated to False . Fine...
# Let's just focus on letters and '-'


@mark.slow
@settings(max_examples=200, suppress_health_check=function_scoped)
@given(diracGroup=text(ascii_letters + '-_' + digits, min_size=1))
def test_diracGroup(get_proxy, diracGroup):
  """ Generate a proxy with a given group and check that we can retrieve it"""
  proxyChain = get_proxy(USERCERT, diracGroup=diracGroup)

  res = proxyChain.getDIRACGroup(ignoreDefault=True)
  assert res['OK']

  assert len(res['Value']) == len(diracGroup)
  assert res['Value'] == diracGroup


@parametrize('isLimited', (True, False))
def test_limitedProxy(get_proxy, isLimited):
  """ Generate limited and non limited proxy"""
  # A group is needed to be limited
  proxyChain = get_proxy(USERCERT, diracGroup='anyGroup', limited=isLimited)

  res = proxyChain.isLimitedProxy()
  assert res['OK']

  assert res['Value'] is isLimited


def test_getIssuerCert(get_proxy):
  """ Generate a proxy and check the issuer of the certificate"""
  proxyChain = get_proxy(USERCERT)

  res = proxyChain.getIssuerCert()
  assert res['OK']

  assert res['Value'].getSubjectDN()['Value'] == getCertOption(USERCERT, 'subjectDN')


################################################################
# From now on, test proxy coming from Requests
################################################################

#
# retVal = chain.generateChainFromRequestString(reqDict['request'],
#                                               lifetime=chainLifeTime,
#                                               diracGroup=diracGroup,
#                                               rfc = rfcIfPossible)
@mark.slow
@settings(max_examples=200, suppress_health_check=function_scoped)
@given(diracGroup=text(ascii_letters + '-', min_size=1), lifetime=integers(min_value=1, max_value=TWENTY_YEARS_IN_SEC))
def test_delegation(get_X509Request, get_proxy, diracGroup, lifetime):
  """
      Test the delegation mechanism.
      Generate a proxy request and generate the proxy from there
      NOTE: DO NOT CHANGE THE NAME OF THIS TEST FUNCTION ! See get_proxy code for details

      :param diracGroup: group of the initial proxy
      :param lifetime: requested lifetime of the delegated proxy
  """

  # The server side generates a request
  # Equivalent to ProxyManager.requestDelegationUpload
  x509Req = get_X509Request()
  x509Req.generateProxyRequest()
  reqStr = x509Req.dumpRequest()['Value']

  # This object contains both the public and private key
  pkeyReq = x509Req.getPKey()

  #######################################################

  # The client side signs the request

  proxyChain = get_proxy(USERCERT, diracGroup=diracGroup)

  # The proxy will contain a "bullshit private key"
  res = proxyChain.generateChainFromRequestString(reqStr, lifetime=lifetime)

  # This is sent back to the server
  delegatedProxyString = res['Value']

  ######################################################
  # Equivalent to ProxyManager.completeDelegationUpload

  # Dirty hack:
  X509Chain = get_X509Chain_from_X509Request(x509Req)

  # Create the new chain
  # the pkey was generated together with the Request
  delegatedProxy = X509Chain(keyObj=pkeyReq)
  delegatedProxy.loadChainFromString(delegatedProxyString)

  # make sure the public key match between Request and the new Chain
  # (Stupid, of course it will ! But it is done in the ProxyManager...)
  res = x509Req.checkChain(delegatedProxy)

  assert res['OK']

  # perform a few checks on the generated proxy

  # There should be one level extra in the delegated proxy
  assert proxyChain.getNumCertsInChain()['Value'] + 1 == delegatedProxy.getNumCertsInChain()['Value']

  # The issuer of the delegatedProxy should be the original proxy
  assert proxyChain.getCertInChain()['Value'].getSubjectDN() == delegatedProxy.getCertInChain()['Value'].getIssuerDN()

  # The groups should be the same
  assert proxyChain.getDIRACGroup(ignoreDefault=True) == delegatedProxy.getDIRACGroup(ignoreDefault=True)

  assert proxyChain.getNotAfterDate()['Value'] >= delegatedProxy.getNotAfterDate()['Value']
