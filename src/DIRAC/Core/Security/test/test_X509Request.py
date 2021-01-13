""" This contains unit tests to make sure that the migration between PyGSI and M2Crypto is as smooth as possible
    The test covers only the method exposed by the PyGSI version for the time being.

    We are not testing:

    * generateProxyRequest with bitStrengsh -> I should...
    * getIssuerDN: does not make any sense here, is never used
    * generateChainFromResponse: not used


    We are skipping:
    * init with arguments, because never used
    * The delegation mechanism (checkChain method)involves also X509Chain so it is in the X509Chain



"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


# redefined-outer-name is needed because we keep bassing get_X509Chain_class as param
# pylint: disable=redefined-outer-name

from datetime import datetime, timedelta
from string import ascii_letters


from pytest import mark, fixture, skip, raises, approx
parametrize = mark.parametrize

from .x509TestUtilities import CERTS, CERTKEYS, CERTCONTENTS, deimportDIRAC, ENCRYPTEDKEYPASS,\
    ENCRYPTEDKEY, getCertOption, HOSTCERT, KEYCONTENTS_PKCS8, USERCERT, X509REQUESTTYPES, \
    get_X509Request, X509REQUESTTYPES


@parametrize('x509ClassName', X509REQUESTTYPES)
def test_dumpRequest_notInitialized(x509ClassName):
  """ Calls dumpRequest a non initlaized Request"""

  with get_X509Request(x509ClassName) as x509Req:
    res = x509Req.dumpRequest()

    assert res['OK'] is False
    from DIRAC.Core.Utilities.DErrno import ENOCERT
    assert res['Errno'] == ENOCERT


@parametrize('x509ClassName', X509REQUESTTYPES)
def test_dumpRequest(x509ClassName):
  """" Generate an X509Request and dumps it"""
  with get_X509Request(x509ClassName) as x509Req:
    x509Req.generateProxyRequest()

    res = x509Req.dumpRequest()

    assert res['OK']
    assert 'CERTIFICATE REQUEST' in res['Value']


@parametrize('x509ClassName', X509REQUESTTYPES)
def test_loadAllFromString_fromDumpRequest(x509ClassName):
  """ Generate a proxy Request and try loading it from incomplete dump"""
  with get_X509Request(x509ClassName) as x509Req:
    x509Req.generateProxyRequest()

    proxyRequest = x509Req.dumpRequest()['Value']

  # This should fail because the proxyRequest does not contain the private key
  with get_X509Request(x509ClassName) as x509ReqLoad:
    res = x509ReqLoad.loadAllFromString(proxyRequest)

    assert res['OK'] is False
    from DIRAC.Core.Utilities.DErrno import ENOPKEY
    assert res['Errno'] == ENOPKEY


@parametrize('x509ClassName', X509REQUESTTYPES)
@parametrize('isLimited', (False, True))
def test_getSubjectDN(x509ClassName, isLimited):
  """ Try getting the subjectDN in case of limited and non limited request
      :param isLimited: request a limited proxy
  """
  with get_X509Request(x509ClassName) as x509Req:
    x509Req.generateProxyRequest(limited=isLimited)

    res = x509Req.getSubjectDN()
    assert res['OK']

    if isLimited:
      assert res['Value'] == '/CN=limited proxy'
    else:
      assert res['Value'] == '/CN=proxy'


@parametrize('x509ClassName', X509REQUESTTYPES)
@parametrize('isLimited', (False, True))
def test_loadAllFromString(x509ClassName, isLimited):
  """ Generate a proxy Request, load it, and check that the subject DN are the same
      :param isLimited: request a limited proxy
"""
  with get_X509Request(x509ClassName) as x509Req:
    x509Req.generateProxyRequest(limited=isLimited)

    proxyRequest = x509Req.dumpAll()['Value']

  with get_X509Request(x509ClassName) as x509ReqLoad:
    res = x509ReqLoad.loadAllFromString(proxyRequest)

    assert res['OK']
    assert x509Req.getSubjectDN() == x509ReqLoad.getSubjectDN()
