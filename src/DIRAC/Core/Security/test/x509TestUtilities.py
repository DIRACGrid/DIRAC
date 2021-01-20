""" Configuration and utilities for all the X509 unit tests """
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import sys

from datetime import datetime
from pytest import fixture
# We use certificates stored in the same folder as this test file
CERTDIR = os.path.join(os.path.dirname(__file__), 'certs')
HOSTCERT = os.path.join(CERTDIR, 'host/hostcert.pem')
HOSTKEY = os.path.join(CERTDIR, 'host/hostkey.pem')
USERCERT = os.path.join(CERTDIR, 'user/usercert.pem')
USERKEY = os.path.join(CERTDIR, 'user/userkey.pem')
VOMSPROXY = os.path.join(CERTDIR, 'voms/proxy.pem')
ENCRYPTEDKEY = os.path.join(CERTDIR, 'key/encrypted_key_pass_0000.pem')
ENCRYPTEDKEYPASS = '0000'


CERTS = (HOSTCERT, USERCERT)
CERTKEYS = (HOSTKEY, USERKEY)

CERTCONTENTS = {
    'HOSTCERTCONTENT': b"""-----BEGIN CERTIFICATE-----
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
Hia1xkpBKOPRY0BrSGCdEUT5+ict
-----END CERTIFICATE-----
""",
    'USERCERTCONTENT': b"""-----BEGIN CERTIFICATE-----
MIIFszCCA5ugAwIBAgICEAEwDQYJKoZIhvcNAQELBQAwVDEYMBYGA1UECgwPRElS
QUMgQ29tcHV0aW5nMTgwNgYDVQQDDC9ESVJBQyBDb21wdXRpbmcgU2lnbmluZyBD
ZXJ0aWZpY2F0aW9uIEF1dGhvcml0eTAeFw0xODA4MjIwOTE1MTRaFw0zNzEwMjEw
OTE1MTRaMDoxGDAWBgNVBAoMD0RpcmFjIENvbXB1dGluZzENMAsGA1UECgwEQ0VS
TjEPMA0GA1UEAwwGTXJVc2VyMIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKC
AgEAqfZnf9wK+a+qx8kfRlIaehzD2ix+6TKZJ+w9aBlh11b5cPfmIMOmTEXe8rD5
G6WKofOKNBiQ4vX2tEv7psYpetMwQ9R5ks67RN/YGFkzEEO7jzYFtWsS2jbsdHVf
/2wejICPhABYP1sGaQbRWtcp690fZ97cM1c7AuN/fFZ9m3mAoop5Bc6p1hqWSXyZ
ce/0J+/SjtrLeWY8yvMx4ztR+8wQG+hXEAifnT77zwxeH7pPkwj3IFpRozimTmaP
g0wpwUJXUd8LpPnF6pBeZPMybJ4b4TfoddCXSF/wT7q9UfTKptcoLayFCLp+mNJI
KkKUzm/1CBMFkhenzSP7uhjhu3Swr6SXlz1pEW7B9FFyyghLd7FMEuDIAu8ULqLA
ATFR95p5ec3GbObV4OX4G1Up9f6vDle+qhwkQ81uWxebsaVWveUo38Hsl37dqxB9
IxNOC/nTQu58l3KnLodMOweCmDnzHFrC5V96pYrKOaFj2Ijg6TO5maQHo0hfwiAC
FNIvYDb8AxNmDzOVAAZkd/Y0nbYeaO6/eNJzRiwJGKZMnXC3UpzRmIBenDTVMCjE
O1ZjsXe0hwjS0/sRytZHN1jWztnMuYftu3BLUQJQL0cmkWvPGjXKBd9kHhuYjtZu
+SEyLni+6VXJJCyR7/2kmlkq9UimB+RLA+EemW7Ik0oDI48CAwEAAaOBqDCBpTAJ
BgNVHRMEAjAAMB0GA1UdDgQWBBRKwv3rLMXxY6XyF2JDa52CbJoTJDAfBgNVHSME
GDAWgBQEwhevOGTghr8fyQBul28bu06HHzAOBgNVHQ8BAf8EBAMCBeAwEwYDVR0l
BAwwCgYIKwYBBQUHAwIwMwYJYIZIAYb4QgENBCYWJE9wZW5TU0wgR2VuZXJhdGVk
IENsaWVudCBDZXJ0aWZpY2F0ZTANBgkqhkiG9w0BAQsFAAOCAgEAOe2uEU17UWOU
iDsZWLDVYC820sXcC19ijco9zNDVfCkKzPMKKPlEA56dY/Kt0cWAtiklPOiWEtKy
bsM7ayZ2FEiPdBSd9P8qHYFMlbsXcyib5QXpdHebcipu9ORzp+hlFvTA1fFErDn+
nPW+xTCp19tdlrNywxDWXbB4KJZ/VxSVuT4lMZYn6wUOMFN/xj41evGqqQfJm+yT
feW3n2ClDCDbk3br/3KY8eCPLUllZfdJgnN24SWrS4S0tBuOZt+hTt7LISPSPIix
xXNsxLCXq7KsElIlzPPbMsdqDJ/lhDUoHPZZu9chi4t8F5JGkzcn1MOSmn5d74kx
SYD1QTgvX77t0A1E7G55NYiZJTSjoaIQiQwBNEak7Oz9QCh+5qHwR/Np4vo4+d4p
yuWxpzHHBuQrV6dDZ0mONBWx6gxpkFN42mt8EUd26faG7kebbeVoUt1VBTcp9HHH
DKQq9loodgGokarycFeJ8l+ZMM93YoPPVlsijG6Jmn+UrZNzwbi5JcE731qEurGY
U4kjpzpirauwCnOgSm7DwawNoilLFOSSh3/iZgDjMyhspGJ2FwXBlJm7wBWyS+0q
TnsekqTamuTDTAPJRhb2LPVFl0L8+frk1gkpw4KTCzGw4rKW++EUjS1i09sq2Dv6
/fW/ybqxpROqmyLHbqEExj0/hPxPKPw=
-----END CERTIFICATE-----
"""
}

# This is not just a copy paste of the key file content.
# The key file is an RSA key (PKCS1)
# What PyGSI and M2Crypto will print are PKCS8 format.
# To go from RSA to generic key:
# openssl pkcs8 -topk8 -nocrypt -in privkey.pem
# Look for 'BEGIN RSA PRIVATE KEY' in the link bellow
# https://tls.mbed.org/kb/cryptography/asn1-key-structures-in-der-and-pem

KEYCONTENTS_PKCS8 = {
    HOSTKEY: b"""-----BEGIN PRIVATE KEY-----
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
CrerHYr0zdgIXTt+xus9RGGmZi1mfjI=
-----END PRIVATE KEY-----
""",
    USERKEY: b"""-----BEGIN PRIVATE KEY-----
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
oYjpqlB4Kj08eIAI5vcWnt/RcE1tLw==
-----END PRIVATE KEY-----
"""
}

# This contains the attributes of the certificates in order to be compared in the tests
# If they are the same, they are directly at the root, otherwise,
# they  are in subdirectory
CERT_ATTRS = {
    # Just take the date, it is the same for both
    'endDate': datetime.strptime('2037-10-21', '%Y-%m-%d').date(),
    'startDate': datetime.strptime('2018-08-22', '%Y-%m-%d').date(),
    'issuerDN': '/O=DIRAC Computing/CN=DIRAC Computing Signing Certification Authority',
    HOSTCERT: {'subjectDN': '/O=Dirac Computing/O=CERN/CN=VOBox',
               'serial': 4098,
               'availableExtensions': ['authorityKeyIdentifier', 'basicConstraints', 'extendedKeyUsage',\
                                       'keyUsage', 'nsComment', 'subjectAltName', 'subjectKeyIdentifier'],
               'basicConstraints': 'CA:FALSE',
               'subjectAltName': 'DNS:VOBox, DNS:localhost',
               'extendedKeyUsage': 'TLS Web Server Authentication, TLS Web Client Authentication',
               'content': CERTCONTENTS['HOSTCERTCONTENT'],
               'keyFile': HOSTKEY,
               },
    USERCERT: {'subjectDN': '/O=Dirac Computing/O=CERN/CN=MrUser',
               'serial': 4097,
               'availableExtensions': ['authorityKeyIdentifier', 'basicConstraints', 'extendedKeyUsage',\
                                       'keyUsage', 'nsComment', 'subjectKeyIdentifier'],
               'basicConstraints': 'CA:FALSE',
               'subjectAltName': 'DNS:VOBox, DNS:localhost',
               'extendedKeyUsage': 'TLS Web Client Authentication',
               'content': CERTCONTENTS['USERCERTCONTENT'],
               'keyFile': USERKEY,
               }
}

VOMS_PROXY_ATTR = {
    'notBefore': datetime(2018, 10, 23, 9, 11, 44),
    'notAfter': datetime(2024, 7, 6, 17, 11, 44),
    'fqan': ['/fakevo/Role=user/Capability=NULL'],
    'vo': 'fakevo',
    'subject': '/O=Dirac Computing/O=CERN/CN=MrUser',
    'issuer': '/O=Dirac Computing/O=CERN/CN=VOBox'}


def getCertOption(cert, optionName):
  """ Return a given option of a given certificate, taken from CERT_ATTRS

      :param cert: effectively, path to the certificate in question
      :param optionName: name of the options

      :returns: the option
  """

  if optionName in CERT_ATTRS:
    return CERT_ATTRS[optionName]
  return CERT_ATTRS[cert][optionName]


def deimportDIRAC():
  """ clean all what has already been imported from DIRAC.

      This method is extremely fragile, but hopefully, we can get ride of all these
      messy tests soon, when PyGSI has gone.
  """
  if len(X509CHAINTYPES) != 1 or len(X509REQUESTTYPES) != 1:
    raise NotImplementedError(
        "This no longer de-imports DIRAC, if we want to test another SSL wrapper "
        "we will have to find another way of doing this or run a separate pytest "
        "process again"
    )
  # for mod in list(sys.modules):
  #   # You should be careful with what you remove....
  #   if (mod == 'DIRAC' or mod.startswith('DIRAC.')) and not mod.startswith('DIRAC.Core.Security.test'):
  #     sys.modules.pop(mod)


X509CHAINTYPES = ('M2_X509Chain',)

# This fixture will return a pyGSI or M2Crypto X509Chain class
# https://docs.pytest.org/en/latest/fixture.html#automatic-grouping-of-tests-by-fixture-instances


@fixture(scope="function", params=X509CHAINTYPES)
def get_X509Chain_class(request):
  """ Fixture to return either the X509Certificate class.
      It also 'de-import' DIRAC before and after
  """
  # Clean before
  deimportDIRAC()

  x509Class = request.param

  if x509Class == 'M2_X509Chain':
    from DIRAC.Core.Security.m2crypto.X509Chain import X509Chain
  else:
    raise NotImplementedError()

  yield X509Chain

  # Clean after
  deimportDIRAC()


X509REQUESTTYPES = ('M2_X509Request',)

# This fixture will return a X509Request class
# https://docs.pytest.org/en/latest/fixture.html#automatic-grouping-of-tests-by-fixture-instances


@fixture(scope="function", params=X509REQUESTTYPES)
def get_X509Request(request):
  """ Fixture to return either the X509Request instance.
      It also 'de-import' DIRAC before and after
  """
  # Clean before
  deimportDIRAC()

  x509Class = request.param

  if x509Class == 'M2_X509Request':
    from DIRAC.Core.Security.m2crypto.X509Request import X509Request
  else:
    raise NotImplementedError()

  def _generateX509Request():
    """ Instanciate the object
        :returns: an X509Request instance
    """
    return X509Request()

  yield _generateX509Request

  # Clean after
  deimportDIRAC()


def get_X509Chain_from_X509Request(x509ReqObj):
  """ This returns an X509Chain class from the same "type" as the X509Request
      object given as param

      :param x509ReqObj: instance of a X509Request object

      :returns: X509Chain class
  """

  # In principle, we should deimport Dirac everywhere, but I am not even sure it makes any difference
  if 'm2crypto' in x509ReqObj.__class__.__module__:
    from DIRAC.Core.Security.m2crypto.X509Chain import X509Chain
  else:
    raise NotImplementedError()

  return X509Chain
