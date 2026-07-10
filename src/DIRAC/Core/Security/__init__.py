"""
DIRAC X509 security infrastructure.

This package provides the X509Certificate, X509Chain, X509Request and X509CRL
classes as submodules, as well as a set of OID constants used for handling
proxies and VOMS extensions.

Two implementations of the X509 classes are available: the default one, based
on pyca/cryptography (in the ``pyca`` subpackage), and the legacy one, based
on M2Crypto and pyasn1 (in the ``m2crypto`` subpackage). The implementation is
selected once, at import time, with the DIRAC_USE_M2CRYPTO environment
variable (default No).
"""
import os
from pkgutil import extend_path

# List of OIDs used in handling VOMS extension.
# VOMS extension is encoded in ASN.1 format and it's surprisingly hard to decode. OIDs describe content of sections
# of the data. There is no "official list of OIDs", ones used here are sourced from analyzing VOMS extensions itself
# and different pieces of code and presentations in subject of X509 certificates, certificate extensions and VOMS.
# Googling names or values of those OIDs, especially VOMS related, usually result in up to three pages of results,
# mainly Java code defining those values like code below.
# This is literally lookup table, so I know WTH is this, when I read value and see '1.3.6.1.4.1.8005.100.100.4'.

DOMAIN_COMPONENT_OID = "0.9.2342.19200300.100.1.25"
DIRAC_GROUP_OID = "1.2.42.42"
VOMS_FQANS_OID = "1.3.6.1.4.1.8005.100.100.4"
VOMS_EXTENSION_OID = "1.3.6.1.4.1.8005.100.100.5"
VOMS_TAGS_EXT_OID = "1.3.6.1.4.1.8005.100.100.11"
COMMON_NAME_OID = "2.5.4.3"
SURNAME_OID = "2.5.4.4"
SERIALNUMBER_OID = "2.5.4.5"
COUNTRY_NAME = "2.5.4.6"
LOCALITY_NAME = "2.5.4.7"
STATE_OR_PROVINCE_NAME = "2.5.4.8"
ORGANIZATION_NAME = "2.5.4.10"
ORGANIZATIONAL_UNIT_NAME_OID = "2.5.4.11"
TITLE_OID = "2.5.4.12"
GIVEN_NAME_OID = "2.5.4.42"

# See https://tools.ietf.org/html/rfc3820#appendix-A
PROXY_CERT_INFO_EXTENSION_OID = "1.3.6.1.5.5.7.1.14"
PROXY_OID = "1.3.6.1.5.5.7.21.1"
LIMITED_PROXY_OID = "1.3.6.1.4.1.3536.1.1.1.9"

# Some specific distinguished names: https://www.cryptosys.net/pki/manpki/pki_distnames.html

DN_MAPPING = {
    COMMON_NAME_OID: "/CN=",
    COUNTRY_NAME: "/C=",
    DOMAIN_COMPONENT_OID: "/DC=",
    GIVEN_NAME_OID: "/G=",
    LOCALITY_NAME: "/L=",
    ORGANIZATION_NAME: "/O=",
    ORGANIZATIONAL_UNIT_NAME_OID: "/OU=",
    SERIALNUMBER_OID: "/SERIALNUMBER=",
    STATE_OR_PROVINCE_NAME: "/ST=",
    SURNAME_OID: "/SN=",
    TITLE_OID: "/T=",
}


#: Default strength of the proxy in bit
DEFAULT_PROXY_STRENGTH = 2048


#####
# SUPER DISGUSTING HACK
# We define these variables, and then remove them immediately.
# it is to allow something like 'from DIRAC.Core.Security import X509Chain'
# But pylint would complain just like that
# I've spent a lot of time trying to get pylint to work, but...
# https://github.com/PyCQA/pylint/issues/2474

X509Chain = None
X509CRL = None
X509Certificate = None
X509Request = None

locals().pop("X509Chain")
locals().pop("X509CRL")
locals().pop("X509Certificate")
locals().pop("X509Request")
####


# If we want to use M2Crypto, we add the m2crypto subpackage to the search path,
# otherwise the default pyca/cryptography based subpackage is used.
# This allows imports like 'from DIRAC.Core.Security.X509Chain...' to work transparently
# Nice kind of tricks you find in libraries like xml...
if os.getenv("DIRAC_USE_M2CRYPTO", "No").lower() in ("yes", "true"):
    __path__ = extend_path(__path__, __name__ + ".m2crypto")
else:
    __path__ = extend_path(__path__, __name__ + ".pyca")
