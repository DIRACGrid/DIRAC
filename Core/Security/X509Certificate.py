"""
Just a selector for X509Certificate between PyGSI and M2Crypto based on DIRAC_USE_M2CRYPTO environment variable
"""
# pylint: disable=unused-import

import os

if os.getenv('DIRAC_USE_M2CRYPTO', 'NO').lower() in ('yes', 'true'):
  from DIRAC.Core.Security.m2crypto.X509Certificate import X509Certificate, \
      DN_MAPPING, DOMAIN_COMPONENT_OID,\
      LIMITED_PROXY_OID, ORGANIZATIONAL_UNIT_NAME_OID,\
      VOMS_EXTENSION_OID, VOMS_FQANS_OID, VOMS_GENERIC_ATTRS_OID
else:
  from DIRAC.Core.Security.pygsi.X509Certificate import X509Certificate, \
      DN_MAPPING, DOMAIN_COMPONENT_OID,\
      LIMITED_PROXY_OID, ORGANIZATIONAL_UNIT_NAME_OID,\
      VOMS_EXTENSION_OID, VOMS_FQANS_OID, VOMS_GENERIC_ATTRS_OID
