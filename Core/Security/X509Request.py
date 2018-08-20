"""
Just a selector for X509Request between PyGSI and M2Crypto based on DIRAC_USE_M2CRYPTO environment variable
"""

# pylint: disable=unused-import

import os

if os.getenv('DIRAC_USE_M2CRYPTO', 'NO').lower() in ('yes', 'true'):
  from DIRAC.Core.Security.m2crypto.X509Request import X509Request
else:
  from DIRAC.Core.Security.pygsi.X509Request import X509Request
