"""
Just a selector for X509Chain between PyGSI and M2Crypto based on DIRAC_USE_M2CRYPTO environment variable
"""
# pylint: disable=unused-import

import os

if os.getenv('DIRAC_USE_M2CRYPTO', 'NO').lower() in ('yes', 'true'):
  from DIRAC.Core.Security.m2crypto.X509Chain import X509Chain, isPUSPdn, g_X509ChainType
else:
  from DIRAC.Core.Security.pygsi.X509Chain import X509Chain, isPUSPdn, g_X509ChainType
