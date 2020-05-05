"""Environment setup for building documentation.

Sets environment variables.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os


# Set this environment variable such that the documentation
# generated for the various X509* classes is the one with M2Crypto
if 'DIRAC_USE_M2CRYPTO' not in os.environ:
  os.environ['DIRAC_USE_M2CRYPTO'] = 'Yes'
