from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

# Define here a few constants useful for all the transport layer

#: Default timeout for the RPC call
DEFAULT_RPC_TIMEOUT = 600
#: Default timeout to establish a connection
DEFAULT_CONNECTION_TIMEOUT = 10

#: Default SSL Ciher accepted. Current default is for pyGSI/M2crypto compatibility
#: Can be changed with DIRAC_M2CRYPTO_SSL_CIPHERS
#: Recommandation (incompatible with pyGSI)
# pylint: disable=line-too-long
#: AES256-GCM-SHA384:AES256-SHA256:AES128-GCM-SHA256:AES128-SHA256:HIGH:MEDIUM:RSA:!3DES:!RC4:!aNULL:!eNULL:!MD5:!SEED:!IDEA:!SHA # noqa
# Cipher line should be as readable as possible, sorry pylint
# pylint: disable=line-too-long
DEFAULT_SSL_CIPHERS = 'AES256-GCM-SHA384:AES256-SHA256:AES256-SHA:CAMELLIA256-SHA:AES128-GCM-SHA256:AES128-SHA256:AES128-SHA:HIGH:MEDIUM:RSA:!3DES:!RC4:!aNULL:!eNULL:!MD5:!SEED:!IDEA'  # noqa

#: Default SSL methods accepted. Current default accepts TLSv1 for pyGSI/M2crypto compatibility
#: Can be changed with DIRAC_M2CRYPTO_SSL_METHODS
#: Recommandation (incompatible with pyGSI)
# TLSv2:TLSv3
DEFAULT_SSL_METHODS = 'TLSv1:TLSv2:TLSv3'
