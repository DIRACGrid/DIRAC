# $HeadURL$
__RCSID__ = "$Id$"

# Define here a few constants useful for all the transport layer

#: Default timeout for the RPC call
DEFAULT_RPC_TIMEOUT = 600
#: Default timeout to establish a connection
DEFAULT_CONNECTION_TIMEOUT = 10
#: Default SSL methods accepted. Current default accepts TLSv1 for pyGSI/M2crypto compatibility
#: Can be changed with DIRAC_M2CRYPTO_SSL_METHODS
DEFAULT_SSL_METHODS = 'TLSv1:TLSv2:TLSv3'
