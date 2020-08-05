#!/usr/bin/env python
"""
Utilities for using M2Crypto SSL with DIRAC.
"""

import os
from M2Crypto import SSL, m2

from DIRAC.Core.Security import Locations
from DIRAC.Core.Security.m2crypto.X509Chain import X509Chain

# Default ciphers to use if unspecified
# Cipher line should be as readable as possible, sorry pylint
# pylint: disable=line-too-long
DEFAULT_SSL_CIPHERS = "AES256-GCM-SHA384:AES256-SHA256:AES256-SHA:CAMELLIA256-SHA:AES128-GCM-SHA256:AES128-SHA256:AES128-SHA:HIGH:MEDIUM:RSA:!3DES:!RC4:!aNULL:!eNULL:!MD5:!SEED:!IDEA"  # noqa
# Verify depth of peer certs
VERIFY_DEPTH = 50


def __loadM2SSLCTXHostcert(ctx):
  """ Load hostcert & key from the default location and set them as the
      credentials for SSL context ctx.
      Returns None.
  """
  certKeyTuple = Locations.getHostCertificateAndKeyLocation()
  if not certKeyTuple:
    raise RuntimeError("Hostcert/key location not set")
  hostcert, hostkey = certKeyTuple
  if not os.path.isfile(hostcert):
    raise RuntimeError("Hostcert file (%s) is missing" % hostcert)
  if not os.path.isfile(hostkey):
    raise RuntimeError("Hostkey file (%s) is missing" % hostkey)
  # Make sure we never stall on a password prompt if the hostkey has a password
  # by specifying a blank string.
  ctx.load_cert(hostcert, hostkey, callback=lambda: "")


def __loadM2SSLCTXProxy(ctx, proxyPath=None):
  """ Load proxy from proxyPath (or default location if not specified) and
      set it as the certificate & key to use for this SSL context.
      Returns None.
  """
  if not proxyPath:
    proxyPath = Locations.getProxyLocation()
  if not proxyPath:
    raise RuntimeError("Proxy location not set")
  if not os.path.isfile(proxyPath):
    raise RuntimeError("Proxy file (%s) is missing" % proxyPath)
  # See __loadM2SSLCTXHostcert for description of why lambda is needed.
  ctx.load_cert_chain(proxyPath, proxyPath, callback=lambda: "")


def getM2SSLContext(ctx=None, **kwargs):
  """ Gets an M2Crypto.SSL.Context configured using the standard
      DIRAC connection keywords from kwargs. The keywords are:

        - clientMode: Boolean, if False hostcerts are always used. If True
                               a proxy is used unless other flags are set.
        - useCertificates: Boolean, Set to true to use hostcerts in client
                           mode.
        - proxyString: String, no-longer supported, used to allow a literal
                               proxy string to be provided.
        - proxyLocation: String, Path to file to use as proxy, defaults to
                                 usual location(s) if not set.
        - skipCACheck: Boolean, if True, don't verify peer certificates.
        - sslMethod: String, List of SSL algorithms to enable in OpenSSL style
                              cipher format, e.g. "SSLv3:TLSv1".
        - sslCiphers: String, OpenSSL style cipher string of ciphers to allow
                              on this connection.

      If an existing context "ctx" is provided, it is just reconfigured with
      the selected arguments.

      Returns the new or updated context.
  """
  if not ctx:
    ctx = SSL.Context()
  # Set certificates for connection
  # CHRIS: I think clientMode was just an internal of pyGSI implementation
  # if kwargs.get('clientMode', False) and not kwargs.get('useCertificates', False):
  # if not kwargs.get('useCertificates', False):
  if kwargs.get('bServerMode', False) or kwargs.get('useCertificates', False):
    # Server mode always uses hostcert
    __loadM2SSLCTXHostcert(ctx)

  else:
    # Client mode has a choice of possible options
    if kwargs.get('proxyString', None):
      # We don't support this any more, there is no easy way
      # to convert a proxy string to something usable by M2Crypto SSL
      # Try writing it to a temp file and use proxyLocation instead?
      raise RuntimeError("Proxy string no longer supported.")
    else:
      # Use normal proxy
      __loadM2SSLCTXProxy(ctx, proxyPath=kwargs.get('proxyLocation', None))

  # Set peer verification
  if kwargs.get('skipCACheck', False):
    # Don't validate peer, but still request creds
    ctx.set_verify(SSL.verify_none, VERIFY_DEPTH)
  else:
    # Do validate peer
    ctx.set_verify(SSL.verify_peer | SSL.verify_fail_if_no_peer_cert, VERIFY_DEPTH)
    # Set CA location
    caPath = Locations.getCAsLocation()
    if not caPath:
      raise RuntimeError("Failed to find CA location")
    if not os.path.isdir(caPath):
      raise RuntimeError("CA path (%s) is not a valid directory" % caPath)
    ctx.load_verify_locations(capath=caPath)

  # Other parameters
  sslMethod = kwargs.get('sslMethod', None)
  if sslMethod:
    # Pylint can't see the m2 constants due to the way the library is loaded
    # We just have to disable that warning for the next bit...
    # pylint: disable=no-member
    methods = [('SSLv2', m2.SSL_OP_NO_SSLv2),
               ('SSLv3', m2.SSL_OP_NO_SSLv3),
               ('TLSv1', m2.SSL_OP_NO_TLSv1)]
    allowed_methods = sslMethod.split(':')
    # If a method isn't explicitly allowed, set the flag to disable it...
    for method, method_flag in methods:
      if method not in allowed_methods:
        ctx.set_options(method_flag)
    # SSL_OP_NO_SSLv2, SSL_OP_NO_SSLv3, SSL_OP_NO_TLSv1
  ciphers = kwargs.get('sslCiphers', DEFAULT_SSL_CIPHERS)
  ctx.set_cipher_list(ciphers)

  # log the debug messages
  # ctx.set_info_callback()

  return ctx


def getM2PeerInfo(conn):
  """ Gets the details of the current peer as a standard dict. The peer
      details are obtained from the supplied M2 SSL Connection obj "conn".
      The details returned are those from ~X509Chain.getCredentials, without Registry info:

         DN - Full peer DN as string
         x509Chain - Full chain of peer
         isProxy - Boolean, True if chain ends with proxy
         isLimitedProxy - Boolean, True if chain ends with limited proxy
         group - String, DIRAC group for this peer, if known

      Returns a dict of details.
  """
  chain = X509Chain.generateX509ChainFromSSLConnection(conn)
  creds = chain.getCredentials(withRegistryInfo=False)
  if not creds['OK']:
    raise RuntimeError("Failed to get SSL peer info (%s)." % creds['Message'])
  peer = creds['Value']

  peer['x509Chain'] = chain
  isProxy = chain.isProxy()
  if not isProxy['OK']:
    raise RuntimeError("Failed to get SSL peer isProxy (%s)." % isProxy['Message'])
  peer['isProxy'] = isProxy['Value']

  if peer['isProxy']:
    peer['DN'] = creds['Value']['identity']
  else:
    peer['DN'] = creds['Value']['subject']

  isLimited = chain.isLimitedProxy()
  if not isLimited['OK']:
    raise RuntimeError("Failed to get SSL peer isProxy (%s)." % isLimited['Message'])
  peer['isLimitedProxy'] = isLimited['Value']

  return peer
