#!/usr/bin/env python
"""
Utilities for using M2Crypto SSL with DIRAC.
"""
import os
import tempfile
import M2Crypto
from packaging.version import Version
from M2Crypto import SSL, m2, X509


from DIRAC.Core.DISET import DEFAULT_SSL_CIPHERS, DEFAULT_SSL_METHODS
from DIRAC.Core.Security import Locations
from DIRAC.Core.Security.m2crypto.X509Chain import X509Chain

# Verify depth of peer certs
VERIFY_DEPTH = 50
DEBUG_M2CRYPTO = os.getenv("DIRAC_DEBUG_M2CRYPTO", "No").lower() in ("yes", "true")


VERIFY_ALLOW_PROXY_CERTS = 0

# If the version of M2Crypto is recent enough, there is an API
# to accept proxy certificate, and we do not need to rely on
# OPENSSL_ALLOW_PROXY_CERT environment variable
# which was removed as of openssl 1.1
# We need this to be merged in M2Crypto: https://gitlab.com/m2crypto/m2crypto/merge_requests/236
# We set the proper verify flag to the X509Store of the context
# as described here https://www.openssl.org/docs/man1.1.1/man7/proxy-certificates.html
if hasattr(SSL, "verify_allow_proxy_certs"):
    VERIFY_ALLOW_PROXY_CERTS = SSL.verify_allow_proxy_certs  # pylint: disable=no-member
# As of M2Crypto 0.37, the `verify_allow_proxy_certs` flag was moved
# to X509 (https://gitlab.com/m2crypto/m2crypto/-/merge_requests/238)
# It is more consistent with all the other flags,
# but pySSL had it in SSL. Well...
elif hasattr(X509, "verify_allow_proxy_certs"):
    VERIFY_ALLOW_PROXY_CERTS = X509.verify_allow_proxy_certs  # pylint: disable=no-member
# As of M2Crypto 0.38, M2Crypto did not export the symbol correctly
# Anymore
# https://gitlab.com/m2crypto/m2crypto/-/issues/298
elif Version(M2Crypto.__version__) >= Version("0.38.0"):
    VERIFY_ALLOW_PROXY_CERTS = 64


def __loadM2SSLCTXHostcert(ctx):
    """Load hostcert & key from the default location and set them as the
    credentials for SSL context ctx.
    Returns None.
    """
    certKeyTuple = Locations.getHostCertificateAndKeyLocation()
    if not certKeyTuple:
        raise RuntimeError("Hostcert/key location not set")
    hostcert, hostkey = certKeyTuple
    if not os.path.isfile(hostcert):
        raise RuntimeError(f"Hostcert file ({hostcert}) is missing")
    if not os.path.isfile(hostkey):
        raise RuntimeError(f"Hostkey file ({hostkey}) is missing")
    # Make sure we never stall on a password prompt if the hostkey has a password
    # by specifying a blank string.
    ctx.load_cert(hostcert, hostkey, callback=lambda: "")


def __loadM2SSLCTXProxy(ctx, proxyPath=None):
    """Load proxy from proxyPath (or default location if not specified) and
    set it as the certificate & key to use for this SSL context.
    Returns None.
    """
    if not proxyPath:
        proxyPath = Locations.getProxyLocation()
    if not proxyPath:
        raise RuntimeError("Proxy location not set")
    if not os.path.isfile(proxyPath):
        raise RuntimeError(f"Proxy file ({proxyPath}) is missing")
    # See __loadM2SSLCTXHostcert for description of why lambda is needed.
    ctx.load_cert_chain(proxyPath, proxyPath, callback=lambda: "")


def ssl_verify_callback_print_error(ok, store):
    """This callback method does nothing but printing the error.
    It prints a few more useful info than the exception

    :param ok: current validation status
    :param store: pointer to the X509_CONTEXT_STORE
    """
    errnum = store.get_error()
    if errnum:
        print(f"SSL DEBUG ERRNUM {errnum} ERRMSG {m2.x509_get_verify_error(errnum)}")  # pylint: disable=no-member
    return ok


def getM2SSLContext(ctx=None, **kwargs):
    """Gets an M2Crypto.SSL.Context configured using the standard
    DIRAC connection keywords from kwargs. The keywords are:

      - clientMode: Boolean, if False hostcerts are always used. If True
                             a proxy is used unless other flags are set.
      - useCertificates: Boolean, Set to true to use hostcerts in client
                         mode.
      - proxyString: String, allow a literal proxy string to be provided.
      - proxyLocation: String, Path to file to use as proxy, defaults to
                               usual location(s) if not set.
      - skipCACheck: Boolean, if True, don't verify peer certificates.
      - sslMethods: String, List of SSL algorithms to enable in OpenSSL style
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
    if kwargs.get("bServerMode", False) or (
        kwargs.get("useCertificates", False) and not kwargs.get("proxyLocation", False)
    ):
        # Server mode always uses hostcert
        __loadM2SSLCTXHostcert(ctx)

    else:
        # Client mode has a choice of possible options
        if kwargs.get("proxyString", None):
            # M2Crypto cannot take an inmemory location or a string, so
            # so write it to a temp file and use proxyLocation
            with tempfile.NamedTemporaryFile(mode="w") as tmpFile:
                tmpFilePath = tmpFile.name
                tmpFile.write(kwargs["proxyString"])
                # Flush, otherwise the file is empty in the subsequent call
                tmpFile.flush()
                __loadM2SSLCTXProxy(ctx, proxyPath=tmpFilePath)
        else:
            # Use normal proxy
            __loadM2SSLCTXProxy(ctx, proxyPath=kwargs.get("proxyLocation", None))

    verify_callback = ssl_verify_callback_print_error if DEBUG_M2CRYPTO else None

    # Set peer verification
    if kwargs.get("skipCACheck", False):
        # Don't validate peer, but still request creds
        ctx.set_verify(SSL.verify_none, VERIFY_DEPTH, callback=verify_callback)
    else:
        # Do validate peer
        ctx.set_verify(SSL.verify_peer | SSL.verify_fail_if_no_peer_cert, VERIFY_DEPTH, callback=verify_callback)
        # Set CA location
        caPath = Locations.getCAsLocation()
        if not caPath:
            raise RuntimeError("Failed to find CA location")
        if not os.path.isdir(caPath):
            raise RuntimeError(f"CA path ({caPath}) is not a valid directory")
        ctx.load_verify_locations(capath=caPath)

    # Allow proxy certificates to be used
    if VERIFY_ALLOW_PROXY_CERTS:
        ctx.get_cert_store().set_flags(VERIFY_ALLOW_PROXY_CERTS)

    # Other parameters
    sslMethods = kwargs.get("sslMethods", DEFAULT_SSL_METHODS)
    if sslMethods:
        # Pylint can't see the m2 constants due to the way the library is loaded
        # We just have to disable that warning for the next bit...
        # pylint: disable=no-member
        methods = [("SSLv2", m2.SSL_OP_NO_SSLv2), ("SSLv3", m2.SSL_OP_NO_SSLv3), ("TLSv1", m2.SSL_OP_NO_TLSv1)]
        allowed_methods = sslMethods.split(":")
        # If a method isn't explicitly allowed, set the flag to disable it...
        for method, method_flag in methods:
            if method not in allowed_methods:
                ctx.set_options(method_flag)
        # SSL_OP_NO_SSLv2, SSL_OP_NO_SSLv3, SSL_OP_NO_TLSv1
    ciphers = kwargs.get("sslCiphers", DEFAULT_SSL_CIPHERS)
    ctx.set_cipher_list(ciphers)

    # log the debug messages
    if DEBUG_M2CRYPTO:
        ctx.set_info_callback()

    return ctx


def getM2PeerInfo(conn):
    """Gets the details of the current peer as a standard dict. The peer
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
    if not creds["OK"]:
        raise RuntimeError(f"Failed to get SSL peer info ({creds['Message']}).")
    peer = creds["Value"]

    peer["x509Chain"] = chain
    isProxy = chain.isProxy()
    if not isProxy["OK"]:
        raise RuntimeError(f"Failed to get SSL peer isProxy ({isProxy['Message']}).")
    peer["isProxy"] = isProxy["Value"]

    if peer["isProxy"]:
        peer["DN"] = creds["Value"]["identity"]
    else:
        peer["DN"] = creds["Value"]["subject"]

    isLimited = chain.isLimitedProxy()
    if not isLimited["OK"]:
        raise RuntimeError(f"Failed to get SSL peer isProxy ({isLimited['Message']}).")
    peer["isLimitedProxy"] = isLimited["Value"]

    return peer
