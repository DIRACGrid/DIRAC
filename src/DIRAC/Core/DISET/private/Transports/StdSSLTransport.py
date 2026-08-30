"""SSL Transport implementation using the standard library ssl module.

The peer chain validation (including RFC 3820 proxy support) is delegated to
OpenSSL through ``ssl.SSLContext``:

* proxy certificates are accepted by setting the ``X509_V_FLAG_ALLOW_PROXY_CERTS``
  verify flag (``ssl.VERIFY_ALLOW_PROXY_CERTS``)
* the peer chain is retrieved after the handshake with
  ``ssl.SSLSocket.get_unverified_chain`` (python >= 3.13), and the DIRAC
  credentials (DN, group, ...) are extracted from it with
  :py:class:`DIRAC.Core.Security.X509Chain.X509Chain`
"""
import os
import socket
import ssl
import tempfile
import threading
import time
from collections import OrderedDict

from DIRAC import gLogger
from DIRAC.Core.DISET import DEFAULT_CONNECTION_TIMEOUT, DEFAULT_RPC_TIMEOUT
from DIRAC.Core.DISET.private.Transports.BaseTransport import BaseTransport
from DIRAC.Core.Security import Locations

# The pyca based X509Chain is imported through its fully qualified path (like
# M2Utils does with the m2crypto one): getPeerInfo relies on
# generateX509ChainFromDERList, which only the pyca implementation provides
from DIRAC.Core.Security.pyca.X509Chain import X509Chain
from DIRAC.Core.Utilities.ReturnValues import S_ERROR, S_OK

# Verify depth of peer certs
VERIFY_DEPTH = 50

# Re-exported for convenience (exists since python 3.10)
VERIFY_ALLOW_PROXY_CERTS = ssl.VERIFY_ALLOW_PROXY_CERTS


def __loadHostCertificate(ctx):
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
    ctx.load_cert_chain(hostcert, keyfile=hostkey)


def __loadProxy(ctx, proxyPath=None):
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
    # A proxy file contains the leaf certificate, its key, and the rest
    # of the chain in a single file, which load_cert_chain handles natively
    ctx.load_cert_chain(proxyPath)


def getSSLContext(**kwargs):
    """Gets an ssl.SSLContext configured using the standard
    DIRAC connection keywords from kwargs. The keywords are:

      - bServerMode: Boolean, if True the context is setup for a server
                     (hostcert is always used).
      - useCertificates: Boolean, Set to true to use hostcerts in client
                         mode.
      - proxyString: String, allow a literal proxy string to be provided.
      - proxyLocation: String, Path to file to use as proxy, defaults to
                               usual location(s) if not set.
      - skipCACheck: Boolean, if True, don't verify peer certificates.
      - optionalClientCert: Boolean, in server mode, request but do not require
                            the client certificate (used by the HTTPS services,
                            where token/visitor authentication also exists).
      - sslCiphers: String, OpenSSL style cipher string of ciphers to allow
                            on this connection.

    Returns the new context.
    """
    serverMode = kwargs.get("bServerMode", False)

    if serverMode:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    else:
        # PROTOCOL_TLS_CLIENT enables check_hostname and CERT_REQUIRED by default
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2

    # Set certificates for connection
    if serverMode or (kwargs.get("useCertificates", False) and not kwargs.get("proxyLocation", False)):
        # Server mode always uses hostcert
        __loadHostCertificate(ctx)

    else:
        # Client mode has a choice of possible options
        if kwargs.get("proxyString", None):
            # ssl cannot take an in-memory location or a string,
            # so write it to a temp file and use proxyLocation
            with tempfile.NamedTemporaryFile(mode="w") as tmpFile:
                tmpFile.write(kwargs["proxyString"])
                # Flush, otherwise the file is empty in the subsequent call
                tmpFile.flush()
                __loadProxy(ctx, proxyPath=tmpFile.name)
        else:
            # Use normal proxy
            __loadProxy(ctx, proxyPath=kwargs.get("proxyLocation", None))

    # Set peer verification
    if kwargs.get("skipCACheck", False):
        # Don't validate peer
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    else:
        # Do validate peer
        if serverMode and kwargs.get("optionalClientCert", False):
            ctx.verify_mode = ssl.CERT_OPTIONAL
        else:
            ctx.verify_mode = ssl.CERT_REQUIRED
        # Allow proxy certificates to be used
        ctx.verify_flags |= VERIFY_ALLOW_PROXY_CERTS
        # Set CA location
        caPath = Locations.getCAsLocation()
        if not caPath:
            raise RuntimeError("Failed to find CA location")
        if not os.path.isdir(caPath):
            raise RuntimeError(f"CA path ({caPath}) is not a valid directory")
        ctx.load_verify_locations(capath=caPath)

    # DIRAC_M2CRYPTO_SSL_CIPHERS is accepted for backward compatibility
    ciphers = (
        kwargs.get("sslCiphers") or os.environ.get("DIRAC_SSL_CIPHERS") or os.environ.get("DIRAC_M2CRYPTO_SSL_CIPHERS")
    )
    if ciphers:
        ctx.set_ciphers(ciphers)

    return ctx


# Client-side SSL contexts are cached and shared between connections, as
# building one means reading and parsing the credential files for every RPC.
# An entry is invalidated when the credential file changes (e.g. proxy
# renewal), and expires after _CLIENT_CTX_CACHE_TTL seconds so that changes
# in the CA directory are eventually picked up.
_CLIENT_CTX_CACHE: OrderedDict = OrderedDict()
_CLIENT_CTX_CACHE_LOCK = threading.Lock()
_CLIENT_CTX_CACHE_MAX = 8
_CLIENT_CTX_CACHE_TTL = 300


def _clientContextCacheKey(kwargs):
    """Compute the cache key for a client SSL context, or None if the
    configuration is not cacheable. The credential selection mirrors
    :py:func:`getSSLContext`, and the key contains the stat info of the
    credential files so the cache is invalidated when they change.

    :param kwargs: the connection keywords, as given to getSSLContext
    """
    if kwargs.get("bServerMode") or kwargs.get("proxyString"):
        return None
    if kwargs.get("useCertificates", False) and not kwargs.get("proxyLocation", False):
        credentialFiles = Locations.getHostCertificateAndKeyLocation()
        if not credentialFiles:
            return None
    else:
        proxyPath = kwargs.get("proxyLocation") or Locations.getProxyLocation()
        if not proxyPath:
            return None
        credentialFiles = (proxyPath,)

    fileStats = []
    try:
        for path in credentialFiles:
            st = os.stat(path)
            fileStats.append((path, st.st_mtime_ns, st.st_size, st.st_ino))
    except OSError:
        return None

    skipCACheck = bool(kwargs.get("skipCACheck", False))
    caPath = None if skipCACheck else Locations.getCAsLocation()
    ciphers = (
        kwargs.get("sslCiphers") or os.environ.get("DIRAC_SSL_CIPHERS") or os.environ.get("DIRAC_M2CRYPTO_SSL_CIPHERS")
    )
    return (tuple(fileStats), skipCACheck, caPath, ciphers)


def _getClientSSLContext(**kwargs):
    """Return a client SSL context, shared from the cache whenever possible
    (ssl.SSLContext objects are safe to share between connections and threads)
    """
    cacheKey = _clientContextCacheKey(kwargs)
    if cacheKey is None:
        return getSSLContext(**kwargs)

    now = time.monotonic()
    with _CLIENT_CTX_CACHE_LOCK:
        cached = _CLIENT_CTX_CACHE.get(cacheKey)
        if cached and now - cached[0] < _CLIENT_CTX_CACHE_TTL:
            _CLIENT_CTX_CACHE.move_to_end(cacheKey)
            return cached[1]

    ctx = getSSLContext(**kwargs)

    with _CLIENT_CTX_CACHE_LOCK:
        _CLIENT_CTX_CACHE[cacheKey] = (now, ctx)
        _CLIENT_CTX_CACHE.move_to_end(cacheKey)
        while len(_CLIENT_CTX_CACHE) > _CLIENT_CTX_CACHE_MAX:
            _CLIENT_CTX_CACHE.popitem(last=False)
    return ctx


def getPeerInfo(sslSocket):
    """Gets the details of the current peer as a standard dict. The peer
    details are obtained from the supplied ssl.SSLSocket.
    The details returned are those from ~X509Chain.getCredentials, without Registry info:

       DN - Full peer DN as string
       x509Chain - Full chain of peer
       isProxy - Boolean, True if chain ends with proxy
       isLimitedProxy - Boolean, True if chain ends with limited proxy
       group - String, DIRAC group for this peer, if known

    Returns a dict of details.
    """
    if not hasattr(sslSocket, "get_unverified_chain"):
        raise RuntimeError("Extracting the peer certificate chain requires python >= 3.13 (ssl get_unverified_chain)")
    # The chain, as sent by the peer (leaf first). It was already validated
    # by OpenSSL during the handshake, and the proxy-ness of it is
    # anyway cryptographically re-checked by X509Chain
    derChain = sslSocket.get_unverified_chain()
    if not derChain:
        return {}
    chain = X509Chain.generateX509ChainFromDERList(derChain)
    creds = chain.getCredentials(withRegistryInfo=False)
    if not creds["OK"]:
        raise RuntimeError(f"Failed to get SSL peer info ({creds['Message']}).")
    peer = creds["Value"]

    # getCredentials already resolves DN (identity for proxies, subject
    # otherwise), isProxy and isLimitedProxy
    peer["x509Chain"] = chain

    return peer


class SSLTransport(BaseTransport):
    """SSL Transport implementation based on the standard library ssl module."""

    # This name is the same as BaseClient,
    # and is used a bit everywhere, so it should be factorized out
    # eventually
    KW_TIMEOUT = "timeout"

    def __init__(self, *args, **kwargs):
        """Create an SSLTransport object, parameters are the same
        as for other transports. If ctx is specified (as an instance of
        ssl.SSLContext) then use that rather than creating a new context.

        kwargs can contain all the parameters defined in BaseClient,
        in particular timeout
        """
        self.remoteAddress = None
        self.peerCredentials = {}

        # The timeout is to be understood here as the timeout for socket
        # operations involved in the RPC call, but NOT the establishment
        # of the connection, for which there is a different timeout
        # (DEFAULT_CONNECTION_TIMEOUT)
        self.__timeout = kwargs.get(SSLTransport.KW_TIMEOUT, DEFAULT_RPC_TIMEOUT)

        self.__locked = False  # We don't support locking, so this is always false.

        self.__ctx = kwargs.pop("ctx", None)
        if not self.__ctx:
            if kwargs.get("bServerMode", False):
                self.__ctx = getSSLContext(**kwargs)
            else:
                self.__ctx = _getClientSSLContext(**kwargs)

        # Note that kwargs is already kept in BaseTransport
        # as self.extraArgsDict, but at least I am sure that
        # self.__kwargs will never be modified
        self.__kwargs = kwargs

        BaseTransport.__init__(self, *args, **kwargs)

    def setSocketTimeout(self, timeout):
        """Set the timeout for RPC calls.

        .. warning: This needs to be called before initAsClient.
          It is used as a timeout for RPC calls, not connection.

        :param timeout: timeout for socket operation in seconds

        """
        self.__timeout = timeout

    def initAsClient(self):
        """Prepare this client socket for use."""
        if self.serverMode():
            raise RuntimeError("SSLTransport is in server mode.")

        errors = []
        host, port = self.stServerAddress

        # Get all available addresses (IPv6 and IPv4) and try them in order
        try:
            addrInfoList = socket.getaddrinfo(host, port, socket.AF_UNSPEC, socket.SOCK_STREAM)
        except OSError as e:
            return S_ERROR(f"DNS lookup failed {e!r}")
        for family, socketType, proto, _canonname, socketAddress in addrInfoList:
            rawSocket = None
            try:
                rawSocket = socket.socket(family, socketType, proto)

                # First set a short connection timeout, that will be used
                # for the TCP connection and the TLS handshake
                rawSocket.settimeout(DEFAULT_CONNECTION_TIMEOUT)

                # Enable keepAlive, with default options
                # (see more comments about keepalive in :py:meth:`.acceptConnection`)
                rawSocket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, True)

                rawSocket.connect(socketAddress)

                # server_hostname takes care of both SNI and the hostname check
                self.oSocket = self.__ctx.wrap_socket(rawSocket, server_hostname=host)

                # Once the connection is established, we can use the timeout
                # asked for RPC
                self.oSocket.settimeout(self.__timeout)

                self.remoteAddress = self.oSocket.getpeername()

                return S_OK()
            # warning: do NOT catch SSL related errors here
            # They should be propagated upwards and caught by the BaseClient
            # not to enter the retry loop
            except ssl.SSLError:
                if rawSocket is not None:
                    rawSocket.close()
                raise
            except OSError as e:
                errors.append(f"{socketAddress} {e}:{repr(e)}")
                if self.oSocket is not None:
                    self.close()
                elif rawSocket is not None:
                    rawSocket.close()

        return S_ERROR("; ".join(errors))

    def initAsServer(self):
        """Prepare this server socket for use.

        Contrary to the client mode, the listening socket is a plain TCP
        socket: the TLS layer is only added on the accepted connections,
        in :py:meth:`.handshake`
        """
        if not self.serverMode():
            raise RuntimeError("SSLTransport is in client mode.")

        try:
            self.oSocket = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
        except OSError:
            # Maybe no IPv6 support? Try IPv4 only socket.
            self.oSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # Make sure reuse address is set correctly
        param = 1 if self.bAllowReuseAddress else 0
        self.oSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, param)

        self.oSocket.bind(self.stServerAddress)
        self.oSocket.listen(self.iListenQueueSize)
        return S_OK()

    def close(self):
        """Close this socket."""
        if self.oSocket:
            try:
                self.oSocket.shutdown(socket.SHUT_RDWR)
            except OSError:
                # The socket may already be in a closed state
                pass
            self.oSocket.close()
            self.oSocket = None
        return S_OK()

    def renewServerContext(self):
        """Renews the server context.
        This reloads the certificates and the CA store by re-creating
        the SSL context, which will be used for the subsequent connections.
        """
        if not self.serverMode():
            raise RuntimeError("SSLTransport is in client mode.")
        super().renewServerContext()
        try:
            self.__ctx = getSSLContext(**self.__kwargs)
        except Exception as e:
            gLogger.error("Failed to renew the server context", repr(e))
            return S_ERROR(f"Failed to renew the server context: {e!r}")

        return S_OK()

    def handshake(self):
        """Perform the SSL handshake.
        This has to be called after the connection was accepted (acceptConnection)

        The remote credentials are gathered here
        """
        try:
            # Warning: this method is called on the object returned
            # by acceptConnection, which shares the context of the
            # listening object
            self.oSocket = self.__ctx.wrap_socket(self.oSocket, server_side=True)

            self.peerCredentials = getPeerInfo(self.oSocket)

            # Now that the handshake has been performed on the server
            # we can set the timeout for the RPC operations.
            # In practice, since we are on the server side, the
            # timeout we set here represents the timeout for receiving
            # the arguments and sending back the response. This should
            # in principle be reasonably quick, but just to be sure
            # we can set it to the DEFAULT_RPC_TIMEOUT
            self.oSocket.settimeout(DEFAULT_RPC_TIMEOUT)

            return S_OK()
        except (OSError, RuntimeError) as e:
            return S_ERROR(f"Error in handshake: {e} {repr(e)}")

    def setClientSocket(self, oSocket):
        """Set the inner socket of this instance to the value of oSocket.
        This method is intended to be used to create client connection objects
        from a server and should be considered to be an internal function.

        :param oSocket: client socket (plain, the handshake is done in :py:meth:`.handshake`)

        """
        # warning: do NOT catch socket.error here, because for who knows what reason
        # exceptions are actually properly used for once, and the calling method
        # relies on it (ServiceReactor.__acceptIncomingConnection)
        self.oSocket = oSocket
        self.remoteAddress = self.oSocket.getpeername()

    def acceptConnection(self):
        """Accept a new client, returns a new SSLTransport object representing
        the client connection.

        The connection is accepted, but no SSL handshake is performed

        :returns: S_OK(SSLTransport object)
        """
        try:
            oClient, _ = self.oSocket.accept()

            # Set the keep alive to true. This keepalive will ensure that we
            # detect remote peer crashing or network interruption
            # Note that this is ineffective if we are in the middle of blocking
            # operations.
            oClient.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, True)

            # Here we set the timeout server side.
            # We first set the connection timeout, which will
            # be effective for the TLS handshake
            oClient.settimeout(DEFAULT_CONNECTION_TIMEOUT)

            oClientTrans = SSLTransport(self.stServerAddress, ctx=self.__ctx)
            oClientTrans.setClientSocket(oClient)
            return S_OK(oClientTrans)
        except (OSError, RuntimeError) as e:
            return S_ERROR(f"Error in acceptConnection: {e} {repr(e)}")

    def _read(self, bufSize=4096, skipReadyCheck=False):
        """Read bufSize bytes from the buffer.

        :param bufSize: size of the buffer to read
        :param skipReadyCheck: ignored.


        :returns: S_OK(data read)
        """
        try:
            read = self.oSocket.recv(bufSize)
            return S_OK(read)
        except OSError as e:
            return S_ERROR(f"Error in _read: {e} {repr(e)}")

    def isLocked(self):
        """Returns if this instance is locked.
        Always returns false.

        :returns: False
        """
        return self.__locked

    def _write(self, buf):
        """Write all bytes contained within iterable "buf" to the
        connected peer.

        :param buf: iterable buffer

        :returns: S_OK(number of bytes written)
        """
        try:
            wrote = self.oSocket.send(buf)
            return S_OK(wrote)
        except OSError as e:
            return S_ERROR(f"Error in _write: {e} {repr(e)}")
