#!/usr/bin/env python
"""
M2Crypto SSLTransport Library
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import os
import socket
from M2Crypto import SSL, threading as M2Threading
from M2Crypto.SSL.Checker import SSLVerificationError

from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
from DIRAC.Core.DISET.private.Transports.BaseTransport import BaseTransport
from DIRAC.Core.DISET.private.Transports.SSL.M2Utils import getM2SSLContext, getM2PeerInfo

from DIRAC.Core.DISET import DEFAULT_CONNECTION_TIMEOUT, DEFAULT_RPC_TIMEOUT

# TODO: For now we have to set an environment variable for proxy support in OpenSSL
# Eventually we may need to add API support for this to M2Crypto...
os.environ['OPENSSL_ALLOW_PROXY_CERTS'] = '1'
M2Threading.init()

# TODO: CRL checking should be implemented but this will require support adding
# to M2Crypto: Quite a few functions will need mapping through from OpenSSL to
# allow the CRL stack to be set on the X509 CTX used for verification.

# TODO: Log useful messages to the logger


class SSLTransport(BaseTransport):
  """ SSL Transport implementation using the M2Crypto library. """

  # This name is the same as BaseClient,
  # and is used a bit everywhere, so it should be factorized out
  # eventually
  KW_TIMEOUT = 'timeout'

  def __getConnection(self):
    """ Helper function to get a connection object,
        Tries IPv6 (AF_INET6) first, then falls back to IPv4 (AF_INET).
    """
    try:
      conn = SSL.Connection(self.__ctx, family=socket.AF_INET6)
    except socket.error:
      # Maybe no IPv6 support? Try IPv4 only socket.
      conn = SSL.Connection(self.__ctx, family=socket.AF_INET)
    return conn

  def __init__(self, *args, **kwargs):
    """ Create an SSLTransport object, parameters are the same
        as for other transports. If ctx is specified (as an instance of
        SSL.Context) then use that rather than creating a new context.

        kwargs can contain all the parameters defined in BaseClient,
        in particular timeout
    """
    # The thread init of M2Crypto is not really thread safe.
    # So we put it a second time
    M2Threading.init()
    self.remoteAddress = None
    self.peerCredentials = {}

    # The timeout used here is different from what it was in pyGSI.
    # It is to be understood here as the timeout for socket operations
    # involved in the RPC call, but NOT the establishment of the connection,
    # for which there is a different timeout.
    #
    # The timeout management of pyGSI was a bit off.
    # This is proven by that type of trace (look at the timestamp):
    #
    # 2020-07-16 09:48:55 UTC dirac-proxy-init [140013698656064] DEBUG: Connection timeout set to:  1
    # 2020-07-16 09:58:55 UTC dirac-proxy-init [140013698656064] WARN: Issue getting socket:
    #

    self.__timeout = kwargs.get(SSLTransport.KW_TIMEOUT, DEFAULT_RPC_TIMEOUT)

    self.__locked = False  # We don't support locking, so this is always false.

    # If not specified in the arguments (never is in DIRAC code...)
    # and we are setting up a server listing connection, set the accepted
    # ssl methods and ciphers
    if kwargs.get('bServerMode'):
      if 'sslMethods' not in kwargs:
        kwargs['sslMethods'] = os.environ.get('DIRAC_M2CRYPTO_SSL_METHODS')
      if 'sslCiphers' not in kwargs:
        kwargs['sslCiphers'] = os.environ.get('DIRAC_M2CRYPTO_SSL_CIPHERS')

    self.__ctx = kwargs.pop('ctx', None)
    if not self.__ctx:
      self.__ctx = getM2SSLContext(**kwargs)

    # Note that kwargs is already kept in BaseTransport
    # as self.extraArgsDict, but at least I am sure that
    # self.__kwargs will never be modified
    self.__kwargs = kwargs

    BaseTransport.__init__(self, *args, **kwargs)

  def setSocketTimeout(self, timeout):
    """ Set the timeout for RPC calls.

        .. warning: This needs to be called before initAsClient.
          It is used as a timeout for RPC calls, not connection.

        :param timeout: timeout for socket operation in seconds

    """
    self.__timeout = timeout

  def initAsClient(self):
    """ Prepare this client socket for use. """
    if self.serverMode():
      raise RuntimeError("SSLTransport is in server mode.")

    error = None
    host, port = self.stServerAddress

    # The following piece of code was inspired by the python socket documentation
    # as well as the implementation of M2Crypto.httpslib.HTTPSConnection

    # We ignore the returned sockaddr because SSL.Connection.connect needs
    # a host name.
    addrInfoList = socket.getaddrinfo(host, port, socket.AF_UNSPEC,
                                      socket.SOCK_STREAM)
    for (family, _socketType, _proto, _canonname, _socketAddress) in \
            addrInfoList:

      try:
        self.oSocket = SSL.Connection(self.__ctx, family=family)

        # First set a short connection timeout, that will trigger
        # during blocking operations (read/write) use to
        # establish the SSL connection
        self.oSocket.settimeout(DEFAULT_CONNECTION_TIMEOUT)

        # Enable keepAlive, with default options
        # (see more comments about keepalive in :py:meth:`.acceptConnection`)
        self.oSocket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, True)

        # set SNI server name since we know it at this point
        self.oSocket.set_tlsext_host_name(host)

        self.oSocket.connect((host, port))

        # Once the connection is established, we can use the timeout
        # asked for RPC
        self.oSocket.settimeout(self.__timeout)

        self.remoteAddress = self.oSocket.getpeername()

        return S_OK()
      # warning: do NOT catch SSL related error here
      # They should be propagated upwards and caught by the BaseClient
      # not to enter the retry loop
      except socket.error as e:
        error = "%s:%s" % (e, repr(e))

        if self.oSocket is not None:
          self.close()

    return S_ERROR(error)

  def initAsServer(self):
    """ Prepare this server socket for use. """
    if not self.serverMode():
      raise RuntimeError("SSLTransport is in client mode.")
    # Before getting the connection object, we need to set
    # a server session ID in the context
    host = self.stServerAddress[0]
    port = self.stServerAddress[1]
    self.__ctx.set_session_id_ctx(("DIRAC-%s-%s" % (host, port)).encode())
    self.oSocket = self.__getConnection()
    # Make sure reuse address is set correctly
    if self.bAllowReuseAddress:
      param = 1
    else:
      param = 0

    self.oSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, param)

    self.oSocket.bind(self.stServerAddress)
    self.oSocket.listen(self.iListenQueueSize)
    return S_OK()

  def close(self):
    # pylint: disable=line-too-long
    """ Close this socket. """

    if self.oSocket:
      # TL;DR:
      # Do NOT touch that method
      #
      # Surprisingly (to me at least), M2Crypto does not close
      # the underlying socket when calling SSL.Connection.close
      # It only does it when the garbage collector kicks in (see ~M2Crypto.SSL.Connection.Connection.__del__)
      # If the socket is not closed, the connection may hang forever.
      #
      # Thus, we are setting self.oSocket to None to allow the GC to do the work, but since we are not sure
      # that it will run, we anyway force the connection to be closed
      #
      # However, we should close the underlying socket only after SSL was shutdown properly.
      # This is because OpenSSL `ssl3_shutdown` (see callstack below) may still read some data
      # (see https://github.com/openssl/openssl/blob/master/ssl/s3_lib.c#L4509)::
      #
      #
      # 1  0x00007fffe9d48fc0 in sock_read () from /lib/libcrypto.so.1.0.0
      # 2  0x00007fffe9d46e83 in BIO_read () from /lib/libcrypto.so.1.0.0
      # 3  0x00007fffe9eab9dd in ssl3_read_n () from /lib/libssl.so.1.0.0
      # 4  0x00007fffe9ead216 in ssl3_read_bytes () from /lib/libssl.so.1.0.0
      # 5  0x00007fffe9ea999c in ssl3_shutdown () from /lib/libssl.so.1.0.0
      # 6  0x00007fffe9ed4f93 in ssl_free () from /lib/libssl.so.1.0.0
      # 7  0x00007fffe9d46d5b in BIO_free () from /lib/libcrypto.so.1.0.0
      # 8  0x00007fffe9f30a96 in bio_free (bio=0x5555556f3200) at SWIG/_m2crypto_wrap.c:5008
      # 9  0x00007fffe9f30b1e in _wrap_bio_free (self=<optimized out>, args=<optimized out>) at SWIG/_m2crypto_wrap.c
      #
      # We unfortunately have no way to force that order, and there is a risk of deadlock
      # when running in a multi threaded environment like the agents::
      #
      # Thread A opens socket, gets FD = 111
      # Thread A works on it
      # Thread A closes FD 111 (underlying socket.close())
      # Thread B opens socket, gets FD = 111
      # Thread A calls read on FD=111 from ssl3_shutdown
      #
      # This is illustrated on the strace below::
      #
      # 26461 14:25:15.266692 write(111<TCPv6:[[<srcAddressV6>]:42688->[<dstAddressV6>]:9140]>,
      #                            "blabla", 37 <unfinished ...>
      # 26464 14:25:15.266857 <... connect resumed>) = 0 <0.000195>
      # 26464 14:25:15.267023 getsockname(120<UDP:[<srcAddress>:44252->188.185.84.86:9140]>, <unfinished ...>
      # 26461 14:25:15.267176 <... write resumed>) = 37 <0.000453>
      # 26464 14:25:15.267425 <... getsockname resumed>{sa_family=AF_INET, sin_port=htons(44252),
      #                        sin_addr=inet_addr("<srcAddress>")}, [28->16]) = 0 <0.000292>
      # 26461 14:25:15.267466 close(111<TCPv6:[[<srcAddressV6>]:42688->[<dstAddressV6>]:9140]> <unfinished ...>
      # 26464 14:25:15.267637 close(120<UDP:[<srcAddress>:44252->188.185.84.86:9140]> <unfinished ...>
      # 26464 14:25:15.267738 <... close resumed>) = 0 <0.000086>
      # 26461 14:25:15.267768 <... close resumed>) = 0 <0.000285>
      # 26464 14:25:15.267827 socket(AF_INET6, SOCK_DGRAM|SOCK_CLOEXEC, IPPROTO_IP <unfinished ...>
      # 26461 14:25:15.267888 futex(0x21f8620, FUTEX_WAKE_PRIVATE, 1 <unfinished ...>
      # 26464 14:25:15.267976 <... socket resumed>) = 111<UDPv6:[1207802822]> <0.000138>
      # 26461 14:25:15.268092 <... futex resumed>) = 1 <0.000196>
      # 26464 14:25:15.268195 connect(111<UDPv6:[1207802822]>,
      #                      {sa_family=AF_INET6, sin6_port=htons(9140),
      #                       inet_pton(AF_INET6, "<dstAddressV6>", &sin6_addr),
      #                      sin6_flowinfo=htonl(0), sin6_scope_id=0
      #                      }, 28 <unfinished ...>
      # 26461 14:25:15.268294 read(111<UDPv6:[[<srcAddressV6>]:42480->[<dstAddressV6>]:9140]>, <unfinished ...>
      # 26464 14:25:15.268503 <... connect resumed>) = 0 <0.000217>
      # 26464 14:25:15.268673 getsockname(111<UDPv6:[[<srcAddressV6>]:42480->[<dstAddressV6>]:9140]>, <unfinished ...>
      # 26464 14:25:15.268862 <... getsockname resumed>{sa_family=AF_INET6, sin6_port=htons(42480),
      #                        inet_pton(AF_INET6, "<srcAddressV6>", &sin6_addr), sin6_flowinfo=htonl(0), sin6_scope_id=
      # 0}, [28]) = 0 <0.000168>
      # 26464 14:25:15.269048
      # close(111<UDPv6:[[<srcAddressV6>]:42480->[<dstAddressV6>]:9140]>
      # <unfinished ...>
      #
      #
      # Update 16.07.20:
      # M2Crypto 0.36 contains the bug fix https://gitlab.com/m2crypto/m2crypto/-/merge_requests/247
      # that allows proper closing. So manual closing of the underlying socket should not be needed anymore

      # Update 16.07.20
      # I add this shutdown call without being 100% sure
      # it solves some hanging connections issues, but it seems
      # to work. it does not appear in any M2Crypto doc, but comparing
      # some internals of M2Crypto and official python SSL library,
      # it seems to make sense
      self.oSocket.shutdown(socket.SHUT_RDWR)

      # Update 16.07.20
      # With freeBio=True, we force the
      # closing of the socket before the GC runs
      self.oSocket.close(freeBio=True)
      # underlyingSocket = self.oSocket.socket
      self.oSocket = None
      # underlyingSocket.close()
    return S_OK()

  def renewServerContext(self):
    # pylint: disable=line-too-long
    """ Renews the server context.
        This reloads the certificates and re-initialises the SSL context.

        NOTE: Chris 15.05.20
        I noticed python segfault on a regular time interval. The stack trace always looks like that::

          #0  0x00007fdb5bbe2388 in ?? () from /opt/dirac/pro/diracos/usr/lib64/python2.7/lib-dynload/../../libcrypto.so.10
          #1  0x00007fdb5bbd8742 in X509_STORE_load_locations () from /opt/dirac/pro/diracos/usr/lib64/python2.7/lib-dynload/../../libcrypto.so.10
          #2  0x00007fdb57edcc9d in _wrap_ssl_ctx_load_verify_locations (self=<optimized out>, args=<optimized out>) at SWIG/_m2crypto_wrap.c:20602
          #3  0x00007fdb644ec484 in PyEval_EvalFrameEx () from /opt/dirac/versions/v10r0_1587978031/diracos/usr/bin/../lib64/libpython2.7.so.1.0

        I could not find anything fundamentaly wrong, and the context renewal is the only place I could think of.

        GSI based SSLTransport did the following: renew the context, and renew the Connection object using the same raw socket
        This still seems very fishy to me though, especially that the ServiceReactor still has the old object in self.__listeningConnections[svcName]['socket']]

        Here, we were are refreshing the CA store. What was missing was the call to the parent class, thus entering some sort of infinite loop.
        The parent's call seems to have fixed it.
    """  # noqa # pylint: disable=line-too-long
    if not self.serverMode():
      raise RuntimeError("SSLTransport is in client mode.")
    super(SSLTransport, self).renewServerContext()
    self.__ctx = getM2SSLContext(self.__ctx, **self.__kwargs)

    return S_OK()

  def handshake_singleStep(self):
    """ Used to perform SSL handshakes.
        These are now done automatically.
    """
    # This isn't used any more, the handshake is done inside the M2Crypto library
    return S_OK()

  def handshake_multipleSteps(self):
    """ Perform SSL handshakes.
        This has to be called after the connection was accepted (acceptConnection_multipleSteps)

        The remote credentials are gathered here
    """
    try:
      # M2Crypto does not provide public method to
      # accept and handshake in two steps.
      # So we have to do it manually
      # The following lines are basically a copy/paste
      # of the end of SSL.Connection.accept method
      self.oSocket.setup_ssl()
      self.oSocket.set_accept_state()
      self.oSocket.accept_ssl()
      check = getattr(self.oSocket, 'postConnectionCheck',
                      self.oSocket.serverPostConnectionCheck)
      if check is not None:
        if not check(self.oSocket.get_peer_cert(), self.oSocket.addr[0]):
          raise SSL.Checker.SSLVerificationError(
              'post connection check failed')

      self.peerCredentials = getM2PeerInfo(self.oSocket)

      # Now that the handshake has been performed on the server
      # we can set the timeout for the RPC operations.
      # In practice, since we are on the server side, the
      # timeout we set here represents the timeout for receiving
      # the arguments and sending back the response. This should
      # in principle be reasonably quick, but just to be sure
      # we can set it to the DEFAULT_RPC_TIMEOUT
      self.oSocket.settimeout(DEFAULT_RPC_TIMEOUT)

      return S_OK()
    except (socket.error, SSL.SSLError, SSLVerificationError) as e:
      return S_ERROR("Error in handhsake: %s %s" % (e, repr(e)))

  def setClientSocket_singleStep(self, oSocket):
    """ Set the inner socket (i.e. SSL.Connection object) of this instance
        to the value of oSocket.
        We also gather the remote peer credentials
        This method is intended to be used to create client connection objects
        from a server and should be considered to be an internal function.

        :param oSocket: client socket SSL.Connection object

    """

    # TODO: The calling method (ServiceReactor.__acceptIncomingConnection) expects
    # socket.error to be thrown in case of issue. Maybe we should catch the M2Crypto
    # errors here and raise socket.error instead

    self.oSocket = oSocket
    self.remoteAddress = self.oSocket.getpeername()
    self.peerCredentials = getM2PeerInfo(self.oSocket)

  def setClientSocket_multipleSteps(self, oSocket):
    """ Set the inner socket (i.e. SSL.Connection object) of this instance
        to the value of oSocket.
        This method is intended to be used to create client connection objects
        from a server and should be considered to be an internal function.

        :param oSocket: client socket SSL.Connection object

    """
    # warning: do NOT catch socket.error here, because for who knows what reason
    # exceptions are actually properly used for once, and the calling method
    # relies on it (ServiceReactor.__acceptIncomingConnection)
    self.oSocket = oSocket
    self.remoteAddress = self.oSocket.getpeername()

  def acceptConnection_multipleSteps(self):
    """ Accept a new client, returns a new SSLTransport object representing
        the client connection.

        The connection is accepted, but no SSL handshake is performed

        :returns: S_OK(SSLTransport object)
    """
    # M2Crypto does not provide public method to
    # accept and handshake in two steps.
    # So we have to do it manually
    # The following lines are basically a copy/paste
    # of the begining of SSL.Connection.accept method
    # with added options and timeout
    try:
      sock, addr = self.oSocket.socket.accept()
      oClient = SSL.Connection(self.oSocket.ctx, sock)

      # Set the keep alive to true. This keepalive will ensure that we
      # detect remote peer crashing or network interruption
      # Note that this is ineffective if we are in the middle of blocking
      # operations.
      oClient.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, True)

      # I am adding here for reference the code that would allow to change
      # the keepalive settings, although we should be fine with the default
      # (connection would be closed after 7200 + 9 * 75 ~= 2h and 10mn )

      # Duration between two keepalive probes, in seconds
      # oClient.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 75)
      # Number of consecutive bad probes to cut the connection
      # oClient.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 9)
      # Delay before the keepalive starts ticking, in seconds
      # oClient.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 7200)

      # Here we set the timeout server side.
      # We first set the connection timeout, which will
      # be effective for TLS handshake
      oClient.settimeout(DEFAULT_CONNECTION_TIMEOUT)

      oClient.addr = addr
      oClientTrans = SSLTransport(self.stServerAddress, ctx=self.__ctx)
      oClientTrans.setClientSocket(oClient)
      return S_OK(oClientTrans)
    except (socket.error, SSL.SSLError, SSLVerificationError) as e:
      return S_ERROR("Error in acceptConnection: %s %s" % (e, repr(e)))

  def acceptConnection_singleStep(self):
    """ Accept a new client, returns a new SSLTransport object representing
        the client connection.

        The SSL handshake is performed here.

        :returns: S_OK(SSLTransport object)
    """
    try:
      oClient, _ = self.oSocket.accept()
      oClientTrans = SSLTransport(self.stServerAddress, ctx=self.__ctx)
      oClientTrans.setClientSocket(oClient)
      return S_OK(oClientTrans)
    except (socket.error, SSL.SSLError, SSLVerificationError) as e:
      return S_ERROR("Error in acceptConnection: %s %s" % (e, repr(e)))

  # Depending on the DIRAC_M2CRYPTO_SPLIT_HANDSHAKE we either do the
  # handshake separately or not
  if os.getenv('DIRAC_M2CRYPTO_SPLIT_HANDSHAKE', 'Yes').lower() in ('yes', 'true'):
    acceptConnection = acceptConnection_multipleSteps
    handshake = handshake_multipleSteps
    setClientSocket = setClientSocket_multipleSteps
  else:
    acceptConnection = acceptConnection_singleStep
    handshake = handshake_singleStep
    setClientSocket = setClientSocket_singleStep

  def _read(self, bufSize=4096, skipReadyCheck=False):
    """ Read bufSize bytes from the buffer.

        :param bufSize: size of the buffer to read
        :param skipReadyCheck: ignored.


        :returns: S_OK(number of byte read)
    """
    try:
      read = self.oSocket.read(bufSize)
      return S_OK(read)
    except (socket.error, SSL.SSLError, SSLVerificationError) as e:
      return S_ERROR("Error in _read: %s %s" % (e, repr(e)))

  def isLocked(self):
    """ Returns if this instance is locked.
        Always returns false.

        :returns: False
    """
    return self.__locked

  def _write(self, buf):
    """ Write all bytes contained within iterable "buf" to the
        connected peer.

        :param buf: iterable buffer

        :returns: S_OK(number of bytes written)
    """
    try:
      # If the client application has abruptly terminated
      # the connection will be in CLOSE_WAIT on the server side.
      # And when the server side will anyway try to send back its data.
      # The first call to this _write method will succeed,
      # and we will never know that the connection was broken.
      # However, the client would answer with an RST packet.
      # And writting on a socket that received an RST packet
      # triggers a SIGPIPE.
      # In practice, this means that if the server replies to a
      # dead client with less that 16384 bytes (see),
      # we will never notice that we sent the answer to the vacuum.
      # And don't look for a fix, there just isn't.
      wrote = self.oSocket.write(buf)
      return S_OK(wrote)
    except (socket.error, SSL.SSLError, SSLVerificationError) as e:
      return S_ERROR("Error in _write: %s %s" % (e, repr(e)))
