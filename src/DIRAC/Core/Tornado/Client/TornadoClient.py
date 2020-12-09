"""
  TornadoClient is equivalent of the RPCClient but in HTTPS.
  Usage of TornadoClient is the same as RPCClient, you can instantiate TornadoClient with
  complete url (https://host:port/System/Component) or just "System/Component". Like RPCClient
  you can use all method defined in your service, your call will be automatically transformed
  in RPC.

  It also exposes the same interface for receiving file as the TransferClient.

  Main changes:

  - KeepAliveLapse is removed, requests library manages it itself.
  - nbOfRetry (defined as private attribute) is removed, requests library manage it itself.
  - Underneath it uses HTTP POST protocol and JSON. See  :ref:`httpsTornado` for details

  Example::

    from DIRAC.Core.Tornado.Client.TornadoClient import TornadoClient
    myService = TornadoClient("Framework/MyService")
    myService.doSomething() #Returns S_OK/S_ERROR

"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

# pylint: disable=broad-except

from DIRAC.Core.Tornado.Client.private.TornadoBaseClient import TornadoBaseClient
from DIRAC.Core.Utilities.JEncode import encode


class TornadoClient(TornadoBaseClient):
  """
    Client for calling tornado services
    Interface is based on RPCClient interface
  """

  def __getattr__(self, attrname):
    """
      Return the RPC call procedure

      :param str attrname: Name of the procedure we are trying to call
      :return: RPC procedure as function
    """
    def call(*args):
      """
        Just returns the right function for RPC Call
      """
      return self.executeRPC(attrname, *args)
    return call

  # Name from RPCClient Interface
  def executeRPC(self, method, *args):
    """
      Calls a remote service

      :param str method: remote procedure name
      :param args: list of arguments
      :returns: decoded response from server, server may return S_OK or S_ERROR
    """
    rpcCall = {'method': method, 'args': encode(args)}
    # Start request
    retVal = self._request(**rpcCall)
    retVal['rpcStub'] = (self._getBaseStub(), method, list(args))
    return retVal

  def receiveFile(self, destFile, *args):
    """
      Equivalent of :py:meth:`~DIRAC.Core.DISET.TransferClient.TransferClient.receiveFile`

      In practice, it calls the remote method `streamToClient` and stores the raw result in a file

      :param str destFile: path where to store the result
      :param args: list of arguments
      :returns: S_OK/S_ERROR
    """
    rpcCall = {'method': 'streamToClient', 'args': encode(args)}
    # Start request
    retVal = self._request(outputFile=destFile, **rpcCall)
    return retVal


def executeRPCStub(rpcStub):
  """
  Playback a stub
  # Copy-paste from DIRAC.Core.DISET.RPCClient with RPCClient changed into TornadoClient
  """
  # Generate a client with the same parameters
  client = TornadoClient(rpcStub[0][0], **rpcStub[0][1])
  # Get a functor to execute the RPC call
  rpcFunc = getattr(client, rpcStub[1])
  # Reproduce the call
  return rpcFunc(*rpcStub[2])
