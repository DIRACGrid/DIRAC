"""
  TornadoClient is equivalent of the RPCClient but in HTTPS.
  Usage of TornadoClient is the same as RPCClient, you can instanciate TornadoClient with
  complete url (https://domain/component/service) or just "component/service". Like RPCClient
  you can use all method defined in your service, your call will be automatically transformed
  in RPC.

  Main changes:
    - KeepAliveLapse is removed, requests library manage it hitself.
    - nbOfRetry (defined as private attribute) is removed, requests library manage it hitself.
    - Underneath it use HTTP POST protocol and JSON


  Example::

    from DIRAC.TornadoServices.Client.TornadoClient import TornadoClient
    myService = TornadoClient("Framework/MyService")
    myService.doSomething() #Returns S_OK/S_ERROR

"""

from DIRAC.Core.Utilities.JEncode import encode
from DIRAC.TornadoServices.Client.private.TornadoBaseClient import TornadoBaseClient



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
      This function call a remote service

      :param str procedure: remote procedure name
      :param args: list of arguments
      :returns: decoded response from server, server may return S_OK or S_ERROR
    """
    rpcCall = {'method': method, 'args': encode(args)}
    # Start request
    retVal = self._request(rpcCall)
    retVal['rpcStub'] = (self._getBaseStub(), method, args)
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
