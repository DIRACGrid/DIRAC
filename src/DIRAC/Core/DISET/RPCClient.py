""" RPCClient object is used to create RPC connection to services
"""
from DIRAC.Core.DISET.private.InnerRPCClient import InnerRPCClient


class _MagicMethod:
    """This object allows to bundle together a function calling
    an RPC and the remote function name.
    When this object is called (__call__), the call is performed.
    """

    def __init__(self, doRPCFunc, remoteFuncName):
        """Constructor

        :param doRPCFunc: the function actually performing the RPC call
        :param remoteFuncName: name of the remote function
        """
        self.__doRPCFunc = doRPCFunc
        self.__remoteFuncName = remoteFuncName

    def __getattr__(self, remoteFuncName):
        """I really do not understand when this would be called.
        I can only imagine it being called by dir, or things like that.
        In any case, it recursively return a MagicMethod object
        where the new remote function name is the old one to which
        we append the new called attribute.
        """
        return _MagicMethod(self.__doRPCFunc, f"{self.__remoteFuncName}.{remoteFuncName}")

    def __call__(self, *args, **kwargs):
        """Triggers the call.
        it uses the RPC calling function given by RPCClient,
        and gives as argument the remote function name and whatever
        arguments given.
        """
        return self.__doRPCFunc(self.__remoteFuncName, args, **kwargs)

    def __str__(self):
        return f"<RPCClient method {self.__remoteFuncName}>"


class RPCClient:
    """This class contains the mechanism to convert normal calls to RPC calls.

    When instanciated, it creates a :class:`~DIRAC.Core.DISET.private.InnerRPCClient.InnerRPCClient`
    as an attribute. Any attribute which is accessed is then either redirected to InnerRPCClient if it has it,
    or creates a MagicMethod object otherwise. If the attribute is a function, MagicMethod will
    trigger the RPC call, using the InnerRPCClient.

    The typical workflow looks like this::

      rpc = RPCClient('DataManagement/FileCatalog')

      # Here, func is the ping function, which we call remotely.
      # We go through RPCClient.__getattr__ which returns us a MagicMethod object
      func = rpc.ping

      # Here we call the method __call__ of the MagicMethod
      func()

    """

    def __init__(self, *args, **kwargs):
        """
        Constructor
        The arguments are just passed on to InnerRPCClient.
        In practice:

          * args: has to be the service name or URL
          * kwargs: all the arguments InnerRPCClient and BaseClient accept as configuration
        """
        self.__innerRPCClient = InnerRPCClient(*args, **kwargs)

    def __doRPC(self, sFunctionName, args, **kwargs):
        """
        Execute the RPC action. This is given as an attribute
        to MagicMethod

        :param sFunctionName: name of the remote function
        :param args: arguments to pass to the function
        """
        return self.__innerRPCClient.executeRPC(sFunctionName, args, **kwargs)

    def __getattr__(self, attrName):
        """Function for emulating the existence of functions.

          In literature this is usually called a "stub function".
        If the attribute exists in InnerRPCClient, return it,
        otherwise we create a _MagicMethod instance

        """
        if attrName in dir(self.__innerRPCClient):
            return getattr(self.__innerRPCClient, attrName)
        return _MagicMethod(self.__doRPC, attrName)


def executeRPCStub(rpcStub):
    """
    Playback a stub
    """
    # Generate a RPCClient with the same parameters
    rpcClient = RPCClient(rpcStub[0][0], **rpcStub[0][1])
    # Get a functor to execute the RPC call
    rpcFunc = getattr(rpcClient, rpcStub[1])
    # Reproduce the call
    return rpcFunc(*rpcStub[2])
