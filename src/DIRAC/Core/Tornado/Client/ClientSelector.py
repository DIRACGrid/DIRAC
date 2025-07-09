"""
  This modules defines two functions that can be used in place of the ``RPCClient`` and
  ``TransferClient`` to transparently switch to ``https``.

  Example::

    from DIRAC.Core.Tornado.Client.ClientSelector import RPCClientSelector as RPCClient
    myService = RPCClient("Framework/MyService")
    myService.doSomething()
"""

import functools

from DIRAC import gLogger
from DIRAC.ConfigurationSystem.Client.PathFinder import getServiceURL, useLegacyAdapter
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.DISET.TransferClient import TransferClient
from DIRAC.Core.Tornado.Client.TornadoClient import TornadoClient

sLog = gLogger.getSubLogger(__name__)


def ClientSelector(disetClient, *args, **kwargs):  # We use same interface as RPCClient
    """
    Select the correct Client (either RPC or Transfer ), instantiate it, and return it.

    The selection is based on the URL:

    * either it contains the protocol, in which case we make a choice
    * or it is in the form <Component/Service>, in which case we resolve first

    This is a generic function. You should rather use :py:class:`.RPCClientSelector`
    or :py:class:`.TransferClientSelector`


    In principle, the only place for this class to be used is in
    :py:class:`DIRAC.Core.Base.Client.Client`, since it is the only
    one supposed to instantiate an :py:class:`DIRAC.Core.Base.DISET.RPCClient.RPCClient`

    :params disetClient: the DISET class to be instantiated, so either
        :py:class:`DIRAC.Core.Base.DISET.RPCClient.RPCClient`
        or :py:class:`DIRAC.Core.Base.DISET.TransferClient.TransferClient`
    :param args: Whatever ``disetClient`` takes as args, but the first one is
        always the URL we want to rely on.
        It can be either "system/service" or "dips://domain:port/system/service"
    :param kwargs: This can contain:

      * Whatever ``disetClient`` takes.
      * httpsClient: specific class inheriting from TornadoClient

    """

    # We detect if we need to use a specific class for the HTTPS client

    tornadoClient = kwargs.pop("httpsClient", TornadoClient)
    diracxClient = kwargs.pop("diracxClient", None)

    # We have to make URL resolution BEFORE the RPCClient or TornadoClient to determine which one we want to use
    # URL is defined as first argument (called serviceName) in RPCClient

    try:
        serviceName = args[0]
        sLog.debug(f"Trying to autodetect client for {serviceName}")

        # If we are not already given a URL, resolve it
        if serviceName.startswith(("http", "dip")):
            completeUrl = serviceName
        elif useLegacyAdapter(serviceName):
            sLog.debug(f"Using legacy adapter for service {serviceName}")
            if diracxClient is None:
                raise NotImplementedError(
                    "DiracX is enabled but no diracxClient is provided, do you need to update your client?"
                )
            return diracxClient()
        else:
            completeUrl = getServiceURL(serviceName)
            sLog.debug(f"URL resolved: {completeUrl}")

        if completeUrl.startswith("http"):
            sLog.debug(f"Using HTTPS for service {serviceName}")
            rpc = tornadoClient(*args, **kwargs)
        else:
            rpc = disetClient(*args, **kwargs)
    except NotImplementedError as e:
        # We catch explicitly NotImplementedError to avoid just printing "there's an error"
        # If we mis-configured the CS for legacy adapted services, we MUST have an error.
        raise e
    except Exception as e:  # pylint: disable=broad-except
        # If anything went wrong in the resolution, we return default RPCClient
        # So the behaviour is exactly the same as before implementation of Tornado
        sLog.warn("Could not select DISET or Tornado client", f"{repr(e)}")
        rpc = disetClient(*args, **kwargs)
    return rpc


# Client to use for RPC selection
RPCClientSelector = functools.partial(ClientSelector, RPCClient)

# Client to use for Transfer selection
TransferClientSelector = functools.partial(ClientSelector, TransferClient)
