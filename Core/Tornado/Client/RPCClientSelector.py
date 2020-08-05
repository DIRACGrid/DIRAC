"""
  RPCClientSelector can replace RPCClient (with import RPCClientSelector as RPCClient)
  to migrate from DISET to Tornado. This method chooses and returns the client which should be
  used for a service. If the url of the service uses HTTPS, TornadoClient is returned, else it returns RPCClient

  Example::

    from DIRAC.Core.Tornado.Client.RPCClientSelector import RPCClientSelector as RPCClient
    myService = RPCClient("Framework/MyService")
    myService.doSomething()
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from DIRAC.Core.Tornado.Client.TornadoClient import TornadoClient
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.ConfigurationSystem.Client.PathFinder import getServiceURL
from DIRAC import gLogger

sLog = gLogger.getSubLogger(__name__)


def RPCClientSelector(*args, **kwargs):  # We use same interface as RPCClient
  """
    Select the correct RPCClient, instanciate it, and return it

    :param args: URL can be just "system/service" or "dips://domain:port/system/service"
  """

  # We detect if we need to use a specific class for the HTTPS client

  TornadoRPCClient = kwargs.pop('httpsClient', TornadoClient)

  # We have to make URL resolution BEFORE the RPCClient or TornadoClient to determine which one we want to use
  # URL is defined as first argument (called serviceName) in RPCClient

  try:
    serviceName = args[0]
    sLog.verbose("Trying to autodetect client for %s" % serviceName)

    # If we are not already given a URL, resolve it
    if serviceName.startswith(('http', 'dip')):
      completeUrl = serviceName
    else:
      completeUrl = getServiceURL(serviceName)
      sLog.verbose("URL resolved: %s" % completeUrl)

    if completeUrl.startswith("http"):
      sLog.info("Using HTTPS for service %s" % serviceName)
      rpc = TornadoRPCClient(*args, **kwargs)
    else:
      rpc = RPCClient(*args, **kwargs)
  except Exception as e:  # pylint: disable=broad-except
    # If anything went wrong in the resolution, we return default RPCClient
    # So the behaviour is exactly the same as before implementation of Tornado
    sLog.warn("Could not select RPC or Tornado client", "%s" % repr(e))
    rpc = RPCClient(*args, **kwargs)
  return rpc
