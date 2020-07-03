"""
  RPCClientSelector can replace RPCClient (with import RPCClientSelector as RPCClient)
  to migrate from DISET to Tornado. This method chooses and returns the client wich should be
  used for a service. If the url of the service uses HTTPS, TornadoClient is returned, else it returns RPCClient

  Example::

    from DIRAC.Core.Tornado.Client.RPCClientSelector import RPCClientSelector as RPCClient
    myService = RPCClient("Framework/MyService")
    myService.doSomething()
"""


from DIRAC.Core.Tornado.Client.TornadoClient import TornadoClient
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.ConfigurationSystem.Client.PathFinder import getServiceURL
from DIRAC import gLogger


def isURL(url):
  """
    Just a test to check if URL is already given or not
  """
  return url.startswith('http') or url.startswith('dip')


def RPCClientSelector(*args, **kwargs):  # We use same interface as RPCClient
  """
    Select the correct RPCClient, instanciate it, and return it

    :param args: URL can be just "system/service" or "dips://domain:port/system/service"
  """

  # We detect if we need to use a specific class for the HTTPS client
  if 'httpsClient' in kwargs:
    TornadoRPCClient = kwargs.pop('httpsClient')
  else:
    TornadoRPCClient = TornadoClient

  # We have to make URL resolution BEFORE the RPCClient or TornadoClient to determine wich one we want to use
  # URL is defined as first argument (called serviceName) in RPCClient

  try:
    serviceName = args[0]
    gLogger.verbose("Trying to autodetect client for %s" % serviceName)
    if not isURL(serviceName):
      completeUrl = getServiceURL(serviceName)
      gLogger.verbose("URL resolved: %s" % completeUrl)
    else:
      completeUrl = serviceName
    if completeUrl.startswith("http"):
      gLogger.info("Using HTTPS for service %s" % serviceName)
      rpc = TornadoRPCClient(*args, **kwargs)
    else:
      rpc = RPCClient(*args, **kwargs)
  except Exception:
    # If anything went wrong in the resolution, we return default RPCClient
    # So the comportement is exactly the same as before implementation of Tornado
    rpc = RPCClient(*args, **kwargs)
  return rpc
