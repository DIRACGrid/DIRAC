"""
  TransferClientSelector can replace TransferClient (with import TransferClientSelector as TransferClient)
  to migrate from DISET to Tornado. This method chooses and returns the client wich should be
  used for a service. If the url of the service uses HTTPS, TornadoClient is returned, else it returns TransferClient

  Example::

    from DIRAC.Core.Tornado.Client.TransferClientSelector import TransferClientSelector as TransferClient
    myService = TransferClient("Framework/MyService")
    myService.doSomething()
"""


from DIRAC.Core.Tornado.Client.TornadoClient import TornadoClient
from DIRAC.Core.DISET.TransferClient import TransferClient
from DIRAC.ConfigurationSystem.Client.PathFinder import getServiceURL
from DIRAC import gLogger


def isURL(url):
  """
    Just a test to check if URL is already given or not
  """
  return url.startswith('http') or url.startswith('dip')


def TransferClientSelector(*args, **kwargs):  # We use same interface as TransferClient
  """
    Select the correct TransferClient, instanciate it, and return it
    :param args[0]: url: URL can be just "system/service" or "dips://domain:port/system/service"
  """

  # We detect if we need to use a specific class for the HTTPS client
  if 'httpsClient' in kwargs:
    TornadoTransClient = kwargs.pop('httpsClient')
  else:
    TornadoTransClient = TornadoClient

  # We have to make URL resolution BEFORE the TransferClient or TornadoClient to determine wich one we want to use
  # URL is defined as first argument (called serviceName) in TransferClient

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
      transClient = TornadoTransClient(*args, **kwargs)
    else:
      transClient = TransferClient(*args, **kwargs)
  except Exception:
    # If anything went wrong in the resolution, we return default TransferClient
    # So the comportement is exactly the same as before implementation of Tornado
    transClient = TransferClient(*args, **kwargs)
  return transClient
