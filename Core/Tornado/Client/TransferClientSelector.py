"""
  TransferClientSelector can replace TransferClient (with import TransferClientSelector as TransferClient)
  to migrate from DISET to Tornado. This method chooses and returns the client which should be
  used for a service. If the url of the service uses HTTPS, TornadoClient is returned, else it returns TransferClient

  Example::

    from DIRAC.Core.Tornado.Client.TransferClientSelector import TransferClientSelector as TransferClient
    myService = TransferClient("Framework/MyService")
    myService.doSomething()
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from DIRAC.Core.Tornado.Client.TornadoClient import TornadoClient
from DIRAC.Core.DISET.TransferClient import TransferClient
from DIRAC.ConfigurationSystem.Client.PathFinder import getServiceURL
from DIRAC import gLogger

sLog = gLogger.getSubLogger(__name__)


def TransferClientSelector(*args, **kwargs):  # We use same interface as TransferClient
  """
    Select the correct TransferClient, instanciate it, and return it

    :param args: URL can be just "system/service" or "dips://domain:port/system/service"
  """

  # We detect if we need to use a specific class for the HTTPS client

  TornadoTransClient = kwargs.pop('httpsClient', TornadoClient)

  # We have to make URL resolution BEFORE the TransferClient or TornadoClient to determine which one we want to use
  # URL is defined as first argument (called serviceName) in TransferClient

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
      transClient = TornadoTransClient(*args, **kwargs)
    else:
      transClient = TransferClient(*args, **kwargs)
  except Exception as e:  # pylint: disable=broad-except
    # If anything went wrong in the resolution, we return default TransferClient
    # So the behavior is exactly the same as before implementation of Tornado
    sLog.warn("Could not select RPC or Tornado client", "%s" % repr(e))
    transClient = TransferClient(*args, **kwargs)
  return transClient
