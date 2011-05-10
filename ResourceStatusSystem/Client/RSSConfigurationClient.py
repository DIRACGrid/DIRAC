"""
RSSConfigurationClient class is a base class for building clients for
the RSSConfiguration Service. It justs set the variable
self.RSSConfiguration ready to call functions exposed by the
RSSConfigurationHandler.
"""

from DIRAC.Core.DISET.RPCClient import RPCClient

class RSSConfigurationClient(object):

  def __init__(self):
    self.RSSConfiguration = RPCClient("ResourceStatus/RSSConfiguration")
