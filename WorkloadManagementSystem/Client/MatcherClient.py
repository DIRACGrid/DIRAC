""" Module that contains simple client access to Matcher service
"""

__RCSID__ = "$Id$"

from DIRAC.Core.Base.Client import Client, createClient


@createClient('WorkloadManagement/Matcher')
class MatcherClient(Client):

  """ Exposes the functionality available in the WorkloadManagement/MatcherHandler

      This inherits the DIRAC base Client for direct execution of server functionality.
      The following methods are available (although not visible here).

  """

  def __init__(self, **kwargs):
    """ Simple constructor
    """

    super(MatcherClient, self).__init__(**kwargs)
    self.setServer('WorkloadManagement/Matcher')
