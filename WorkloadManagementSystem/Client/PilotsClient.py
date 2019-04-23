""" Module that contains client access to the Pilots handler.
"""

from DIRAC.Core.Base.Client import Client


class PilotsClient(Client):
  """JobManagerClient sets url for the PilotsHandler.
  """

  def __init__(self, url=None, **kwargs):
    """
    Sets URL for Pilots handler

    :param self: self reference
    :param url: url of the PilotsHandler
    :param kwargs: forwarded to the Base Client class
    """

    super(PilotsClient, self).__init__(**kwargs)

    if not url:
      self.serverURL = 'WorkloadManagement/Pilots'

    else:
      self.serverURL = url
