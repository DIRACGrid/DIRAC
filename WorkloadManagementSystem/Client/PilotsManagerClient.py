""" Module that contains client access to the Pilots handler.
"""

from DIRAC.Core.Base.Client import Client


class PilotsManagerClient(Client):
  """ PilotsManagerClient sets url for the PilotsManagerHandler.
  """

  def __init__(self, url=None, **kwargs):
    """
    Sets URL for PilotsManager handler

    :param self: self reference
    :param url: url of the PilotsManagerHandler
    :param kwargs: forwarded to the Base Client class
    """

    super(PilotsManagerClient, self).__init__(**kwargs)

    if not url:
      self.serverURL = 'WorkloadManagement/PilotsManager'

    else:
      self.serverURL = url
