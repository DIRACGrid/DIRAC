""" Module that contains client access to the WMSAdministrator handler.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from DIRAC.Core.Base.Client import Client, createClient


@createClient('WorkloadManagement/WMSAdministrator')
class WMSAdministratorClient(Client):
  """JobManagerClient sets url for the WMSAdministratorHandler.
  """

  def __init__(self, url=None, **kwargs):
    """
    Sets URL for WMSAdministrator handler

    :param self: self reference
    :param url: url of the WMSAdministratorHandler
    :param kwargs: forwarded to the Base Client class
    """

    super(WMSAdministratorClient, self).__init__(**kwargs)

    if not url:
      self.serverURL = 'WorkloadManagement/WMSAdministrator'

    else:
      self.serverURL = url
