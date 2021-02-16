"""Class for making requests to a ComponentMonitoring Service."""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

from DIRAC.Core.Base.Client import Client, createClient


@createClient('Framework/ComponentMonitoring')
class ComponentMonitoringClient(Client):

  def __init__(self, **kwargs):
    """
    Constructor function
    """

    super(ComponentMonitoringClient, self).__init__(**kwargs)
    self.setServer('Framework/ComponentMonitoring')
