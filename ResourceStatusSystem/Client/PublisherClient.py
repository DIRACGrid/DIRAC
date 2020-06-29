""" Class that contains client access to the Publisher handler. """
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

from DIRAC.Core.Base.Client import Client


class PublisherClient(Client):

  def __init__(self, **kwargs):

    super(PublisherClient, self).__init__(**kwargs)
    self.setServer('ResourceStatus/Publisher')
