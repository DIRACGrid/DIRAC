""" Class that contains client access to the JobManager handler. """

from __future__ import absolute_import
from six import add_metaclass

from DIRAC.Core.Base.Client import Client, ClientCreator


@add_metaclass(ClientCreator)
class JobManagerClient(Client):
  """JobManagerClient sets url for the JobManagerHandler.
  """
  handlerModuleName = 'DIRAC.WorkloadManagementSystem.Service.JobManagerHandler'
  handlerClassName = 'JobManagerHandler'

  def __init__(self, url=None, **kwargs):
    """
    Sets URL for JobManager handler

    :param self: self reference
    :param url: url of the JobManagerHandler
    :param kwargs: forwarded to the Base Client class
    """

    super(JobManagerClient, self).__init__(**kwargs)

    if not url:
      self.serverURL = 'WorkloadManagement/JobManager'

    else:
      self.serverURL = url
