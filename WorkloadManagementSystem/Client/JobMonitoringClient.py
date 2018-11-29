""" Class that contains client access to the job monitoring handler. """

__RCSID__ = "$Id$"

from DIRAC.Core.Base.Client import Client


class JobMonitoringClient(Client):

  def __init__(self, **kwargs):

    super(JobMonitoringClient, self).__init__(**kwargs)
    self.setServer('WorkloadManagement/JobMonitoring')

  def traceJobParameters(self, site, localID, parameterList=None, attributeList=None, date=None, until=None):
    return self._getRPC().traceJobParameters(site, localID, parameterList, attributeList, date, until)

  def traceJobParameter(self, site, localID, parameter, date=None, until=None):
    return self._getRPC().traceJobParameter(site, localID, parameter, date, until)
