""" Class that contains client access to the job monitoring handler. """

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

from DIRAC.Core.Base.Client import Client, createClient
from DIRAC.Core.Utilities.Decorators import deprecated


@createClient('WorkloadManagement/JobMonitoring')
class JobMonitoringClient(Client):

  def __init__(self, **kwargs):

    super(JobMonitoringClient, self).__init__(**kwargs)
    self.setServer('WorkloadManagement/JobMonitoring')

  @deprecated("Unused")
  def traceJobParameters(self, site, localID, parameterList=None, attributeList=None, date=None, until=None):
    return self._getRPC().traceJobParameters(site, localID, parameterList, attributeList, date, until)

  @deprecated("Unused")
  def traceJobParameter(self, site, localID, parameter, date=None, until=None):
    return self._getRPC().traceJobParameter(site, localID, parameter, date, until)
