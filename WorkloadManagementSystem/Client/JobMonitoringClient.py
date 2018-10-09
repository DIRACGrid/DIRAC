""" Class that contains client access to the job monitoring handler. """

from __future__ import absolute_import

__RCSID__ = "$Id$"

from DIRAC.Core.Base.Client import Client, ClientCreator


class JobMonitoringClient(Client):
  __metaclass__ = ClientCreator
  handlerModuleName = 'DIRAC.WorkloadManagementSystem.Service.JobMonitoringHandler'
  handlerClassName = 'JobMonitoringHandler'

  def __init__(self, **kwargs):
    Client.__init__(self, **kwargs)
    self.setServer('WorkloadManagement/JobMonitoring')

  def traceJobParameters(self, site, localID, parameterList=None, attributeList=None, date=None, until=None):
    return self._getRPC().traceJobParameters(site, localID, parameterList, attributeList, date, until)

  def traceJobParameter(self, site, localID, parameter, date=None, until=None):
    return self._getRPC().traceJobParameter(site, localID, parameter, date, until)
