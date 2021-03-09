''' StatesMonitoringAgent
  sends periodically numbers of jobs in various states for various
     sites to the Monitoring system to create monitoring plots.

  As of DIRAC v7r2, this agent is an almost exact copy of StatesAccountingAgent,
  the only difference being that it will only use the Monitoring System
  as its backend (and so ElasticSearch).
  This agent will be removed from DIRAC v7r3.

.. literalinclude:: ../ConfigTemplate.cfg
  :start-after: ##BEGIN StatesMonitoringAgent
  :end-before: ##END
  :dedent: 2
  :caption: StatesMonitoringAgent options

'''
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

from DIRAC import S_OK
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Utilities import Time
from DIRAC.MonitoringSystem.Client.MonitoringReporter import MonitoringReporter
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB


class StatesMonitoringAgent(AgentModule):
  """
  """

  __summaryKeyFieldsMapping = ['Status',
                               'Site',
                               'User',
                               'UserGroup',
                               'JobGroup',
                               'JobType',
                               'ApplicationStatus',
                               'MinorStatus']
  __summaryDefinedFields = [('ApplicationStatus', 'unset'),
                            ('MinorStatus', 'unset')]
  __summaryValueFieldsMapping = ['Jobs',
                                 'Reschedules']
  __renameFieldsMapping = {'JobType': 'JobSplitType'}

  __jobDBFields = []

  jobDB = None
  monitoringReporter = None

  def initialize(self):
    """ Standard initialization
    """

    self.jobDB = JobDB()

    self.am_setOption("PollingTime", 900)
    self.messageQueue = self.am_getOption('MessageQueue', 'dirac.wmshistory')

    self.monitoringReporter = MonitoringReporter(
        monitoringType="WMSHistory",
        failoverQueueName=self.messageQueue)

    for field in self.__summaryKeyFieldsMapping:
      if field == 'User':
        field = 'Owner'
      elif field == 'UserGroup':
        field = 'OwnerGroup'
      self.__jobDBFields.append(field)

    return S_OK()

  def execute(self):
    """ Main execution method
    """
    # Get the WMS Snapshot!
    result = self.jobDB.getSummarySnapshot(self.__jobDBFields)
    now = Time.dateTime()
    if not result['OK']:
      self.log.error("Can't get the JobDB summary", "%s: won't commit at this cycle" % result['Message'])
    else:
      values = result['Value'][1]

      self.log.info("Start sending records")
      for record in values:
        record = record[1:]
        rD = {}
        for fV in self.__summaryDefinedFields:
          rD[fV[0]] = fV[1]
        for iP in range(len(self.__summaryKeyFieldsMapping)):
          fieldName = self.__summaryKeyFieldsMapping[iP]
          rD[self.__renameFieldsMapping.get(fieldName, fieldName)] = record[iP]
        record = record[len(self.__summaryKeyFieldsMapping):]
        for iP in range(len(self.__summaryValueFieldsMapping)):
          rD[self.__summaryValueFieldsMapping[iP]] = int(record[iP])
        rD['timestamp'] = int(Time.toEpoch(now))
        self.monitoringReporter.addRecord(rD)

      retVal = self.monitoringReporter.commit()
      if retVal['OK']:
         self.log.info("Records sent", "(%s)" % retVal['Value'])
      else:
        self.log.error("Failed to insert the records, it will be retried in the next iteration", retVal['Message'])

    return S_OK()
