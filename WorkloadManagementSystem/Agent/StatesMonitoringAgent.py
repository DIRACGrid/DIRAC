''' StatesMonitoringAgent
  sends periodically numbers of jobs in various states for various
     sites to the Monitoring system to create historical plots.

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

from DIRAC import gConfig, S_OK
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Utilities import Time
from DIRAC.MonitoringSystem.Client.MonitoringReporter import MonitoringReporter
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB


class StatesMonitoringAgent(AgentModule):
  """
      The specific agents must provide the following methods:
        - initialize() for initial settings
        - beginExecution()
        - execute() - the main method called in the agent cycle
        - endExecution()
        - finalize() - the graceful exit of the method, this one is usually used
                   for the agent restart
  """

  __summaryKeyFieldsMapping = ['Status',
                               'Site',
                               'User',
                               'UserGroup',
                               'JobGroup',
                               'JobType',
                               'ApplicationStatus',
                               'MinorStatus']
  __summaryDefinedFields = [('ApplicationStatus', 'unset'), ('MinorStatus', 'unset')]
  __summaryValueFieldsMapping = ['Jobs',
                                 'Reschedules']
  __renameFieldsMapping = {'JobType': 'JobSplitType'}

  __jobDBFields = []

  jobDB = None
  monitoringReporter = None

  def initialize(self):
    """ Standard constructor
    """

    self.jobDB = JobDB()

    self.am_setOption("PollingTime", 900)
    self.messageQueue = self.am_getOption('MessageQueue', 'dirac.wmshistory')

    self.monitoringReporter = MonitoringReporter(monitoringType="WMSHistory", failoverQueueName=self.messageQueue)

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
    result = gConfig.getSections("/DIRAC/Setups")
    if not result['OK']:
      return result
    validSetups = result['Value']
    self.log.info("Valid setups for this cycle are %s" % ", ".join(validSetups))
    # Get the WMS Snapshot!
    result = self.jobDB.getSummarySnapshot(self.__jobDBFields)
    now = Time.dateTime()
    if not result['OK']:
      self.log.error("Can't get the jobdb summary", result['Message'])
    else:
      values = result['Value'][1]
      self.log.info("Start sending records!")
      for record in values:
        recordSetup = record[0]
        if recordSetup not in validSetups:
          self.log.error("Setup %s is not valid" % recordSetup)
          continue
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
        self.log.info("The records are successfully sent to the Store!")
      else:
        self.log.warn("Faild to insert the records! It will be retried in the next iteration", retVal['Message'])

    return S_OK()
