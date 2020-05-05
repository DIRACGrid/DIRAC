########################################################################
# File :    StatesAccountingAgent.py
# Author :  A.T.
########################################################################

"""  StatesAccountingAgent sends periodically numbers of jobs in various states for various
     sites to the Monitoring system to create historical plots.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
__RCSID__ = "$Id$"


from DIRAC import gConfig, S_OK
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
from DIRAC.AccountingSystem.Client.Types.WMSHistory import WMSHistory
from DIRAC.AccountingSystem.Client.DataStoreClient import DataStoreClient
from DIRAC.Core.Utilities import Time


class StatesAccountingAgent(AgentModule):
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
                               ]
  __summaryDefinedFields = [('ApplicationStatus', 'unset'), ('MinorStatus', 'unset')]
  __summaryValueFieldsMapping = ['Jobs',
                                 'Reschedules',
                                 ]
  __renameFieldsMapping = {'JobType': 'JobSplitType'}

  def initialize(self):
    """ Standard constructor
    """
    self.dsClients = {}
    self.jobDB = JobDB()
    self.retryOnce = False
    self.retryValues = []

    self.reportPeriod = 850
    self.am_setOption("PollingTime", self.reportPeriod)
    self.__jobDBFields = []
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
      self.log.error("Can't get the JobDB summary", "%s: won't commit at this cycle" % result['Message'])
    else:
      values = result['Value'][1]

      if self.retryOnce:
        self.log.verbose("Adding to records to commit those not committed within the previous cycle")
      acWMSListAdded = []

      for record in values:
        recordSetup = record[0]
        if recordSetup not in validSetups:
          self.log.error("Setup %s is not valid" % recordSetup)
          continue
        if recordSetup not in self.dsClients:
          self.log.info("Creating DataStore client for %s" % recordSetup)
          self.dsClients[recordSetup] = DataStoreClient(retryGraceTime=900)
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
        acWMS = WMSHistory()
        acWMS.setStartTime(now)
        acWMS.setEndTime(now)
        acWMS.setValuesFromDict(rD)
        retVal = acWMS.checkValues()
        if not retVal['OK']:
          self.log.error("Invalid accounting record ", "%s -> %s" % (retVal['Message'], rD))
        else:
          self.dsClients[recordSetup].addRegister(acWMS)
          acWMSListAdded.append(acWMS)

      if self.retryOnce and self.retryValues:
        for acWMSCumulated in self.retryValues:
          retVal = acWMSCumulated.checkValues()
          if not retVal['OK']:
            self.log.error("Invalid accounting record ", "%s" % (retVal['Message']))
          else:
            self.dsClients[recordSetup].addRegister(acWMSCumulated)

      for setup in self.dsClients:
        self.log.info("Sending records for setup %s" % setup)
        result = self.dsClients[setup].commit()
        if not result['OK']:
          self.log.error("Couldn't commit wms history for setup %s" % setup, result['Message'])
          # Re-creating the client: for new connection, and for avoiding accumulating too large of a backlog
          self.dsClients[setup] = DataStoreClient(retryGraceTime=900)
          if not self.retryOnce:
            self.log.info("Will try again at next cycle")
            self.retryOnce = True
            self.retryValues = acWMSListAdded
          else:
            self.log.warn("Won't retry one more time")
            self.retryOnce = False
            self.retryValues = []
        else:
          self.log.info("Sent %s records for setup %s" % (result['Value'], setup))
          self.retryOnce = False
    return S_OK()
