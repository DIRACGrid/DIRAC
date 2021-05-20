########################################################################
# File :    PilotStatusAgent.py
# Author :  Stuart Paterson
########################################################################
"""  The Pilot Status Agent updates the status of the pilot jobs in the
     PilotAgents database.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
__RCSID__ = "$Id$"

from DIRAC import S_OK, S_ERROR, gConfig
from DIRAC.AccountingSystem.Client.Types.Pilot import Pilot as PilotAccounting
from DIRAC.AccountingSystem.Client.DataStoreClient import gDataStoreClient
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getCESiteMapping
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Utilities import Time
from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin
from DIRAC.WorkloadManagementSystem.Client.PilotManagerClient import PilotManagerClient
from DIRAC.WorkloadManagementSystem.DB.PilotAgentsDB import PilotAgentsDB
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB

MAX_JOBS_QUERY = 10
MAX_WAITING_STATE_LENGTH = 3


class PilotStatusAgent(AgentModule):
  """
      The specific agents must provide the following methods:
        - initialize() for initial settings
        - beginExecution()
        - execute() - the main method called in the agent cycle
        - endExecution()
        - finalize() - the graceful exit of the method, this one is usually used
                   for the agent restart
  """

  queryStateList = ['Ready', 'Submitted', 'Running', 'Waiting', 'Scheduled']
  finalStateList = ['Done', 'Aborted', 'Cleared', 'Deleted', 'Failed']

  def __init__(self, *args, **kwargs):
    """ c'tor
    """
    AgentModule.__init__(self, *args, **kwargs)

    self.jobDB = None
    self.pilotDB = None
    self.diracadmin = None

  #############################################################################
  def initialize(self):
    """Sets defaults
    """

    self.am_setOption('PollingTime', 120)
    self.am_setOption('GridEnv', '')
    self.am_setOption('PilotStalledDays', 3)
    self.pilotDB = PilotAgentsDB()
    self.diracadmin = DiracAdmin()
    self.jobDB = JobDB()
    self.clearPilotsDelay = self.am_getOption('ClearPilotsDelay', 30)
    self.clearAbortedDelay = self.am_getOption('ClearAbortedPilotsDelay', 7)
    self.pilots = PilotManagerClient()

    return S_OK()

  #############################################################################
  def execute(self):
    """The PilotAgent execution method.
    """

    self.pilotStalledDays = self.am_getOption('PilotStalledDays', 3)
    self.gridEnv = self.am_getOption('GridEnv')
    if not self.gridEnv:
      # No specific option found, try a general one
      setup = gConfig.getValue('/DIRAC/Setup', '')
      if setup:
        instance = gConfig.getValue('/DIRAC/Setups/%s/WorkloadManagement' % setup, '')
        if instance:
          self.gridEnv = gConfig.getValue('/Systems/WorkloadManagement/%s/GridEnv' % instance, '')
    result = self.pilotDB._getConnection()
    if result['OK']:
      connection = result['Value']
    else:
      return result

    # Now handle pilots not updated in the last N days (most likely the Broker is no
    # longer available) and declare them Deleted.
    result = self.handleOldPilots(connection)

    connection.close()

    result = self.pilots.clearPilots(self.clearPilotsDelay, self.clearAbortedDelay)
    if not result['OK']:
      self.log.warn('Failed to clear old pilots in the PilotAgentsDB')

    return S_OK()

  def clearWaitingPilots(self, condDict):
    """ Clear pilots in the faulty Waiting state
    """

    last_update = Time.dateTime() - MAX_WAITING_STATE_LENGTH * Time.hour
    clearDict = {'Status': 'Waiting',
                 'OwnerDN': condDict['OwnerDN'],
                 'OwnerGroup': condDict['OwnerGroup'],
                 'GridType': condDict['GridType'],
                 'Broker': condDict['Broker']}
    result = self.pilotDB.selectPilots(clearDict, older=last_update)
    if not result['OK']:
      self.log.warn('Failed to get the Pilot Agents for Waiting state')
      return result
    if not result['Value']:
      return S_OK()
    refList = result['Value']

    for pilotRef in refList:
      self.log.info('Setting Waiting pilot to Stalled: %s' % pilotRef)
      result = self.pilotDB.setPilotStatus(pilotRef, 'Stalled', statusReason='Exceeded max waiting time')

    return S_OK()

  def clearParentJob(self, pRef, pDict, connection):
    """ Clear the parameteric parent job from the PilotAgentsDB
    """

    childList = pDict['ChildRefs']

    # Check that at least one child is in the database
    children_ok = False
    for child in childList:
      result = self.pilotDB.getPilotInfo(child, conn=connection)
      if result['OK']:
        if result['Value']:
          children_ok = True

    if children_ok:
      return self.pilotDB.deletePilot(pRef, conn=connection)
    else:
      self.log.verbose('Adding children for parent %s' % pRef)
      result = self.pilotDB.getPilotInfo(pRef)
      parentInfo = result['Value'][pRef]
      tqID = parentInfo['TaskQueueID']
      ownerDN = parentInfo['OwnerDN']
      ownerGroup = parentInfo['OwnerGroup']
      broker = parentInfo['Broker']
      gridType = parentInfo['GridType']
      result = self.pilotDB.addPilotTQReference(childList, tqID, ownerDN, ownerGroup,
                                                broker=broker, gridType=gridType)
      if not result['OK']:
        return result
      children_added = True
      for chRef, chDict in pDict['ChildDicts'].items():
        result = self.pilotDB.setPilotStatus(chRef, chDict['Status'],
                                             destination=chDict['DestinationSite'],
                                             conn=connection)
        if not result['OK']:
          children_added = False
      if children_added:
        result = self.pilotDB.deletePilot(pRef, conn=connection)
      else:
        return S_ERROR('Failed to add children')
    return S_OK()

  def handleOldPilots(self, connection):
    """
      select all pilots that have not been updated in the last N days and declared them
      Deleted, accounting for them.
    """
    pilotsToAccount = {}
    timeLimitToConsider = Time.toString(Time.dateTime() - Time.day * self.pilotStalledDays)
    result = self.pilotDB.selectPilots({'Status': self.queryStateList},
                                       older=timeLimitToConsider,
                                       timeStamp='LastUpdateTime')
    if not result['OK']:
      self.log.error('Failed to get the Pilot Agents')
      return result
    if not result['Value']:
      return S_OK()

    refList = result['Value']
    result = self.pilotDB.getPilotInfo(refList)
    if not result['OK']:
      self.log.error('Failed to get Info for Pilot Agents')
      return result

    pilotsDict = result['Value']

    for pRef in pilotsDict:
      if pilotsDict[pRef].get('Jobs') and self._checkJobLastUpdateTime(pilotsDict[pRef]['Jobs'], self.pilotStalledDays):
        self.log.debug('%s should not be deleted since one job of %s is running.' %
                       (str(pRef), str(pilotsDict[pRef]['Jobs'])))
        continue
      deletedJobDict = pilotsDict[pRef]
      deletedJobDict['Status'] = 'Deleted'
      deletedJobDict['StatusDate'] = Time.dateTime()
      pilotsToAccount[pRef] = deletedJobDict
      if len(pilotsToAccount) > 100:
        self.accountPilots(pilotsToAccount, connection)
        self._killPilots(pilotsToAccount)
        pilotsToAccount = {}

    self.accountPilots(pilotsToAccount, connection)
    self._killPilots(pilotsToAccount)

    return S_OK()

  def accountPilots(self, pilotsToAccount, connection):
    """ account for pilots
    """
    accountingFlag = False
    pae = self.am_getOption('PilotAccountingEnabled', 'yes')
    if pae.lower() == "yes":
      accountingFlag = True

    if not pilotsToAccount:
      self.log.info('No pilots to Account')
      return S_OK()

    accountingSent = False
    if accountingFlag:
      retVal = self.pilotDB.getPilotInfo(list(pilotsToAccount), conn=connection)
      if not retVal['OK']:
        self.log.error('Fail to retrieve Info for pilots', retVal['Message'])
        return retVal
      dbData = retVal['Value']
      for pref in dbData:
        if pref in pilotsToAccount:
          if dbData[pref]['Status'] not in self.finalStateList:
            dbData[pref]['Status'] = pilotsToAccount[pref]['Status']
            dbData[pref]['DestinationSite'] = pilotsToAccount[pref]['DestinationSite']
            dbData[pref]['LastUpdateTime'] = pilotsToAccount[pref]['StatusDate']

      retVal = self._addPilotsAccountingReport(dbData)
      if not retVal['OK']:
        self.log.error('Fail to retrieve Info for pilots', retVal['Message'])
        return retVal

      self.log.info("Sending accounting records...")
      retVal = gDataStoreClient.commit()
      if not retVal['OK']:
        self.log.error("Can't send accounting reports", retVal['Message'])
      else:
        self.log.info("Accounting sent for %s pilots" % len(pilotsToAccount))
        accountingSent = True

    if not accountingFlag or accountingSent:
      for pRef in pilotsToAccount:
        pDict = pilotsToAccount[pRef]
        self.log.verbose('Setting Status for %s to %s' % (pRef, pDict['Status']))
        self.pilotDB.setPilotStatus(pRef,
                                    pDict['Status'],
                                    pDict['DestinationSite'],
                                    pDict['StatusDate'],
                                    conn=connection)

    return S_OK()

  def _addPilotsAccountingReport(self, pilotsData):
    """ fill accounting data
    """
    for pRef in pilotsData:
      pData = pilotsData[pRef]
      pA = PilotAccounting()
      pA.setEndTime(pData['LastUpdateTime'])
      pA.setStartTime(pData['SubmissionTime'])
      retVal = Registry.getUsernameForDN(pData['OwnerDN'])
      if not retVal['OK']:
        userName = 'unknown'
        self.log.error(
            "Can't determine username for dn",
            ": %s : %s" % (pData["OwnerDN"], retVal["Message"]),
        )
      else:
        userName = retVal['Value']
      pA.setValueByKey('User', userName)
      pA.setValueByKey('UserGroup', pData['OwnerGroup'])
      result = getCESiteMapping(pData['DestinationSite'])
      if result['OK'] and pData['DestinationSite'] in result['Value']:
        pA.setValueByKey('Site', result['Value'][pData['DestinationSite']].strip())
      else:
        pA.setValueByKey('Site', 'Unknown')
      pA.setValueByKey('GridCE', pData['DestinationSite'])
      pA.setValueByKey('GridMiddleware', pData['GridType'])
      pA.setValueByKey('GridResourceBroker', pData['Broker'])
      pA.setValueByKey('GridStatus', pData['Status'])
      if 'Jobs' not in pData:
        pA.setValueByKey('Jobs', 0)
      else:
        pA.setValueByKey('Jobs', len(pData['Jobs']))
      self.log.verbose("Added accounting record for pilot %s" % pData['PilotID'])
      retVal = gDataStoreClient.addRegister(pA)
      if not retVal['OK']:
        return retVal
    return S_OK()

  def _killPilots(self, acc):
    for i in sorted(acc.keys()):
      result = self.diracadmin.getPilotInfo(i)
      if result['OK'] and i in result['Value'] and 'Status' in result['Value'][i]:
        ret = self.diracadmin.killPilot(str(i))
        if ret['OK']:
          self.log.info(
              "Successfully deleted", ": %s (Status : %s)" % (i, result["Value"][i]["Status"])
          )
        else:
          self.log.error("Failed to delete pilot: ", "%s : %s" % (i, ret['Message']))
      else:
        self.log.error("Failed to get pilot info", "%s : %s" % (i, str(result)))

  def _checkJobLastUpdateTime(self, joblist, StalledDays):
    timeLimitToConsider = Time.dateTime() - Time.day * StalledDays
    ret = False
    for jobID in joblist:
      result = self.jobDB.getJobAttributes(int(jobID))
      if result['OK']:
        if 'LastUpdateTime' in result['Value']:
          lastUpdateTime = result['Value']['LastUpdateTime']
          if Time.fromString(lastUpdateTime) > timeLimitToConsider:
            ret = True
            self.log.debug(
                'Since %s updates LastUpdateTime on %s this does not to need to be deleted.' %
                (str(jobID), str(lastUpdateTime)))
            break
      else:
        self.log.error("Error taking job info from DB", result['Message'])
    return ret
