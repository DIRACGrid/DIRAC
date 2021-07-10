#!/usr/bin/env python
"""
Script that synchronizes the resources described on the CS with the RSS.
By default, it sets their Status to `Unknown`, StatusType to `all` and
reason to `Synchronized`. However, it can copy over the status on the CS to
the RSS. Important: If the StatusType is not defined on the CS, it will set
it to Banned !
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = '$Id$'

from DIRAC import version, gLogger, exit as DIRACExit, S_OK
from DIRAC.Core.Utilities.DIRACScript import DIRACScript as _DIRACScript


class DIRACScript(_DIRACScript):

  def initParameters(self):
    self.subLogger = gLogger.getSubLogger(__file__)
    self.switchDict = {}
    self.DEFAULT_STATUS = ""

  def registerSwitches(self):
    '''
      Registers all switches that can be used while calling the script from the
      command line interface.
    '''

    self.registerSwitch('', 'init', 'Initialize the element to the status in the CS ( applicable for StorageElements )')
    self.registerSwitch('', 'element=', 'Element family to be Synchronized ( Site, Resource or Node ) or `all`')
    self.registerSwitch('', 'defaultStatus=', 'Default element status if not given in the CS')

  def registerUsageMessage(self):
    '''
      Takes the script __doc__ and adds the DIRAC version to it
    '''
    usageMessage = 'DIRAC %s\n' % version
    usageMessage += __doc__

    self.setUsageMessage(usageMessage)

  def parseSwitches(self):
    '''
      Parses the arguments passed by the user
    '''

    switches, args = self.parseCommandLine(ignoreErrors=True)
    if args:
      self.subLogger.error("Found the following positional args '%s', but we only accept switches" % args)
      self.subLogger.error("Please, check documentation below")
      self.showHelp(exitCode=1)

    switches = dict(switches)

    # Default values
    switches.setdefault('element', None)
    switches.setdefault('defaultStatus', 'Banned')
    if not switches['element'] in ('all', 'Site', 'Resource', 'Node', None):
      self.subLogger.error("Found %s as element switch" % switches['element'])
      self.subLogger.error("Please, check documentation below")
      self.showHelp(exitCode=1)

    self.subLogger.debug("The switches used are:")
    map(self.subLogger.debug, switches.items())

    return switches

  def synchronize(self):
    '''
      Given the element switch, adds rows to the <element>Status tables with Status
      `Unknown` and Reason `Synchronized`.
    '''
    from DIRAC.ResourceStatusSystem.Utilities import Synchronizer

    synchronizer = Synchronizer.Synchronizer(defaultStatus=self.DEFAULT_STATUS)

    if self.switchDict['element'] in ('Site', 'all'):
      self.subLogger.info('Synchronizing Sites')
      res = synchronizer._syncSites()
      if not res['OK']:
        return res

    if self.switchDict['element'] in ('Resource', 'all'):
      self.subLogger.info('Synchronizing Resource')
      res = synchronizer._syncResources()
      if not res['OK']:
        return res

    if self.switchDict['element'] in ('Node', 'all'):
      self.subLogger.info('Synchronizing Nodes')
      res = synchronizer._syncNodes()
      if not res['OK']:
        return res

    return S_OK()

  def initSites(self):
    '''
      Initializes Sites statuses taking their values from the "SiteMask" table of "JobDB" database.
    '''
    from DIRAC.WorkloadManagementSystem.Client.WMSAdministratorClient import WMSAdministratorClient
    from DIRAC.ResourceStatusSystem.Client import ResourceStatusClient

    rssClient = ResourceStatusClient.ResourceStatusClient()

    sites = WMSAdministratorClient().getAllSiteMaskStatus()

    if not sites['OK']:
      self.subLogger.error(sites['Message'])
      DIRACExit(1)

    for site, elements in sites['Value'].items():
      result = rssClient.addOrModifyStatusElement("Site", "Status",
                                                  name=site,
                                                  statusType='all',
                                                  status=elements[0],
                                                  elementType=site.split('.')[0],
                                                  tokenOwner='rs_svc',
                                                  reason='dirac-rss-sync')
      if not result['OK']:
        self.subLogger.error(result['Message'])
        DIRACExit(1)

    return S_OK()

  def initSEs(self):
    '''
      Initializes SEs statuses taking their values from the CS.
    '''
    from DIRAC import gConfig
    from DIRAC.DataManagementSystem.Utilities.DMSHelpers import DMSHelpers
    from DIRAC.ResourceStatusSystem.Utilities import CSHelpers, RssConfiguration
    from DIRAC.ResourceStatusSystem.PolicySystem import StateMachine
    from DIRAC.ResourceStatusSystem.Client import ResourceStatusClient

    # WarmUp local copy
    CSHelpers.warmUp()

    self.subLogger.info('Initializing SEs')

    rssClient = ResourceStatusClient.ResourceStatusClient()

    statuses = StateMachine.RSSMachine(None).getStates()
    statusTypes = RssConfiguration.RssConfiguration().getConfigStatusType('StorageElement')
    reason = 'dirac-rss-sync'

    self.subLogger.debug(statuses)
    self.subLogger.debug(statusTypes)

    for se in DMSHelpers().getStorageElements():

      self.subLogger.debug(se)

      opts = gConfig.getOptionsDict('/Resources/StorageElements/%s' % se)
      if not opts['OK']:
        self.subLogger.warn(opts['Message'])
        continue
      opts = opts['Value']

      self.subLogger.debug(opts)

      # We copy the list into a new object to remove items INSIDE the loop !
      statusTypesList = statusTypes[:]

      for statusType, status in opts.items():

        # Sanity check...
        if statusType not in statusTypesList:
          continue

        # Transforms statuses to RSS terms
        if status in ('NotAllowed', 'InActive'):
          status = 'Banned'

        if status not in statuses:
          self.subLogger.error('%s not a valid status for %s - %s' % (status, se, statusType))
          continue

        # We remove from the backtracking
        statusTypesList.remove(statusType)

        self.subLogger.debug([se, statusType, status, reason])
        result = rssClient.addOrModifyStatusElement('Resource', 'Status',
                                                    name=se,
                                                    statusType=statusType,
                                                    status=status,
                                                    elementType='StorageElement',
                                                    tokenOwner='rs_svc',
                                                    reason=reason)

        if not result['OK']:
          self.subLogger.error('Failed to modify')
          self.subLogger.error(result['Message'])
          continue

      # Backtracking: statusTypes not present on CS
      for statusType in statusTypesList:

        result = rssClient.addOrModifyStatusElement('Resource', 'Status',
                                                    name=se,
                                                    statusType=statusType,
                                                    status=self.DEFAULT_STATUS,
                                                    elementType='StorageElement',
                                                    tokenOwner='rs_svc',
                                                    reason=reason)
        if not result['OK']:
          self.subLogger.error('Error in backtracking for %s,%s,%s' % (se, statusType, status))
          self.subLogger.error(result['Message'])

    return S_OK()

  def run(self):
    '''
      Main function of the script
    '''

    result = self.synchronize()
    if not result['OK']:
      self.subLogger.error(result['Message'])
      DIRACExit(1)

    if 'init' in self.switchDict:

      if self.switchDict.get('element') == "Site":
        result = self.initSites()
        if not result['OK']:
          self.subLogger.error(result['Message'])
          DIRACExit(1)

      if self.switchDict.get('element') == "Resource":
        result = self.initSEs()
        if not result['OK']:
          self.subLogger.error(result['Message'])
          DIRACExit(1)


@DIRACScript()
def main(self):
  self.registerSwitches()
  self.registerUsageMessage()
  self.switchDict = self.parseSwitches()
  self.DEFAULT_STATUS = self.switchDict.get('defaultStatus', 'Banned')

  # Run script
  self.run()

  # Bye
  DIRACExit(0)


if __name__ == "__main__":
  main()  # pylint: disable=no-value-for-parameter
