#!/usr/bin/env python
"""
Script that synchronizes the resources described on the CS with the RSS.
By default, it sets their Status to `Unknown`, StatusType to `all` and
reason to `Synchronized`. However, it can copy over the status on the CS to
the RSS. Important: If the StatusType is not defined on the CS, it will set
it to Banned !

Usage:
  dirac-rss-sync [options]

Verbosity::

  -o LogLevel=LEVEL     NOTICE by default, levels available: INFO, DEBUG, VERBOSE..
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = '$Id$'

from DIRAC import version, gLogger, exit as DIRACExit, S_OK
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.DIRACScript import DIRACScript

subLogger = None
switchDict = {}
DEFAULT_STATUS = ""


def registerSwitches():
  '''
    Registers all switches that can be used while calling the script from the
    command line interface.
  '''

  Script.registerSwitch('', 'init', 'Initialize the element to the status in the CS ( applicable for StorageElements )')
  Script.registerSwitch('', 'element=', 'Element family to be Synchronized ( Site, Resource or Node ) or `all`')
  Script.registerSwitch('', 'defaultStatus=', 'Default element status if not given in the CS')


def registerUsageMessage():
  '''
    Takes the script __doc__ and adds the DIRAC version to it
  '''
  usageMessage = 'DIRAC %s\n' % version
  usageMessage += __doc__

  Script.setUsageMessage(usageMessage)


def parseSwitches():
  '''
    Parses the arguments passed by the user
  '''

  Script.parseCommandLine(ignoreErrors=True)
  args = Script.getPositionalArgs()
  if args:
    subLogger.error("Found the following positional args '%s', but we only accept switches" % args)
    subLogger.error("Please, check documentation below")
    Script.showHelp(exitCode=1)

  switches = dict(Script.getUnprocessedSwitches())

  # Default values
  switches.setdefault('element', None)
  switches.setdefault('defaultStatus', 'Banned')
  if not switches['element'] in ('all', 'Site', 'Resource', 'Node', None):
    subLogger.error("Found %s as element switch" % switches['element'])
    subLogger.error("Please, check documentation below")
    Script.showHelp(exitCode=1)

  subLogger.debug("The switches used are:")
  map(subLogger.debug, switches.items())

  return switches


def synchronize():
  '''
    Given the element switch, adds rows to the <element>Status tables with Status
    `Unknown` and Reason `Synchronized`.
  '''
  global DEFAULT_STATUS

  from DIRAC.ResourceStatusSystem.Utilities import Synchronizer

  synchronizer = Synchronizer.Synchronizer(defaultStatus=DEFAULT_STATUS)

  if switchDict['element'] in ('Site', 'all'):
    subLogger.info('Synchronizing Sites')
    res = synchronizer._syncSites()
    if not res['OK']:
      return res

  if switchDict['element'] in ('Resource', 'all'):
    subLogger.info('Synchronizing Resource')
    res = synchronizer._syncResources()
    if not res['OK']:
      return res

  if switchDict['element'] in ('Node', 'all'):
    subLogger.info('Synchronizing Nodes')
    res = synchronizer._syncNodes()
    if not res['OK']:
      return res

  return S_OK()


def initSites():
  '''
    Initializes Sites statuses taking their values from the "SiteMask" table of "JobDB" database.
  '''
  from DIRAC.WorkloadManagementSystem.Client.WMSAdministratorClient import WMSAdministratorClient
  from DIRAC.ResourceStatusSystem.Client import ResourceStatusClient

  rssClient = ResourceStatusClient.ResourceStatusClient()

  sites = WMSAdministratorClient().getAllSiteMaskStatus()

  if not sites['OK']:
    subLogger.error(sites['Message'])
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
      subLogger.error(result['Message'])
      DIRACExit(1)

  return S_OK()


def initSEs():
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

  subLogger.info('Initializing SEs')

  rssClient = ResourceStatusClient.ResourceStatusClient()

  statuses = StateMachine.RSSMachine(None).getStates()
  statusTypes = RssConfiguration.RssConfiguration().getConfigStatusType('StorageElement')
  reason = 'dirac-rss-sync'

  subLogger.debug(statuses)
  subLogger.debug(statusTypes)

  for se in DMSHelpers().getStorageElements():

    subLogger.debug(se)

    opts = gConfig.getOptionsDict('/Resources/StorageElements/%s' % se)
    if not opts['OK']:
      subLogger.warn(opts['Message'])
      continue
    opts = opts['Value']

    subLogger.debug(opts)

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
        subLogger.error('%s not a valid status for %s - %s' % (status, se, statusType))
        continue

      # We remove from the backtracking
      statusTypesList.remove(statusType)

      subLogger.debug([se, statusType, status, reason])
      result = rssClient.addOrModifyStatusElement('Resource', 'Status',
                                                  name=se,
                                                  statusType=statusType,
                                                  status=status,
                                                  elementType='StorageElement',
                                                  tokenOwner='rs_svc',
                                                  reason=reason)

      if not result['OK']:
        subLogger.error('Failed to modify')
        subLogger.error(result['Message'])
        continue

    # Backtracking: statusTypes not present on CS
    for statusType in statusTypesList:

      result = rssClient.addOrModifyStatusElement('Resource', 'Status',
                                                  name=se,
                                                  statusType=statusType,
                                                  status=DEFAULT_STATUS,
                                                  elementType='StorageElement',
                                                  tokenOwner='rs_svc',
                                                  reason=reason)
      if not result['OK']:
        subLogger.error('Error in backtracking for %s,%s,%s' % (se, statusType, status))
        subLogger.error(result['Message'])

  return S_OK()


def run():
  '''
    Main function of the script
  '''

  result = synchronize()
  if not result['OK']:
    subLogger.error(result['Message'])
    DIRACExit(1)

  if 'init' in switchDict:

    if switchDict.get('element') == "Site":
      result = initSites()
      if not result['OK']:
        subLogger.error(result['Message'])
        DIRACExit(1)

    if switchDict.get('element') == "Resource":
      result = initSEs()
      if not result['OK']:
        subLogger.error(result['Message'])
        DIRACExit(1)


@DIRACScript()
def main():
  global subLogger
  global switchDict
  global DEFAULT_STATUS

  subLogger = gLogger.getSubLogger(__file__)
  registerSwitches()
  registerUsageMessage()
  switchDict = parseSwitches()
  DEFAULT_STATUS = switchDict.get('defaultStatus', 'Banned')

  # Run script
  run()

  # Bye
  DIRACExit(0)


if __name__ == "__main__":
  main()
