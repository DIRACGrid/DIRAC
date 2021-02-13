#!/usr/bin/env python
"""
Get computing resources capable to execute a job with the given description.

Note that only statically defined computing resource parameters are considered although sites
can fail matching due to their dynamic state, e.g. occupancy by other jobs. Also input data
proximity is not taken into account.

Usage:
  dirac-wms-match [option]... <job_JDL>
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from DIRAC.Core.Base import Script
from DIRAC import S_OK, gLogger, exit as DIRACExit
from DIRAC.Core.Utilities.DIRACScript import DIRACScript

__RCSID__ = "$Id$"

fullMatch = False
sites = None


def setFullMatch(optVal_):
  global fullMatch
  fullMatch = True
  return S_OK()


def setSites(optVal_):
  global sites
  sites = optVal_.split(',')
  return S_OK()


@DIRACScript()
def main():
  global fullMatch
  global sites
  Script.registerSwitch("F", "full-match", "Check all the matching criteria", setFullMatch)
  Script.registerSwitch("S:", "site=", "Check matching for these sites (comma separated list)", setSites)

  Script.parseCommandLine(ignoreErrors=True)
  args = Script.getPositionalArgs()

  if len(args) == 0:
    gLogger.error("Error: No job description provided")
    Script.showHelp(exitCode=1)

  from DIRAC.Core.Security.ProxyInfo import getVOfromProxyGroup
  from DIRAC.ConfigurationSystem.Client.Helpers import Resources
  from DIRAC.Core.Utilities.PrettyPrint import printTable
  from DIRAC.ResourceStatusSystem.Client.ResourceStatus import ResourceStatus
  from DIRAC.ResourceStatusSystem.Client.SiteStatus import SiteStatus
  from DIRAC.WorkloadManagementSystem.Utilities.QueueUtilities import getQueuesResolved, matchQueue

  with open(args[0]) as f:
    jdl = f.read()

  # Get the current VO
  result = getVOfromProxyGroup()
  if not result['OK']:
    gLogger.error('No proxy found, please login')
    DIRACExit(-1)
  voName = result['Value']

  resultQueues = Resources.getQueues(siteList=sites, community=voName)
  if not resultQueues['OK']:
    gLogger.error('Failed to get CE information')
    DIRACExit(-1)
  siteDict = resultQueues['Value']
  result = getQueuesResolved(siteDict)
  if not resultQueues['OK']:
    gLogger.error('Failed to get CE information')
    DIRACExit(-1)
  queueDict = result['Value']

  # get list of usable sites within this cycle
  resultMask = SiteStatus().getUsableSites()
  if not resultMask['OK']:
    gLogger.error('Failed to get Site mask information')
    DIRACExit(-1)
  siteMaskList = resultMask.get('Value', [])

  rssClient = ResourceStatus()

  fields = ('Site', 'CE', 'Queue', 'Status', 'Match', 'Reason')
  records = []

  for queue, queueInfo in queueDict.items():
    site = queueInfo['Site']
    ce = queueInfo['CEName']
    siteStatus = "Active" if site in siteMaskList else "InActive"
    ceStatus = siteStatus
    if rssClient.rssFlag:
      result = rssClient.getElementStatus(ce, "ComputingElement")
      if result['OK']:
        ceStatus = result['Value'][ce]['all']

    result = matchQueue(jdl, queueInfo, fullMatch=fullMatch)
    if not result['OK']:
      gLogger.error('Failed in getting match data', result['Message'])
      DIRACExit(-1)
    status = "Active" if siteStatus == "Active" and ceStatus == "Active" else "Inactive"
    if result['Value']['Match']:
      records.append((site, ce, queueInfo['Queue'], status, 'Yes', ''))
    else:
      records.append((site, ce, queueInfo['Queue'], status, 'No', result['Value']['Reason']))

  gLogger.notice(printTable(fields, records, sortField='Site', columnSeparator='  ', printOut=False))


if __name__ == "__main__":
  main()
