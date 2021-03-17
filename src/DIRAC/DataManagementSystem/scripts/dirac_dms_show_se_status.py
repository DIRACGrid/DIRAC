#!/usr/bin/env python
"""
Get status of the available Storage Elements

Usage:
  dirac-dms-show-se-status [<options>]

Example:
  $ dirac-dms-show-se-status
  Storage Element               Read Status    Write Status
  DIRAC-USER                         Active          Active
  IN2P3-disk                         Active          Active
  IPSL-IPGP-disk                     Active          Active
  IRES-disk                        InActive        InActive
  M3PEC-disk                         Active          Active
  ProductionSandboxSE                Active          Active
"""
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

from DIRAC import S_OK, exit as DIRACexit
from DIRAC.Core.Utilities.DIRACScript import DIRACScript

__RCSID__ = "$Id$"


class ShowSEStatus(DIRACScript):

  def initParameters(self):
    """ init """
    self.vo = None
    self.allVOsFlag = False
    self.noVOFlag = False

  def setVO(self, arg):
    self.vo = arg
    return S_OK()

  def setAllVO(self, arg):
    self.allVOsFlag = True
    return S_OK()

  def setNoVO(self, arg):
    self.noVOFlag = True
    self.allVOsFlag = False
    return S_OK()


@ShowSEStatus()
def main(self):
  self.registerSwitch("V:", "vo=", "Virtual Organization", self.setVO)
  self.registerSwitch("a", "all", "All Virtual Organizations flag", self.setAllVO)
  self.registerSwitch("n", "noVO", "No Virtual Organizations assigned flag", self.setNoVO)
  self.parseCommandLine()

  from DIRAC import gConfig, gLogger
  from DIRAC.ResourceStatusSystem.Client.ResourceStatus import ResourceStatus
  from DIRAC.Core.Utilities.PrettyPrint import printTable
  from DIRAC.Core.Security.ProxyInfo import getVOfromProxyGroup

  storageCFGBase = "/Resources/StorageElements"

  res = gConfig.getSections(storageCFGBase, True)
  if not res['OK']:
    gLogger.error('Failed to get storage element info')
    gLogger.error(res['Message'])
    DIRACexit(1)

  gLogger.info("%s %s %s" % ('Storage Element'.ljust(25), 'Read Status'.rjust(15), 'Write Status'.rjust(15)))

  seList = sorted(res['Value'])

  resourceStatus = ResourceStatus()

  res = resourceStatus.getElementStatus(seList, "StorageElement")
  if not res['OK']:
    gLogger.error("Failed to get StorageElement status for %s" % str(seList))
    DIRACexit(1)

  fields = ['SE', 'ReadAccess', 'WriteAccess', 'RemoveAccess', 'CheckAccess']
  records = []

  if self.vo is None and not self.allVOsFlag:
    result = getVOfromProxyGroup()
    if not result['OK']:
      gLogger.error('Failed to determine the user VO')
      DIRACexit(1)
    self.vo = result['Value']

  for se, statusDict in res['Value'].items():

    # Check if the SE is allowed for the user VO
    if not self.allVOsFlag:
      voList = gConfig.getValue('/Resources/StorageElements/%s/VO' % se, [])
      if self.noVOFlag and voList:
        continue
      if voList and self.vo not in voList:
        continue

    record = [se]
    for status in fields[1:]:
      value = statusDict.get(status, 'Unknown')
      record.append(value)
    records.append(record)

  printTable(fields, records, numbering=False, sortField='SE')

  DIRACexit(0)


if __name__ == "__main__":
  main()  # pylint: disable=no-value-for-parameter
