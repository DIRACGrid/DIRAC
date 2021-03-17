#! /usr/bin/env python
"""
Get Pilots Logging for specific Pilot UUID or Job ID.

WARNING: Only one option (either uuid or jobid) should be used.
"""
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

__RCSID__ = "$Id$"

import DIRAC
from DIRAC import S_OK, gLogger
from DIRAC.Core.Utilities.DIRACScript import DIRACScript


class PilotLoggingInfo(DIRACScript):

  def initParameters(self):
    self.uuid = None
    self.jobid = None

  def setUUID(self, optVal):
    """
    Set UUID from arguments
    """
    self.uuid = optVal
    return S_OK()

  def setJobID(self, optVal):
    """
    Set JobID from arguments
    """
    self.jobid = optVal
    return S_OK()


@PilotLoggingInfo()
def main(self):
  self.registerSwitch('u:', 'uuid=', 'get PilotsLogging for given Pilot UUID', self.setUUID)
  self.registerSwitch('j:', 'jobid=', 'get PilotsLogging for given Job ID', self.setJobID)
  self.parseCommandLine()

  from DIRAC.WorkloadManagementSystem.Client.PilotManagerClient import PilotManagerClient

  if self.jobid:
    result = PilotManagerClient().getPilots(self.jobid)
    if not result['OK']:
      gLogger.error(result['Message'])
      DIRAC.exit(1)
    gLogger.debug(result['Value'])
    self.uuid = list(result['Value'])[0]

  result = PilotManagerClient().getPilotLoggingInfo(self.uuid)
  if not result['OK']:
    gLogger.error(result['Message'])
    DIRAC.exit(1)
  gLogger.notice(result['Value'])

  DIRAC.exit(0)


if __name__ == "__main__":
  main()  # pylint: disable=no-value-for-parameter
