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


class Params(object):
  uuid = None
  jobid = None

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


@DIRACScript()
def main(self):
  params = Params()
  self.registerSwitch('u:', 'uuid=', 'get PilotsLogging for given Pilot UUID', params.setUUID)
  self.registerSwitch('j:', 'jobid=', 'get PilotsLogging for given Job ID', params.setJobID)
  self.parseCommandLine()

  from DIRAC.WorkloadManagementSystem.Client.PilotManagerClient import PilotManagerClient

  if params.jobid:
    result = PilotManagerClient().getPilots(params.jobid)
    if not result['OK']:
      gLogger.error(result['Message'])
      DIRAC.exit(1)
    gLogger.debug(result['Value'])
    params.uuid = list(result['Value'])[0]

  result = PilotManagerClient().getPilotLoggingInfo(params.uuid)
  if not result['OK']:
    gLogger.error(result['Message'])
    DIRAC.exit(1)
  gLogger.notice(result['Value'])

  DIRAC.exit(0)


if __name__ == "__main__":
  main()  # pylint: disable=no-value-for-parameter
