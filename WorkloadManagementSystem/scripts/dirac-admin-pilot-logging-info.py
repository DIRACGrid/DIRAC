#! /usr/bin/env python
"""
  Get Pilots Logging for specific Pilot UUID or Job ID.
"""
from __future__ import print_function
__RCSID__ = "$Id$"

import DIRAC
from DIRAC import S_OK, gLogger
from DIRAC.Core.Base import Script

uuid = None
jobid = None


def setUUID(optVal):
  """
  Set UUID from arguments
  """
  global uuid
  uuid = optVal
  return S_OK()


def setJobID(optVal):
  """
  Set JobID from arguments
  """
  global jobid
  jobid = optVal
  return S_OK()


Script.registerSwitch('u:', 'uuid=', 'get PilotsLogging for given Pilot UUID', setUUID)
Script.registerSwitch('j:', 'jobid=', 'get PilotsLogging for given Job ID', setJobID)

Script.setUsageMessage('\n'.join([__doc__.split('\n')[1],
                                  'Usage:',
                                  '  %s option value ' % Script.scriptName,
                                  'Only one option (either uuid or jobid) should be used.']))

Script.parseCommandLine()

from DIRAC.WorkloadManagementSystem.Client.PilotManagerClient import PilotManagerClient

if jobid:
  result = PilotManagerClient().getPilots(jobid)
  if not result['OK']:
    gLogger.error(result['Message'])
    DIRAC.exit(1)
  gLogger.debug(result['Value'])
  uuid = result['Value'].keys()[0]

result = PilotManagerClient().getPilotLoggingInfo(uuid)
if not result['OK']:
  gLogger.error(result['Message'])
  DIRAC.exit(1)
gLogger.notice(result['Value'])

DIRAC.exit(0)
