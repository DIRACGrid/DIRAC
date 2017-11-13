#! /usr/bin/env python
"""
  Get Pilots Logging for specific Pilot UUID or Job ID.
"""
__RCSID__ = "$Id$"

import DIRAC
from DIRAC import S_OK
from DIRAC.Core.Base import Script
from DIRAC.WorkloadManagementSystem.Client.PilotsLoggingClient import PilotsLoggingClient
from DIRAC.WorkloadManagementSystem.DB.PilotAgentsDB import PilotAgentsDB
from DIRAC.Core.Utilities.PrettyPrint import printTable

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


def printPilotsLogging(logs):
  """
  Print results using printTable from PrettyPrint
  """
  content = []
  labels = ['pilotUUID', 'timestamp', 'source', 'phase', 'status', 'messageContent']
  for log in logs:
    content.append([log[label] for label in labels])
  printTable(labels, content, numbering=False, columnSeparator=' | ')


if uuid:
  pilotsLogging = PilotsLoggingClient()
  result = pilotsLogging.getPilotsLogging(uuid)
  if not result['OK']:
    print 'ERROR: %s' % result['Message']
    DIRAC.exit(1)
  printPilotsLogging(result['Value'])
  DIRAC.exit(0)
else:
  pilotDB = PilotAgentsDB()
  pilotsLogging = PilotsLoggingClient()
  pilots = pilotDB.getPilotsForJobID(jobid)
  if not pilots['OK ']:
    print pilots['Message']
  for pilotID in pilots:
    info = pilotDB.getPilotInfo(pilotID=pilotID)
    if not info['OK']:
      print info['Message']
    for pilot in info:
      logging = pilotsLogging.getPilotsLogging(pilot['PilotJobReference'])
      if not logging['OK']:
        print logging['Message']
      printPilotsLogging(logging)
  DIRAC.exit(0)
