#!/bin/env python
"""
Script to call the DataRecoveryAgent functionality by hand.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from DIRAC import S_OK, gLogger
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.DIRACScript import DIRACScript

__RCSID__ = '$Id$'


class Params(object):
  """Collection of Parameters set via CLI switches."""

  def __init__(self):
    self.enabled = False
    self.transID = 0

  def setEnabled(self, _):
    self.enabled = True
    return S_OK()

  def setTransID(self, transID):
    self.transID = int(transID)
    return S_OK()

  def registerSwitches(self):
    Script.registerSwitch('T:', 'TransID=', 'TransID to Check/Fix', self.setTransID)
    Script.registerSwitch('X', 'Enabled', 'Enable the changes', self.setEnabled)


@DIRACScript()
def main():
  PARAMS = Params()
  PARAMS.registerSwitches()
  Script.parseCommandLine(ignoreErrors=False)

  # Create Data Recovery Agent and run over single transformation.
  from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
  from DIRAC.TransformationSystem.Agent.DataRecoveryAgent import DataRecoveryAgent
  DRA = DataRecoveryAgent('Transformation/DataRecoveryAgent', 'Transformation/DataRecoveryAgent')
  DRA.jobStatus = ['Done', 'Failed']
  DRA.enabled = PARAMS.enabled
  TRANSFORMATION = TransformationClient().getTransformations(condDict={'TransformationID': PARAMS.transID})
  if not TRANSFORMATION['OK']:
    gLogger.error('Failed to find transformation: %s' % TRANSFORMATION['Message'])
    exit(1)
  if not TRANSFORMATION['Value']:
    gLogger.error('Did not find any transformations')
    exit(1)
  TRANS_INFO_DICT = TRANSFORMATION['Value'][0]
  TRANS_INFO_DICT.pop('Body', None)
  gLogger.notice('Found transformation: %s' % TRANS_INFO_DICT)
  DRA.treatTransformation(PARAMS.transID, TRANS_INFO_DICT)
  exit(0)


if __name__ == "__main__":
  main()
