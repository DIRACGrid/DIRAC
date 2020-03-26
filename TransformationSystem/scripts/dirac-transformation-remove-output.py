#!/usr/bin/env python
""" Remove the outputs produced by a transformation
"""

from __future__ import print_function
import sys

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

if len(sys.argv) < 2:
  print('Usage: dirac-transformation-remove-output transID [transID] [transID]')
  sys.exit()
else:
  transIDs = [int(arg) for arg in sys.argv[1:]]

from DIRAC.TransformationSystem.Agent.TransformationCleaningAgent import TransformationCleaningAgent
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient

agent = TransformationCleaningAgent('Transformation/TransformationCleaningAgent',
                                    'Transformation/TransformationCleaningAgent',
                                    'dirac-transformation-remove-output')
agent.initialize()

client = TransformationClient()
for transID in transIDs:
  agent.removeTransformationOutput(transID)
