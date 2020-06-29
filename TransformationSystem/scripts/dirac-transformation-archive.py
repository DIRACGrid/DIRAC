#!/usr/bin/env python
""" Archive a transformation
"""

from __future__ import print_function
import sys

from DIRAC.Core.Base.Script import parseCommandLine, getPositionalArgs
parseCommandLine()

if not getPositionalArgs():
  print('Usage: dirac-transformation-archive transID [transID] [transID]')
  sys.exit()
else:
  transIDs = [int(arg) for arg in getPositionalArgs()]

from DIRAC.TransformationSystem.Agent.TransformationCleaningAgent import TransformationCleaningAgent
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient

agent = TransformationCleaningAgent('Transformation/TransformationCleaningAgent',
                                    'Transformation/TransformationCleaningAgent',
                                    'dirac-transformation-archive')
agent.initialize()

client = TransformationClient()
for transID in transIDs:
  agent.archiveTransformation(transID)
