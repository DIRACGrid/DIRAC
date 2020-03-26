#!/usr/bin/env python
""" runs checkTransformationIntegrity from ValidateOutputDataAgent on selected Tranformation
"""

from __future__ import print_function
import sys

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

if len(sys.argv) < 2:
  print('Usage: dirac-transformation-verify-outputdata transID [transID] [transID]')
  sys.exit()
else:
  transIDs = [int(arg) for arg in sys.argv[1:]]

from DIRAC.TransformationSystem.Agent.ValidateOutputDataAgent import ValidateOutputDataAgent
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient

agent = ValidateOutputDataAgent('Transformation/ValidateOutputDataAgent',
                                'Transformation/ValidateOutputDataAgent',
                                'dirac-transformation-verify-outputdata')
agent.initialize()

client = TransformationClient()
for transID in transIDs:
  agent.checkTransformationIntegrity(transID)
