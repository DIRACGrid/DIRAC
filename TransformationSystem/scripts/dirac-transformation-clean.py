#!/usr/bin/env python
""" Clean a tranformation
"""

import sys

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC.TransformationSystem.Agent.TransformationCleaningAgent     import TransformationCleaningAgent
from DIRAC.TransformationSystem.Client.TransformationClient         import TransformationClient

if len( sys.argv ) < 2:
  print 'Usage: dirac-transformation-clean transID [transID] [transID]'
  sys.exit()
else:
  transIDs = [int( arg ) for arg in sys.argv[1:]]


agent = TransformationCleaningAgent( 'Transformation/TransformationCleaningAgent',
                                     'Transformation/TransformationCleaningAgent',
                                     'dirac-transformation-clean' )
agent.initialize()

client = TransformationClient()
for transID in transIDs:
  agent.cleanTransformation( transID )
