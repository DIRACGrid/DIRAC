#!/usr/bin/env python
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

import sys
if len( sys.argv ) < 2:
  print 'Usage: dirac-transformation-archive transID [transID] [transID]'
  sys.exit()
else:
  transIDs = [int( arg ) for arg in sys.argv[1:]]


from DIRAC.TransformationSystem.Agent.TransformationCleaningAgent     import TransformationCleaningAgent
from DIRAC.TransformationSystem.Client.TransformationClient           import TransformationClient
from DIRAC                                                            import gLogger
import DIRAC

agent = TransformationCleaningAgent( 'Transformation/TransformationCleaningAgent',
                                     'Transformation/TransformationCleaningAgent',
                                     'dirac-transformation-archive' )
agent.initialize()

client = TransformationClient()
for transID in transIDs:
  agent.archiveTransformation( transID )
