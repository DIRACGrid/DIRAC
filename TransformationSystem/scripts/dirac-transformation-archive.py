#!/usr/bin/env python
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()
########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/LHCbDIRAC/trunk/LHCbDIRAC/TransformationSystem/scripts/dirac-transformation-archive.py $
########################################################################
__RCSID__   = "$Id: dirac-transformation-archive.py 29046 2010-10-05 16:27:16Z acsmith $"
__VERSION__ = "$Revision: 1.1 $"

import sys
if len(sys.argv) < 2:
  print 'Usage: dirac-transformation-archive transID [transID] [transID]'
  sys.exit()
else:
  transIDs = [int(arg) for arg in sys.argv[1:]]


from DIRAC.TransformationSystem.Agent.TransformationCleaningAgent     import TransformationCleaningAgent
from DIRAC.TransformationSystem.Client.TransformationClient           import TransformationClient
from DIRAC                                                            import gLogger
import DIRAC

agent = TransformationCleaningAgent('Transformation/TransformationCleaningAgent','dirac-transformation-archive')
agent.initialize()

client = TransformationClient()
for transID in transIDs:
  res = client.getTransformationParameters(transID,['Status'])
  if not res['OK']:
    gLogger.error("Failed to determine transformation status")
    gLogger.error(res['Message'])
    continue
  status = res['Value']
  if not status in ['Completed']:
    gLogger.error("The transformation is in %s status and can not be archived" % status)
    continue
  agent.archiveTransformation(transID)