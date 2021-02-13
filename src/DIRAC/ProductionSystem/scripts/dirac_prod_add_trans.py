#!/usr/bin/env python
"""
Add an existing transformation to an existing production.
Transformations already belonging to another production cannot be added.

Usage:
  dirac-prod-add-trans prodID transID [parentTransID]

Arguments:
  prodID:         Production ID
  transID:        Transformation ID
  parentTransID:  Parent Transformation ID
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.DIRACScript import DIRACScript


@DIRACScript()
def main():
  Script.parseCommandLine()

  from DIRAC.ProductionSystem.Client.ProductionClient import ProductionClient
  from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient

  prodClient = ProductionClient()
  transClient = TransformationClient()

  # get arguments
  args = Script.getPositionalArgs()
  if len(args) == 3:
    parentTransID = args[2]
  elif len(args) == 2:
    parentTransID = ''
  else:
    Script.showHelp(exitCode=1)

  prodID = args[0]
  transID = args[1]

  res = transClient.getTransformation(transID)
  if not res['OK']:
    DIRAC.gLogger.error('Failed to get transformation %s: %s' % (transID, res['Message']))
    DIRAC.exit(-1)

  transID = res['Value']['TransformationID']

  if parentTransID:
    res = transClient.getTransformation(parentTransID)
    if not res['OK']:
      DIRAC.gLogger.error('Failed to get transformation %s: %s' % (parentTransID, res['Message']))
      DIRAC.exit(-1)
    parentTransID = res['Value']['TransformationID']

  res = prodClient.getProduction(prodID)
  if not res['OK']:
    DIRAC.gLogger.error('Failed to get production %s: %s' % (prodID, res['Message']))
    DIRAC.exit(-1)

  prodID = res['Value']['ProductionID']
  res = prodClient.addTransformationsToProduction(prodID, transID, parentTransID)
  if not res['OK']:
    DIRAC.gLogger.error(res['Message'])
    DIRAC.exit(-1)

  if parentTransID:
    msg = 'Transformation %s successfully added to production %s with parent transformation %s' % \
          (transID, prodID, parentTransID)
  else:
    msg = 'Transformation %s successfully added to production %s with no parent transformation' %  \
          (transID, prodID)

  DIRAC.gLogger.notice(msg)

  DIRAC.exit(0)


if __name__ == "__main__":
  main()
