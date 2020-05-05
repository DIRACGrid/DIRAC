#!/bin/env python
"""
Create a production to replicate files from some storage elements to others

:since:  May 31, 2018
:author: A. Sailer
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from DIRAC.Core.Base import Script
from DIRAC import gLogger, exit as dexit
from DIRAC.TransformationSystem.Utilities.ReplicationCLIParameters import Params
from DIRAC.TransformationSystem.Utilities.ReplicationTransformation import createDataTransformation

__RCSID__ = "$Id$"


def _createTrafo():
  """reads command line parameters, makes check and creates replication transformation"""
  clip = Params()
  clip.registerSwitches(Script)
  Script.parseCommandLine()
  if not clip.checkSettings(Script)['OK']:
    gLogger.error("ERROR: Missing settings")
    return 1
  for metaValue in clip.metaValues:
    resCreate = createDataTransformation(flavour=clip.flavour,
                                         targetSE=clip.targetSE,
                                         sourceSE=clip.sourceSE,
                                         metaKey=clip.metaKey,
                                         metaValue=metaValue,
                                         extraData=clip.extraData,
                                         extraname=clip.extraname,
                                         groupSize=clip.groupSize,
                                         tGroup=clip.groupName,
                                         plugin=clip.plugin,
                                         enable=clip.enable,
                                         )
    if not resCreate['OK']:
      gLogger.error("Failed to create Transformation", resCreate['Message'])
      return 1

  return 0


if __name__ == '__main__':
  dexit(_createTrafo())
