#!/bin/env python
"""
Create a production to replicate files from some storage elments to others

Example::

  dirac-ilc-replication-transformation <MetaValue> <TargetSEs> -S<SourceSEs> -G<GroupSize> -NExtraName

Options:
   -G, --GroupSize <value>     Number of Files per transformation task
   -S, --SourceSEs <value>     SourceSE(s) to use
   -N, --Extraname <value>    String to append to transformation name in case one already exists with that name
   -T, --TransformationType <value>  Which transformation type to use, default 'Replication'
   -K, --MetaKey <value>       Which MetaKey to use
   -M, --MetaData <value>      Key-Value Pair to use in addition to the default MetaKey and Value


:since:  May 31, 2018
:author: A. Sailer
"""
from DIRAC.Core.Base import Script
from DIRAC import gLogger, exit as dexit

__RCSID__ = "$Id$"


def _createTrafo():
  """reads command line parameters, makes check and creates replication transformation"""
  from DIRAC.TransformationSystem.Utilities.ReplicationCLIParameters import Params
  clip = Params()
  clip.registerSwitches(Script)
  Script.parseCommandLine()
  if not clip.checkSettings(Script)['OK']:
    gLogger.error("ERROR: Missing settings")
    return 1
  from DIRAC.TransformationSystem.Utilities.ReplicationTransformation import createDataTransformation
  for metaValue in clip.metaValues:
    resCreate = createDataTransformation(transformationType=clip.transformationType,
                                         targetSE=clip.targetSE,
                                         sourceSE=clip.sourceSE,
                                         metaKey=clip.metaKey,
                                         metaValue=metaValue,
                                         extraData=clip.extraData,
                                         extraname=clip.extraname,
                                         groupSize=clip.groupSize,
                                         plugin=clip.plugin,
                                         enable=clip.enable,
                                         )
    if not resCreate['OK']:
      gLogger.error("Failed to create transformation", resCreate['Message'])
      return 1

  return 0


if __name__ == '__main__':
  dexit(_createTrafo())
