#!/bin/env python
"""
Create a production to replicate files from some storage elements to others

:since:  May 31, 2018
:author: A. Sailer
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

from DIRAC.TransformationSystem.Utilities.ReplicationCLIParameters import Params


@Params()
def main(self):
  """reads command line parameters, makes check and creates replication transformation"""
  from DIRAC import gLogger, exit as dexit

  self.registerSwitches()
  self.parseCommandLine()

  from DIRAC.TransformationSystem.Utilities.ReplicationTransformation import createDataTransformation

  if not self.checkSettings()['OK']:
    gLogger.error("ERROR: Missing settings")
    dexit(1)
  for metaValue in self.metaValues:
    resCreate = createDataTransformation(flavour=self.flavour,
                                         targetSE=self.targetSE,
                                         sourceSE=self.sourceSE,
                                         metaKey=self.metaKey,
                                         metaValue=metaValue,
                                         extraData=self.extraData,
                                         extraname=self.extraname,
                                         groupSize=self.groupSize,
                                         tGroup=self.groupName,
                                         plugin=self.plugin,
                                         enable=self.enable,
                                         )
    if not resCreate['OK']:
      gLogger.error("Failed to create Transformation", resCreate['Message'])
      dexit(1)

  dexit(0)


if __name__ == "__main__":
  main()  # pylint: disable=no-value-for-parameter
