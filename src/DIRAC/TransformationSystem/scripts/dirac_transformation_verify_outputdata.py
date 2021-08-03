#!/usr/bin/env python
"""
Runs checkTransformationIntegrity from ValidateOutputDataAgent on selected Tranformation
"""

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

__RCSID__ = "$Id$"

import sys

from DIRAC.Core.Utilities.DIRACScript import DIRACScript as Script


@Script()
def main():
  # Registering arguments will automatically add their description to the help menu
  Script.registerArgument(["transID: transformation ID"])
  _, args = Script.parseCommandLine()

  transIDs = [int(arg) for arg in args]

  from DIRAC.TransformationSystem.Agent.ValidateOutputDataAgent import ValidateOutputDataAgent
  from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient

  agent = ValidateOutputDataAgent('Transformation/ValidateOutputDataAgent',
                                  'Transformation/ValidateOutputDataAgent',
                                  'dirac-transformation-verify-outputdata')
  agent.initialize()

  client = TransformationClient()
  for transID in transIDs:
    agent.checkTransformationIntegrity(transID)


if __name__ == "__main__":
  main()
