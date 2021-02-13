#!/usr/bin/env python
"""
Runs checkTransformationIntegrity from ValidateOutputDataAgent on selected Tranformation

Usage:
  dirac-transformation-verify-outputdata transID [transID] [transID]
"""

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
import sys

from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.DIRACScript import DIRACScript


@DIRACScript()
def main():
  Script.parseCommandLine()

  args = Script.getPositionalArgs()
  if not args:
    Script.showHelp()

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
