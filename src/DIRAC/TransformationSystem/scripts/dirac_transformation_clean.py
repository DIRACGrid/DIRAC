#!/usr/bin/env python
"""
Clean a tranformation

Usage:
  dirac-transformation-clean transID [transID] [transID]
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

  from DIRAC.TransformationSystem.Agent.TransformationCleaningAgent import TransformationCleaningAgent
  from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient

  transIDs = [int(arg) for arg in args]

  agent = TransformationCleaningAgent('Transformation/TransformationCleaningAgent',
                                      'Transformation/TransformationCleaningAgent',
                                      'dirac-transformation-clean')
  agent.initialize()

  client = TransformationClient()
  for transID in transIDs:
    agent.cleanTransformation(transID)


if __name__ == "__main__":
  main()
