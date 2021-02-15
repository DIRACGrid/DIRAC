#!/usr/bin/env python
"""
Archive a transformation

Usage:
  dirac-transformation-archive transID [transID] [transID]
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

  from DIRAC.TransformationSystem.Agent.TransformationCleaningAgent import TransformationCleaningAgent
  from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient

  agent = TransformationCleaningAgent('Transformation/TransformationCleaningAgent',
                                      'Transformation/TransformationCleaningAgent',
                                      'dirac-transformation-archive')
  agent.initialize()

  client = TransformationClient()
  for transID in transIDs:
    agent.archiveTransformation(transID)


if __name__ == "__main__":
  main()
