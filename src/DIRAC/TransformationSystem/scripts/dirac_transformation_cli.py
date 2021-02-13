#!/usr/bin/env python
"""
Command to launch the Transformation Shell
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.DIRACScript import DIRACScript


@DIRACScript()
def main():
  Script.parseCommandLine(ignoreErrors=False)

  from DIRAC.TransformationSystem.Client.TransformationCLI import TransformationCLI

  cli = TransformationCLI()
  cli.cmdloop()


if __name__ == "__main__":
  main()
