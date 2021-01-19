#!/usr/bin/env python

"""
Script that emulates the behaviour of a shell to edit the CS config.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import sys

from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.DIRACScript import DIRACScript

# Invariants:
# * root does not end with "/" or root is "/"
# * root starts with "/"


@DIRACScript()
def main():
  Script.parseCommandLine()
  from DIRAC.ConfigurationSystem.Client.CSShellCLI import CSShellCLI
  shell = CSShellCLI()
  shell.cmdloop()


if __name__ == "__main__":
  main()
