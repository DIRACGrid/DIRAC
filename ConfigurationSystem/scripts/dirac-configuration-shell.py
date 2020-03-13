#!/usr/bin/env python

"""
Script that emulates the behaviour of a shell to edit the CS config.
"""
import sys

from DIRAC.Core.Base import Script

Script.parseCommandLine()

from DIRAC.ConfigurationSystem.Client.CSShellCLI import CSShellCLI

# Invariants:
# * root does not end with "/" or root is "/"
# * root starts with "/"


def main():
  shell = CSShellCLI()
  shell.cmdloop()


if __name__ == "__main__":
  sys.exit(main())
