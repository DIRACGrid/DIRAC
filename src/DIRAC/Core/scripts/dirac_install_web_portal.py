#!/usr/bin/env python
########################################################################
# File :    dirac-install-web-portal
# Author :  Ricardo Graciani
########################################################################
"""
Do the initial installation of a DIRAC Web portal
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.DIRACScript import DIRACScript


@DIRACScript()
def main():
  Script.disableCS()
  Script.parseCommandLine()

  from DIRAC.FrameworkSystem.Client.ComponentInstaller import gComponentInstaller

  gComponentInstaller.exitOnError = True
  gComponentInstaller.setupPortal()


if __name__ == "__main__":
  main()
