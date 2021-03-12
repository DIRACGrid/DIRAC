#!/usr/bin/env python
########################################################################
# File :   dirac-configuration-cli
# Author : Adria Casajus
########################################################################
"""
Command line interface to DIRAC Configuration Server
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

from DIRAC.Core.Utilities.DIRACScript import DIRACScript
from DIRAC.ConfigurationSystem.Client.CSCLI import CSCLI


@DIRACScript()
def main():
  from DIRAC.Core.Base import Script

  Script.localCfg.addDefaultEntry("LogLevel", "fatal")
  Script.parseCommandLine()

  CSCLI().start()


if __name__ == "__main__":
  main()
