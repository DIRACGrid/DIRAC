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

from DIRAC.Core.Utilities.DIRACScript import DIRACScript as Script
from DIRAC.ConfigurationSystem.Client.CSCLI import CSCLI


@Script()
def main():
  Script.localCfg.addDefaultEntry("LogLevel", "fatal")
  Script.parseCommandLine()

  CSCLI().start()


if __name__ == "__main__":
  main()
