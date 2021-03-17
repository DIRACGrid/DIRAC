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
def main(self):
  self.localCfg.addDefaultEntry("LogLevel", "fatal")
  self.parseCommandLine()

  CSCLI().start()


if __name__ == "__main__":
  main()  # pylint: disable=no-value-for-parameter
