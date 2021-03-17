#!/usr/bin/env python
########################################################################
# File :   dirac-configuration-dump-local-cache
# Author : Adria Casajus
########################################################################
"""
Dump DIRAC Configuration data
"""
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

__RCSID__ = "$Id$"

import sys
import DIRAC
from DIRAC.Core.Utilities.DIRACScript import DIRACScript

class ConfDumpLocalCaChe(DIRACScript):

  def initParameters(self):
    """ init """
    self.fileName = ""
    self.raw = False

  def setFilename(self, args):
    self.fileName = args
    return DIRAC.S_OK()

  def setRaw(self, args):
    self.raw = True
    return DIRAC.S_OK()

@ConfDumpLocalCaChe()
def main(self):
  self.localCfg.addDefaultEntry("LogLevel", "fatal")

  self.registerSwitch("f:", "file=", "Dump Configuration data into <file>", self.setFilename)
  self.registerSwitch("r", "raw", "Do not make any modification to the data", self.setRaw)
  self.parseCommandLine()

  from DIRAC import gConfig, gLogger
  result = gConfig.dumpCFGAsLocalCache(self.fileName, self.raw)
  if not result['OK']:
    print("Error: %s" % result['Message'])
    sys.exit(1)

  if not self.fileName:
    print(result['Value'])

  sys.exit(0)


if __name__ == "__main__":
  main()  # pylint: disable=no-value-for-parameter
