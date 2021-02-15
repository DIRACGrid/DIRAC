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
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.DIRACScript import DIRACScript


@DIRACScript()
def main():
  Script.localCfg.addDefaultEntry("LogLevel", "fatal")

  fileName = ""

  def setFilename(args):
    global fileName
    fileName = args
    return DIRAC.S_OK()

  raw = False

  def setRaw(args):
    global raw
    raw = True
    return DIRAC.S_OK()

  Script.registerSwitch("f:", "file=", "Dump Configuration data into <file>", setFilename)
  Script.registerSwitch("r", "raw", "Do not make any modification to the data", setRaw)
  Script.parseCommandLine()

  from DIRAC import gConfig, gLogger
  result = gConfig.dumpCFGAsLocalCache(fileName, raw)
  if not result['OK']:
    print("Error: %s" % result['Message'])
    sys.exit(1)

  if not fileName:
    print(result['Value'])

  sys.exit(0)


if __name__ == "__main__":
  main()
