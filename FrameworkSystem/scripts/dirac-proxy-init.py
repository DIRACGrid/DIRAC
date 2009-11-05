#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :   dirac-proxy-init.py
# Author : Adrian Casajus
########################################################################
__RCSID__   = "$Id$"
__VERSION__ = "$Revision: 1.6 $"

import sys
from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.FrameworkSystem.Client.ProxyGeneration import CLIParams, generateProxy

if __name__ == "__main__":
  cliParams = CLIParams()
  cliParams.registerCLISwitches()

  Script.disableCS()
  Script.parseCommandLine()

  retVal = generateProxy( cliParams )
  if not retVal[ 'OK' ]:
    print retVal[ 'Message' ]
    sys.exit(1)
  sys.exit(0)
