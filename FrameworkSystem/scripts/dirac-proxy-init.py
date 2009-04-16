#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/FrameworkSystem/scripts/dirac-proxy-init.py,v 1.6 2009/04/16 15:39:58 rgracian Exp $
# File :   dirac-proxy-init.py
# Author : Adrian Casajus
########################################################################
__RCSID__   = "$Id: dirac-proxy-init.py,v 1.6 2009/04/16 15:39:58 rgracian Exp $"
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
