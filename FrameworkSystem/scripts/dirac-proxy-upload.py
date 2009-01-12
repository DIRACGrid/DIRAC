#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/FrameworkSystem/scripts/dirac-proxy-upload.py,v 1.3 2009/01/12 15:44:15 acasajus Exp $
# File :   dirac-proxy-init.py
# Author : Adrian Casajus
###########################################################from DIRAC.Core.Base import Script#############
__RCSID__   = "$Id: dirac-proxy-upload.py,v 1.3 2009/01/12 15:44:15 acasajus Exp $"
__VERSION__ = "$Revision: 1.3 $"

import sys
from DIRAC.Core.Base import Script
from DIRAC.FrameworkSystem.Client.ProxyUpload import CLIParams, uploadProxy

if __name__ == "__main__":
  cliParams = CLIParams()
  cliParams.registerCLISwitches()

  Script.parseCommandLine()

  retVal = uploadProxy( cliParams )
  if not retVal[ 'OK' ]:
    print retVal[ 'Message' ]
    sys.exit(1)
  sys.exit(0)
