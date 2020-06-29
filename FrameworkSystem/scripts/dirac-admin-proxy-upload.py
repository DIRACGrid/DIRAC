#!/usr/bin/env python
########################################################################
# File :    dirac-admin-proxy-upload.py
# Author :  Adrian Casajus
########################################################################

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
import sys
from DIRAC.Core.Base import Script
from DIRAC.FrameworkSystem.Client.ProxyUpload import CLIParams, uploadProxy

__RCSID__ = "$Id$"

if __name__ == "__main__":
  cliParams = CLIParams()
  cliParams.registerCLISwitches()

  Script.parseCommandLine()

  retVal = uploadProxy(cliParams)
  if not retVal['OK']:
    print(retVal['Message'])
    sys.exit(1)
  sys.exit(0)
