#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :   dirac-configuration-cli
# Author : Adria Casajus
########################################################################
__RCSID__   = "$Id$"
__VERSION__ = "$Revision: 1.1 $"

import sys
import DIRAC
from DIRAC.Core.Base import Script

Script.localCfg.addDefaultEntry( "LogLevel", "fatal" )

fileName = ""
def setFilename( args ):
  global fileName
  fileName = args
  return DIRAC.S_OK()

Script.registerSwitch( "f:", "file=", "File to dump into", setFilename )

Script.parseCommandLine()

from DIRAC import gConfig, gLogger

result = gConfig.dumpCFGAsLocalCache( fileName )
if not result[ 'OK' ]:
  print "Error: %s" % result[ 'Message' ]
  sys.exit(1)

if not fileName:
  print result[ 'Value' ]

sys.exit(0)