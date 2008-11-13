#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/ConfigurationSystem/scripts/dirac-configuration-dump-local-cache.py,v 1.1 2008/11/13 17:13:47 acasajus Exp $
# File :   dirac-configuration-cli
# Author : Adria Casajus
########################################################################
__RCSID__   = "$Id: dirac-configuration-dump-local-cache.py,v 1.1 2008/11/13 17:13:47 acasajus Exp $"
__VERSION__ = "$Revision: 1.1 $"

import sys
from DIRACEnvironment import DIRAC
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