#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :   dirac-configuration-cli
# Author : Adria Casajus
########################################################################
"""
  Dump DIRAC Configuration data
"""
__RCSID__   = "$Id$"

import sys
import DIRAC
from DIRAC.Core.Base import Script

Script.localCfg.addDefaultEntry( "LogLevel", "fatal" )

fileName = ""
def setFilename( args ):
  global fileName
  fileName = args
  return DIRAC.S_OK()

raw = False
def setRaw( args ):
  global raw
  raw = True
  return DIRAC.S_OK()

Script.registerSwitch( "f:", "file=", "Dump Configuration data into <file>", setFilename )
Script.registerSwitch( "r", "raw", "Do not make any modification to the data", setRaw )
Script.setUsageMessage('\n'.join( [ __doc__.split( '\n' )[1],
                                    'Usage:',
                                    '  %s [option|cfgfile] ...' % Script.scriptName, ] )   )
Script.parseCommandLine()

from DIRAC import gConfig, gLogger
result = gConfig.dumpCFGAsLocalCache( fileName, raw )
if not result[ 'OK' ]:
  print "Error: %s" % result[ 'Message' ]
  sys.exit(1)

if not fileName:
  print result[ 'Value' ]

sys.exit(0)