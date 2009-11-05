#!/usr/bin/env python
########################################################################
# $HeadURL$
########################################################################
__RCSID__   = "$Id$"
__VERSION__ = "$Revision: 1.2 $"
import DIRAC
from DIRAC                          import gLogger
from DIRAC.Core.Utilities.Adler     import fileAdler
from DIRAC.Core.Base                import Script

Script.parseCommandLine( ignoreErrors = False )
files = Script.getPositionalArgs()
for file in files:
  try:
    adler = fileAdler(file)
    gLogger.info("%s %s" % (file.rjust(100),adler.ljust(10)))
  except Exception,x:
    gLogger.error("Failed to get adler for file",file,x)
