#!/usr/bin/env python
########################################################################
# $HeadURL$
########################################################################
__RCSID__   = "$Id$"
__VERSION__ = "$Revision: 1.1 $"
import DIRAC
from DIRAC                          import gLogger
from DIRAC.Core.Utilities.File      import makeGuid
from DIRAC.Core.Base                import Script

Script.parseCommandLine( ignoreErrors = False )
files = Script.getPositionalArgs()
for file in files:
  try:
    md5 = makeGuid(file)
    gLogger.info("%s %s" % (file.rjust(100),md5.ljust(10)))
  except Exception,x:
    gLogger.error("Failed to get md5 for file",file,x)
