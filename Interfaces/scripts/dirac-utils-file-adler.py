#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Interfaces/scripts/dirac-utils-file-adler.py,v 1.1 2009/10/28 18:37:28 acsmith Exp $
########################################################################
__RCSID__   = "$Id: dirac-utils-file-adler.py,v 1.1 2009/10/28 18:37:28 acsmith Exp $"
__VERSION__ = "$Revision: 1.1 $"
import DIRAC
from DIRAC                          import gLogger
from DIRAC.Core.Utilities.File      import fileAdler
from DIRAC.Core.Base                import Script

Script.parseCommandLine( ignoreErrors = False )
files = Script.getPositionalArgs()
for file in files:
  try:
    adler = fileAdler(file)
    gLogger.info("%s %s" % (file.rjust(100),adler.ljust(10)))
  except Exception,x:
    gLogger.error("Failed to get adler for file",file,x)
