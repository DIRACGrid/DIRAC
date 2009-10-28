#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Interfaces/scripts/dirac-utils-file-md5.py,v 1.1 2009/10/28 18:42:01 acsmith Exp $
########################################################################
__RCSID__   = "$Id: dirac-utils-file-md5.py,v 1.1 2009/10/28 18:42:01 acsmith Exp $"
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
