#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Interfaces/scripts/dirac-repo-monitor.py,v 1.1 2009/07/08 13:00:04 acsmith Exp $
########################################################################
__RCSID__   = "$Id: dirac-repo-monitor.py,v 1.1 2009/07/08 13:00:04 acsmith Exp $"
__VERSION__ = "$Revision: 1.1 $"

import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Interfaces.API.Dirac import Dirac
import os,sys

Script.parseCommandLine( ignoreErrors = False )
args = sys.argv

def usage():
  print 'Usage: %s repo' % (Script.scriptName)
  DIRAC.exit(2)

if len(args) < 2:
  usage()

repoLocation = args[1]
dirac=Dirac(True, repoLocation)

exitCode = 0
dirac.monitorRepository(True)
DIRAC.exit(exitCode)
