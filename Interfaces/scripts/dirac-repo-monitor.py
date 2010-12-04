#!/usr/bin/env python
########################################################################
# $HeadURL$
########################################################################
__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Interfaces.API.Dirac import Dirac
import os, sys

Script.parseCommandLine( ignoreErrors = False )
args = sys.argv

def usage():
  print 'Usage: %s repo' % ( Script.scriptName )
  DIRAC.exit( 2 )

if len( args ) < 2:
  usage()

repoLocation = args[1]
dirac = Dirac( True, repoLocation )

exitCode = 0
dirac.monitorRepository( True )
DIRAC.exit( exitCode )
