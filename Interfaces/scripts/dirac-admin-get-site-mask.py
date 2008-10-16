#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Interfaces/scripts/dirac-admin-get-site-mask.py,v 1.1 2008/10/16 09:21:28 paterson Exp $
# File :   dirac-admin-get-site-mask
# Author : Stuart Paterson
########################################################################
__RCSID__   = "$Id: dirac-admin-get-site-mask.py,v 1.1 2008/10/16 09:21:28 paterson Exp $"
__VERSION__ = "$Revision: 1.1 $"
from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Interfaces.API.DiracAdmin                         import DiracAdmin

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

diracAdmin = DiracAdmin()

result = diracAdmin.getSiteMask(printOutput=True)
if result['OK']:
  DIRAC.exit(0)
else:
  print result['Message']
  DIRAC.exit(2)

