#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Interfaces/scripts/dirac-admin-upload-proxy.py,v 1.1 2008/10/16 09:21:28 paterson Exp $
# File :   dirac-admin-upload-proxy
# Author : Stuart Paterson
########################################################################
__RCSID__   = "$Id: dirac-admin-upload-proxy.py,v 1.1 2008/10/16 09:21:28 paterson Exp $"
__VERSION__ = "$Revision: 1.1 $"
from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Interfaces.API.DiracAdmin                         import DiracAdmin

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

def usage():
  print 'Usage: %s <DIRAC group>' %(Script.scriptName)
  DIRAC.exit(2)

if len(args) != 1:
  usage()
  
diracAdmin = DiracAdmin()

try:
  group = str(args[0])
except Exception,x:
  print 'Expected string for DIRAC proxy group', args
  DIRAC.exit(2)

result = diracAdmin.uploadProxy(group)
if result['OK']:
  print 'Proxy uploaded for group %s' %(group)
  DIRAC.exit(0)
else:
  print result['Message']
  DIRAC.exit(2)