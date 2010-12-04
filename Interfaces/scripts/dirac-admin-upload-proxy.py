#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-admin-upload-proxy
# Author :  Stuart Paterson
########################################################################
__RCSID__ = "$Id$"
import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Interfaces.API.DiracAdmin                         import DiracAdmin

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

def usage():
  print 'Usage: %s <DIRAC group>' % ( Script.scriptName )
  DIRAC.exit( 2 )

if len( args ) != 1:
  usage()

diracAdmin = DiracAdmin()

try:
  group = str( args[0] )
except Exception, x:
  print 'Expected string for DIRAC proxy group', args
  DIRAC.exit( 2 )

result = diracAdmin.uploadProxy( group )
if result['OK']:
  print 'Proxy uploaded for group %s' % ( group )
  DIRAC.exit( 0 )
else:
  print result['Message']
  DIRAC.exit( 2 )
