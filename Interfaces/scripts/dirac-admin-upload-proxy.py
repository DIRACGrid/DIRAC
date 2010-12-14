#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-admin-upload-proxy
# Author :  Stuart Paterson
########################################################################
"""
  Upload a proxy to the Proxy Manager using delegation
"""
__RCSID__ = "$Id$"
import DIRAC
from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... Group' % Script.scriptName,
                                     'Arguments:',
                                     '  Group:    Group name in the uploaded proxy' ] ) )

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

if len( args ) != 1:
  Script.showHelp()

from DIRAC.Interfaces.API.DiracAdmin                         import DiracAdmin
diracAdmin = DiracAdmin()

group = args[0]
result = diracAdmin.uploadProxy( group )
if result['OK']:
  print 'Proxy uploaded for group %s' % ( group )
  DIRAC.exit( 0 )
else:
  print result['Message']
  DIRAC.exit( 2 )
