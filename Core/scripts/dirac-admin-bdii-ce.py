#! /usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-admin-bdii-site
# Author :  Adria Casajus
########################################################################
"""
  Check info on BDII for CE
"""
__RCSID__ = "$Id$"
from DIRAC import exit as DIRACExit
from DIRAC.Core.Base                                         import Script

Script.registerSwitch( "H:", "host=", "BDII host" )
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... CE' % Script.scriptName,
                                     'Arguments:',
                                     '  CE:       Name of the CE(ie: ce111.cern.ch)'] ) )

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

from DIRAC.Interfaces.API.DiracAdmin                         import DiracAdmin

if not len( args ) == 1:
  Script.showHelp()

ce = args[0]

host = None

for unprocSw in Script.getUnprocessedSwitches():
  if unprocSw[0] in ( "H", "host" ):
        host = unprocSw[1]

diracAdmin = DiracAdmin()

result = diracAdmin.getBDIICE( ce, host = host )
if not result['OK']:
  print result['Message']
  DIRACExit( 2 )

ces = result['Value']
for ce in ces:
  print "CE: %s {" % ce.get( 'GlueSubClusterName', 'Unknown' )
  for item in ce.iteritems():
    print "%s: %s" % item
  print "}"


