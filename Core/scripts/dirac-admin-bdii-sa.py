#! /usr/bin/env python
########################################################################
# $HeadURL$
########################################################################
__RCSID__ = "$Id$"
import DIRAC
from DIRAC.Core.Base                                         import Script

Script.registerSwitch( "H:", "host=", "BDII host" )
Script.registerSwitch( "V:", "vo=", "vo" )
Script.setUsageMessage('\n'.join( ['Check SA info on BDII for Site',
                                    'Usage:',
                                    '  %s [option|cfgfile] ... Site' % Script.scriptName,
                                    'Arguments:',
                                    '  Site: Name of the Site (ie: CERN-PROD)'] ) )

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

from DIRAC.Interfaces.API.DiracAdmin                         import DiracAdmin

def usage():
  Script.showHelp()
  DIRAC.exit(2)

if not len(args)==1:
  usage()

site = args[0]

host = None
vo = DIRAC.gConfig.getValue('/DIRAC/VirtualOrganization', 'lhcb')
for unprocSw in Script.getUnprocessedSwitches():
  if unprocSw[0] in ( "H", "host" ):
        host = unprocSw[1]
  if unprocSw[0] in ( "V", "vo" ):
        vo = unprocSw[1]

diracAdmin = DiracAdmin()

result = diracAdmin.getBDIISA(site, useVO=vo, host=host)
if not ['OK']:
  print test['Message']
  DIRAC.exit(2)
  

sas = result['Value']
for sa in sas:
  print "SA: %s {"%sa.get('GlueChunkKey','Unknown')
  for item in sa.iteritems():
    print "%s: %s"%item
  print "}"


