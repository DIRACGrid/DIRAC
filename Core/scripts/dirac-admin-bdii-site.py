#! /usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-admin-bdii-site
# Author :  Adria Casajus
########################################################################
"""
  Check info on BDII for Site
"""
__RCSID__ = "$Id$"

from DIRAC.Core.Base                                         import Script

Script.registerSwitch( "H:", "host=", "BDII host" )
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... Site' % Script.scriptName,
                                     'Arguments:',
                                     '  Site:     Name of the Site (ie: CERN-PROD)'] ) )

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

from DIRAC import exit as DIRACExit
from DIRAC.Interfaces.API.DiracAdmin                         import DiracAdmin

if not len( args ) == 1:
  Script.showHelp()

site = args[0]

host = None

for unprocSw in Script.getUnprocessedSwitches():
  if unprocSw[0] in ( "h", "host" ):
        host = unprocSw[1]

diracAdmin = DiracAdmin()

result = diracAdmin.getBDIISite( site, host = host )
if not result['OK']:
  print result['Message']
  DIRACExit( 2 )


sites = result['Value']
for site in sites:
  print "Site: %s {" % site.get( 'GlueSiteName', 'Unknown' )
  for item in site.iteritems():
    print "%s: %s" % item
  print "}"


