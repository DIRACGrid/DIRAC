#! /usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-admin-bdii-ce-voview
# Author :  Adria Casajus
########################################################################
"""
  Check info on BDII for VO view of CE
"""
__RCSID__ = "$Id$"

from DIRAC.Core.Base                                         import Script

Script.registerSwitch( "H:", "host=", "BDII host" )
Script.registerSwitch( "V:", "vo=", "vo" )
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... CE' % Script.scriptName,
                                     'Arguments:',
                                     '  CE:       Name of the CE(ie: ce111.cern.ch)'] ) )

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

from DIRAC import exit as DIRACExit
from DIRAC.Core.Security.ProxyInfo                           import getProxyInfo
from DIRAC.ConfigurationSystem.Client.Helpers.Registry       import getVOForGroup

if not len( args ) == 1:
  Script.showHelp()

ceName = args[0]

host = None
vo = None
ret = getProxyInfo( disableVOMS = True )
if ret['OK'] and 'group' in ret['Value']:
  vo = getVOForGroup( ret['Value']['group'] )

for unprocSw in Script.getUnprocessedSwitches():
  if unprocSw[0] in ( "H", "host" ):
    host = unprocSw[1]
  if unprocSw[0] in ( "V", "vo" ):
    vo = unprocSw[1]

if not vo:
  Script.gLogger.error( 'Could not determine VO' )
  Script.showHelp()

from DIRAC.Interfaces.API.DiracAdmin                         import DiracAdmin
diracAdmin = DiracAdmin()

result = diracAdmin.getBDIICEVOView( ceName, useVO = vo, host = host )
if not result['OK']:
  print result['Message']
  DIRACExit( 2 )


ces = result['Value']
for ceObj in ces:
  print "CEVOView: %s {" % ceObj.get( 'GlueChunkKey', 'Unknown' )
  for item in ceObj.iteritems():
    print "%s: %s" % item
  print "}"


