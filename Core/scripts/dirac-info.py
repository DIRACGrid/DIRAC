#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-info
# Author :  Andrei Tsaregorodtsev
########################################################################
"""
  Report info about local DIRAC installation
"""
__RCSID__ = "$Id$"

import DIRAC
from DIRAC                                                   import gConfig
from DIRAC.Core.Base                                         import Script
from DIRAC.Core.Security.ProxyInfo                           import getProxyInfo
from DIRAC.ConfigurationSystem.Client.Helpers.Registry       import getVOForGroup

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... Site' % Script.scriptName, ] ) )
Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

infoDict = {}

infoDict['Setup'] = gConfig.getValue( '/DIRAC/Setup', 'Unknown' )
infoDict['ConfigurationServer'] = gConfig.getValue( '/DIRAC/Configuration/Servers', [] )
ret = getProxyInfo( disableVOMS = True )
if ret['OK'] and 'group' in ret['Value']:
  infoDict['VirtualOrganization'] = getVOForGroup( ret['Value']['group'] )
else:
  infoDict['VirtualOrganization'] = getVOForGroup( '' )
  
try:
  import gfalthr
  infoDict['gfal version'] = gfalthr.gfal_version()
except:
  pass

try:
  import lcg_util
  infoDict['lcg_util version'] = lcg_util.lcg_util_version()
except:
  pass    

print 'DIRAC version'.rjust( 20 ), ':', DIRAC.version

for k, v in infoDict.items():
  print k.rjust( 20 ), ':', str( v )
