#!/usr/bin/env python
""" update local cfg
"""

from DIRAC.Core.Base import Script
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgFile]' % Script.scriptName] ) )

Script.registerSwitch( 'F:', 'file=', "set the cfg file to update." )
Script.registerSwitch( 'V:', 'vo=', "set the VO." )
Script.registerSwitch( 'S:', 'setup=', "set the software dist module to update." )
Script.registerSwitch( 'D:', 'softwareDistModule=', "set the software dist module to update." )

Script.parseCommandLine()
args = Script.getPositionalArgs()

cFile = ''
sMod = ''
vo = ''
for unprocSw in Script.getUnprocessedSwitches():
  if unprocSw[0] in ( "F", "file" ):
    cFile = unprocSw[1]
  if unprocSw[0] in ( "V", "vo" ):
    vo = unprocSw[1]
  if unprocSw[0] in ( "D", "softwareDistModule" ):
    sMod = unprocSw[1]
  if unprocSw[0] in ( "S", "setup" ):
    setup = unprocSw[1]

import os

from DIRAC.Core.Utilities.CFG import CFG

localCfg = CFG()
if cFile:
  localConfigFile = cFile
else:
  localConfigFile = './etc/dirac.cfg'

localCfg.loadFromFile( localConfigFile )
if not localCfg.isSection( '/LocalSite' ):
  localCfg.createNewSection( '/LocalSite' )
localCfg.setOption( '/LocalSite/CPUTimeLeft', 5000 )
localCfg.setOption( '/DIRAC/Security/UseServerCertificate', False )

if not sMod:
  if not localCfg.isSection( '/DIRAC' ):
    localCfg.createNewSection( '/DIRAC' )
  if not localCfg.isSection( '/DIRAC/VOPolicy' ):
    localCfg.createNewSection( '/DIRAC/VOPolicy' )
  if not localCfg.isSection( '/DIRAC/VOPolicy/%s' % vo ):
    localCfg.createNewSection( '/DIRAC/VOPolicy/%s' % vo )
  if not localCfg.isSection( '/DIRAC/VOPolicy/%s/%s' % ( vo, setup ) ):
    localCfg.createNewSection( '/DIRAC/VOPolicy/%s/%s' % ( vo, setup ) )
  localCfg.setOption( '/DIRAC/VOPolicy/%s/%s/SoftwareDistModule' % ( vo, setup ), '' )

localCfg.writeToFile( localConfigFile )

