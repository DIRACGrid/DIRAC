#!/usr/bin/env python
""" update local cfg
"""

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
import os

from diraccfg import CFG

from DIRAC.Core.Base import Script
Script.setUsageMessage('\n'.join([__doc__.split('\n')[1],
                                  'Usage:',
                                  '  %s [options]' % Script.scriptName]))

Script.registerSwitch('F:', 'file=', "set the cfg file to update.")
Script.registerSwitch('V:', 'vo=', "set the VO.")
Script.registerSwitch('S:', 'setup=', "set the software dist module to update.")
Script.registerSwitch('D:', 'softwareDistModule=', "set the software dist module to update.")

Script.parseCommandLine()
args = Script.getPositionalArgs()

from DIRAC import gConfig

cFile = ''
sMod = ''
vo = ''
setup = ''

for unprocSw in Script.getUnprocessedSwitches():
  if unprocSw[0] in ("F", "file"):
    cFile = unprocSw[1]
  if unprocSw[0] in ("V", "vo"):
    vo = unprocSw[1]
  if unprocSw[0] in ("D", "softwareDistModule"):
    sMod = unprocSw[1]
  if unprocSw[0] in ("S", "setup"):
    setup = unprocSw[1]

localCfg = CFG()
if cFile:
  localConfigFile = cFile
else:
  print("WORKSPACE: %s" % os.path.expandvars('$WORKSPACE'))
  if os.path.isfile(os.path.expandvars('$WORKSPACE') + '/PilotInstallDIR/etc/dirac.cfg'):
    localConfigFile = os.path.expandvars('$WORKSPACE') + '/PilotInstallDIR/etc/dirac.cfg'
  elif os.path.isfile(os.path.expandvars('$WORKSPACE') + '/ServerInstallDIR/etc/dirac.cfg'):
    localConfigFile = os.path.expandvars('$WORKSPACE') + '/ServerInstallDIR/etc/dirac.cfg'
  elif os.path.isfile('./etc/dirac.cfg'):
    localConfigFile = './etc/dirac.cfg'
  else:
    print("Local CFG file not found")
    exit(2)

localCfg.loadFromFile(localConfigFile)
if not localCfg.isSection('/LocalSite'):
  localCfg.createNewSection('/LocalSite')
localCfg.setOption('/LocalSite/CPUTimeLeft', 5000)
localCfg.setOption('/DIRAC/Security/UseServerCertificate', False)

if not sMod:
  if not setup:
    setup = gConfig.getValue('/DIRAC/Setup')
    if not setup:
      setup = 'dirac-JenkinsSetup'
  if not vo:
    vo = gConfig.getValue('/DIRAC/VirtualOrganization')
    if not vo:
      vo = 'dirac'

  if not localCfg.isSection('/DIRAC/VOPolicy'):
    localCfg.createNewSection('/DIRAC/VOPolicy')
  if not localCfg.isSection('/DIRAC/VOPolicy/%s' % vo):
    localCfg.createNewSection('/DIRAC/VOPolicy/%s' % vo)
  if not localCfg.isSection('/DIRAC/VOPolicy/%s/%s' % (vo, setup)):
    localCfg.createNewSection('/DIRAC/VOPolicy/%s/%s' % (vo, setup))
  localCfg.setOption('/DIRAC/VOPolicy/%s/%s/SoftwareDistModule' % (vo, setup), '')

localCfg.writeToFile(localConfigFile)
