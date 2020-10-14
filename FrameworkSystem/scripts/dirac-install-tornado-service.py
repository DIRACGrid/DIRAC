#!/usr/bin/env python
"""
Do the initial installation and configuration of a DIRAC service based on tornado
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

from DIRAC import gConfig, gLogger, S_OK
from DIRAC.ConfigurationSystem.Client.Helpers import getCSExtensions
from DIRAC.FrameworkSystem.Utilities import MonitoringUtilities
from DIRAC.Core.Base import Script
from DIRAC import exit as DIRACexit
from DIRAC.FrameworkSystem.Client.ComponentInstaller import gComponentInstaller

__RCSID__ = "$Id$"

gComponentInstaller.exitOnError = True

overwrite = False


def setOverwrite(opVal):
  global overwrite
  overwrite = True
  return S_OK()


module = ''
specialOptions = {}


def setModule(optVal):
  global specialOptions, module
  specialOptions['Module'] = optVal
  module = optVal
  return S_OK()


def setSpecialOption(optVal):
  global specialOptions
  option, value = optVal.split('=')
  specialOptions[option] = value
  return S_OK()


Script.registerSwitch("w", "overwrite", "Overwrite the configuration in the global CS", setOverwrite)
Script.registerSwitch("m:", "module=", "Python module name for the component code", setModule)
Script.registerSwitch("p:", "parameter=", "Special component option ", setSpecialOption)
Script.setUsageMessage('\n'.join([__doc__.split('\n')[1],
                                  'Usage:',
                                  '  %s [option|cfgfile] ... System Component|System/Component' % Script.scriptName,
                                  'Arguments:',
                                  '  System:  Name of the DIRAC system (ie: WorkloadManagement)',
                                  '  Service: Name of the DIRAC component (ie: Matcher)']))

Script.parseCommandLine()
args = Script.getPositionalArgs()

if len(args) == 1:
  args = args[0].split('/')

if len(args) != 2:
  Script.showHelp()
  DIRACexit(1)

system = args[0]
component = args[1]
compOrMod = module if module else component


result = gComponentInstaller.addDefaultOptionsToCS(gConfig, 'service', system, component,
                                                   getCSExtensions(),
                                                   specialOptions=specialOptions,
                                                   overwrite=overwrite)

if not result['OK']:
  gLogger.error(result['Message'])
  DIRACexit(1)


result = gComponentInstaller.addTornadoOptionsToCS(gConfig)
if not result['OK']:
  gLogger.error(result['Message'])
  DIRACexit(1)

result = gComponentInstaller.installTornado()
if not result['OK']:
  gLogger.error(result['Message'])
  DIRACexit(1)


gLogger.notice('Successfully installed component %s in %s system, now setting it up' % (component, system))
result = gComponentInstaller.setupTornadoService(system, component, getCSExtensions(), module)
if not result['OK']:
  gLogger.error(result['Message'])
  DIRACexit(1)

result = MonitoringUtilities.monitorInstallation('service', system, component, module)
if not result['OK']:
  gLogger.error(result['Message'])
  DIRACexit(1)
gLogger.notice('Successfully completed the installation of %s/%s' % (system, component))
DIRACexit()
