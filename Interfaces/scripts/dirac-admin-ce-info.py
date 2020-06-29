#! /usr/bin/env python
########################################################################
# File :    dirac-admin-ce-info
# Author :  Vladimir Romanovsky
########################################################################
"""
  Retrieve Site Associated to a given CE
"""
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
__RCSID__ = "$Id$"

from DIRAC import gConfig, gLogger, exit as Dexit
from DIRAC.Core.Base import Script
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getCESiteMapping
from DIRAC.ConfigurationSystem.Client.Helpers import cfgPath


Script.setUsageMessage('\n'.join([__doc__.split('\n')[1],
                                  'Usage:',
                                  '  %s [option|cfgfile] ... CE ...' % Script.scriptName,
                                  'Arguments:',
                                  '  CE:       Name of the CE']))

Script.parseCommandLine(ignoreErrors=True)
args = Script.getPositionalArgs()

if len(args) < 1:
  Script.showHelp()

res = getCESiteMapping(args[0])
if not res['OK']:
  gLogger.error(res['Message'])
  Dexit(1)
site = res['Value'][args[0]]

res = gConfig.getOptionsDict(cfgPath('Resources', 'Sites', site.split('.')[0], site, 'CEs', args[0]))
if not res['OK']:
  gLogger.error(res['Message'])
  Dexit(1)
gLogger.notice(res['Value'])
