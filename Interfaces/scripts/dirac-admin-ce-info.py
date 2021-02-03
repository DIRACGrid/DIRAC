#! /usr/bin/env python
########################################################################
# File :    dirac-admin-ce-info
# Author :  Vladimir Romanovsky
########################################################################
"""
Retrieve Site Associated to a given CE

Usage:

  dirac-admin-ce-info [option|cfgfile] ... CE ...

Arguments:

  CE:       Name of the CE (mandatory)

Example:

  $ dirac-admin-ce-info LCG.IN2P3.fr
"""
from __future__ import print_function
__RCSID__ = "$Id$"

from DIRAC import gConfig, gLogger, exit as Dexit
from DIRAC.Core.Base import Script

Script.setUsageMessage(__doc__)
Script.parseCommandLine(ignoreErrors=True)
args = Script.getPositionalArgs()

from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getCESiteMapping
from DIRAC.ConfigurationSystem.Client.Helpers import cfgPath


if len(args) < 1:
  Script.showHelp(exitCode=1)

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
