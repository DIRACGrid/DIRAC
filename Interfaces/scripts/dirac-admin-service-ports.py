#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-admin-service-ports
# Author :  Stuart Paterson
########################################################################
"""
  Print the service ports for the specified setup
"""
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
__RCSID__ = "$Id$"
import DIRAC
from DIRAC.Core.Base import Script

Script.setUsageMessage('\n'.join([__doc__.split('\n')[1],
                                  'Usage:',
                                  '  %s [option|cfgfile] ... [Setup]' % Script.scriptName,
                                  'Arguments:',
                                  '  Setup:    Name of the setup']))
Script.parseCommandLine(ignoreErrors=True)
args = Script.getPositionalArgs()

setup = ''
if args:
  setup = args[0]

from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin
diracAdmin = DiracAdmin()
result = diracAdmin.getServicePorts(setup, printOutput=True)
if result['OK']:
  DIRAC.exit(0)
else:
  print(result['Message'])
  DIRAC.exit(2)
