#!/usr/bin/env python
"""
  Monitor the jobs present in the repository
"""
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base import Script

Script.setUsageMessage('\n'.join([__doc__.split('\n')[1],
                                  'Usage:',
                                  '  %s [option|cfgfile] ... RepoDir' % Script.scriptName,
                                  'Arguments:',
                                  '  RepoDir:  Location of Job Repository']))
Script.parseCommandLine(ignoreErrors=False)
args = Script.getPositionalArgs()

if len(args) != 1:
  Script.showHelp()

repoLocation = args[0]
from DIRAC.Interfaces.API.Dirac import Dirac
dirac = Dirac(withRepo=True, repoLocation=repoLocation)

exitCode = 0
result = dirac.monitorRepository(printOutput=True)
if not result['OK']:
  print('ERROR: ', result['Message'])
  exitCode = 2

DIRAC.exit(exitCode)
