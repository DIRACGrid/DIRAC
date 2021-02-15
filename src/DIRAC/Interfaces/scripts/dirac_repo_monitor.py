#!/usr/bin/env python
"""
Monitor the jobs present in the repository

Usage:
  dirac-repo-monitor [options] ... RepoDir

Arguments:
  RepoDir:  Location of Job Repository
"""
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.DIRACScript import DIRACScript


@DIRACScript()
def main():
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


if __name__ == "__main__":
  main()
