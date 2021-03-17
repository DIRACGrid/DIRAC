#!/usr/bin/env python
"""
Returns the LFN matching given GUIDs

Usage:
  dirac-dms-resolve-guid <GUIDs>
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.DIRACScript import DIRACScript


@DIRACScript()
def main():
  Script.parseCommandLine()

  import DIRAC
  from DIRAC import gLogger

  args = Script.getPositionalArgs()
  if len(args) != 1:
    Script.showHelp()
  guids = args[0]

  try:
    guids = guids.split(',')
  except Exception:
    pass

  from DIRAC.Resources.Catalog.FileCatalog import FileCatalog

  fc = FileCatalog()
  res = fc.getLFNForGUID(guids)
  if not res['OK']:
    gLogger.error("Failed to get the LFNs", res['Message'])
    DIRAC.exit(-2)

  errorGuid = {}
  for guid, reason in res['Value']['Failed'].items():
    errorGuid.setdefault(reason, []).append(guid)

  for error, guidList in errorGuid.items():
    gLogger.notice("Error '%s' for guids %s" % (error, guidList))

  for guid, lfn in res['Value']['Successful'].items():
    gLogger.notice("%s -> %s" % (guid, lfn))

  DIRAC.exit(0)


if __name__ == "__main__":
  main()
