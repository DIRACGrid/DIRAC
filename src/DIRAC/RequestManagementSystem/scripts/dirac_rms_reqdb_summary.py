#!/bin/env python
"""
Show ReqDB summary

Usage:
  dirac-rms-reqdb-summary [options]
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.DIRACScript import DIRACScript


@DIRACScript()
def main():
  from DIRAC.Core.Base.Script import parseCommandLine
  parseCommandLine()

  import DIRAC

  from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
  reqClient = ReqClient()

  dbSummary = reqClient.getDBSummary()
  if not dbSummary["OK"]:
    DIRAC.gLogger.error(dbSummary["Message"])
    DIRAC.exit(-1)

  dbSummary = dbSummary["Value"]
  if not dbSummary:
    DIRAC.gLogger.info("ReqDB is empty!")
    DIRAC.exit(0)

  reqs = dbSummary.get("Request", {})
  ops = dbSummary.get("Operation", {})
  fs = dbSummary.get("File", {})

  DIRAC.gLogger.always("Requests:")
  for reqState, reqCount in sorted(reqs.items()):
    DIRAC.gLogger.always("- '%s' %s" % (reqState, reqCount))
  DIRAC.gLogger.always("Operations:")
  for opType, opDict in sorted(ops.items()):
    DIRAC.gLogger.always("- '%s':" % opType)
    for opState, opCount in sorted(opDict.items()):
      DIRAC.gLogger.always("  - '%s' %s" % (opState, opCount))
  DIRAC.gLogger.always("Files:")
  for fState, fCount in sorted(fs.items()):
    DIRAC.gLogger.always("- '%s' %s" % (fState, fCount))

  DIRAC.exit(0)


if __name__ == "__main__":
  main()
