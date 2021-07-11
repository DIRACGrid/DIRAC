#!/usr/bin/env python

"""
Get the files attached to a transformation
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Utilities.DIRACScript import DIRACScript


@DIRACScript()
def main():
  # Registering arguments will automatically add their description to the help menu
  DIRACScript.registerArgument("transID: transformation ID")
  _, args = DIRACScript.parseCommandLine()

  from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient

  if len(args) != 1:
    DIRACScript.showHelp(exitCode=1)

  tc = TransformationClient()
  res = tc.getTransformationFiles({'TransformationID': args[0]})

  if not res['OK']:
    DIRAC.gLogger.error(res['Message'])
    DIRAC.exit(2)

  for transfile in res['Value']:
    DIRAC.gLogger.notice(transfile['LFN'])


if __name__ == "__main__":
  main()
