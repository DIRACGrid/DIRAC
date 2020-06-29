#!/usr/bin/env python

"""
  Get the files attached to a transformation
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base import Script

Script.setUsageMessage('\n'.join([__doc__.split('\n')[1],
                                  'Usage:',
                                  '  %s TransID' % Script.scriptName
                                  ]))

Script.parseCommandLine()

from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient

args = Script.getPositionalArgs()
if len(args) != 1:
  Script.showHelp()
  DIRAC.exit(1)

tc = TransformationClient()
res = tc.getTransformationFiles({'TransformationID': args[0]})

if not res['OK']:
  DIRAC.gLogger.error(res['Message'])
  DIRAC.exit(2)

for transfile in res['Value']:
  DIRAC.gLogger.notice(transfile['LFN'])
