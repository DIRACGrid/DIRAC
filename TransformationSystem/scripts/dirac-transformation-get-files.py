#!/usr/bin/env python

"""
  Get the files attached to a transformation
"""

__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s TransID' % Script.scriptName
                                     ] ) )

Script.parseCommandLine()

from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient

args = Script.getPositionalArgs()
if ( len( args ) != 1 ):
  Script.showHelp()

# get arguments
TransID = args[0]

tc = TransformationClient()
res = tc.getTransformationFiles( {'TransformationID': TransID} )

if not res['OK']:
  DIRAC.gLogger.error ( res['Message'] )
  DIRAC.exit( -1 )

for transfile in res['Value']:
  DIRAC.gLogger.notice( transfile['LFN'] )






