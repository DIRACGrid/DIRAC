#!/usr/bin/env python
########################################################################
# File :    dirac-wms-get-queue-cpu-time.py
# Author :  Federico Stagni
########################################################################
""" Report CPU length of queue, in seconds
    This script is used by the dirac-pilot script to set the CPUTime left, which is a limit for the matching
"""
import DIRAC
from DIRAC.Core.Base import Script

__RCSID__ = "$Id$"


Script.registerSwitch( "C:", "CPUNormalizationFactor=", "CPUNormalizationFactor, in case it is known" )
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile]' % Script.scriptName ] ) )
Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

CPUNormalizationFactor = 0.0
for unprocSw in Script.getUnprocessedSwitches():
  if unprocSw[0] in ( "C", "CPUNormalizationFactor" ):
    CPUNormalizationFactor = float( unprocSw[1] )

if __name__ == "__main__":
  from DIRAC.WorkloadManagementSystem.Client.CPUNormalization import getCPUTime
  cpuTime = getCPUTime( CPUNormalizationFactor )
  # I hate this kind of output... PhC
  print "CPU time left determined as", cpuTime
  DIRAC.exit( 0 )
