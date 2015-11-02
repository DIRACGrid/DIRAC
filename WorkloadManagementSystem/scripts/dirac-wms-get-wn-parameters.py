"""
  Determine number of processors and memory for the worker node
"""

from DIRAC.Core.Base import Script
from DIRAC import gConfig, gLogger, exit as DIRACExit
from DIRAC.Core.Utilities import Os
from DIRAC.WorkloadManagementSystem.Utilities import JobMemory

Script.setUsageMessage( '\n'.join( ['Get the Tag of a CE',
                                    'Usage:',
                                    '%s [option]... [cfgfile]' % Script.scriptName,
                                    'Arguments:',
                                    ' cfgfile: DIRAC Cfg with description of the configuration (optional)'] ) )

Script.parseCommandLine( ignoreErrors = True )


NumberOfProcessor = Os.getNumberOfCores()
MemTotal = JobMemory.getMemoryFromMJF()
if not MemTotal:
  MemTotal = JobMemory.getMemoryFromProc()
gLogger.notice( NumberOfProcessor, MemTotal )
