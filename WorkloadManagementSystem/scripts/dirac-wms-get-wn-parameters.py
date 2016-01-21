#!/usr/bin/env python
"""
  Determine number of processors and memory for the worker node
"""
__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script
from DIRAC import gLogger, gConfig
from DIRAC.Core.Utilities import Os
from DIRAC.WorkloadManagementSystem.Utilities import JobMemory, WNProcessors

Script.setUsageMessage( '\n'.join( ['Get the parameters (Memory and Number of processors) of a worker node',
                                    'Usage:',
                                    '%s [option]... [cfgfile]' % Script.scriptName,
                                    'Arguments:',
                                    ' cfgfile: DIRAC Cfg with description of the configuration (optional)'] ) )

ceName = ''
ceType = ''

def setCEName( args ):
  global ceName
  ceName = args

def setSite( args ):
  global Site
  Site = args

def setQueue( args ):
  global Queue
  Queue = args

Script.registerSwitch( "N:", "Name=", "Computing Element Name (Mandatory)", setCEName )
Script.registerSwitch( "S:", "Site=", "Site Name (Mandatory)", setSite )
Script.registerSwitch( "Q:", "Queue=", "Queue Name (Mandatory)", setQueue )
Script.parseCommandLine( ignoreErrors = True )

grid = Site.split( '.' )[0]
NumberOfProcessor = WNProcessors.getProcessorFromMJF()
if not NumberOfProcessor:
  NumberOfProcessor = gConfig.getValue( '/Resources/Sites/%s/%s/CEs/%s/Queues/%s/NumberOfProcessor' % ( grid, Site, ceName, Queue ) )
  if not NumberOfProcessor:
    NumberOfProcessor = gConfig.getValue( '/Resources/Sites/%s/NumberOfProcessor' % grid )
    if not NumberOfProcessor:
      NumberOfProcessor = Os.getNumberOfCores()
  
MaxRAM = JobMemory.getMemoryFromMJF()
if not MaxRAM:
  MaxRAM = JobMemory.getMemoryFromProc()
gLogger.notice( NumberOfProcessor, MaxRAM )
