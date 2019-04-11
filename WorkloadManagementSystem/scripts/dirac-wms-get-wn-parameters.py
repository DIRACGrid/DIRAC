#!/usr/bin/env python
"""
  Determine number of processors and memory for the worker node
"""

__RCSID__ = "$Id$"

import multiprocessing

from DIRAC.Core.Base import Script
from DIRAC import gLogger, gConfig
from DIRAC.WorkloadManagementSystem.Utilities import JobParameters

Script.setUsageMessage('\n'.join(['Get the parameters (Memory and Number of processors) of a worker node',
                                  'Usage:',
                                  '%s [option]... [cfgfile]' % Script.scriptName,
                                  'Arguments:',
                                  ' cfgfile: DIRAC Cfg with description of the configuration (optional)']))

ceName = ''
ceType = ''


def setCEName(args):
  global ceName
  ceName = args


def setSite(args):
  global Site
  Site = args


def setQueue(args):
  global Queue
  Queue = args


Script.registerSwitch("N:", "Name=", "Computing Element Name (Mandatory)", setCEName)
Script.registerSwitch("S:", "Site=", "Site Name (Mandatory)", setSite)
Script.registerSwitch("Q:", "Queue=", "Queue Name (Mandatory)", setQueue)
Script.parseCommandLine(ignoreErrors=True)

grid = Site.split('.')[0]
gLogger.info("Getting numberOfProcessors from MJF")
numberOfProcessor = JobParameters.getProcessorFromMJF()
if not numberOfProcessor:
  gLogger.info("NumberOfProcessors could not be found in MJF, trying from CS (queue definition)")
  numberOfProcessor = gConfig.getValue('/Resources/Sites/%s/%s/CEs/%s/Queues/%s/NumberOfProcessors' % (grid,
                                                                                                       Site,
                                                                                                       ceName,
                                                                                                       Queue))
  if not numberOfProcessor:
    gLogger.info("NumberOfProcessors could not be found in CS queue definition, ",
                 "trying from /Resources/Sites/%s/NumberOfProcessors" % grid)
    numberOfProcessor = gConfig.getValue('/Resources/Sites/%s/NumberOfProcessors' % grid)
    if not numberOfProcessor:
      gLogger.info("NumberOfProcessors could not be found in CS, using multiprocessing.cpu_count()")
      numberOfProcessor = multiprocessing.cpu_count()

gLogger.info("Getting memory (RAM) from MJF")
maxRAM = JobParameters.getMemoryFromMJF()
if not maxRAM:
  gLogger.info("maxRAM could not be found in MJF, using JobParameters.getMemoryFromProc()")
  maxRAM = JobParameters.getMemoryFromProc()
gLogger.notice(numberOfProcessor, maxRAM)
