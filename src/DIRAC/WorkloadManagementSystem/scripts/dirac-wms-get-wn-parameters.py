#!/usr/bin/env python
"""
  Determine number of processors and memory for the worker node
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script
from DIRAC import gLogger
from DIRAC.WorkloadManagementSystem.Utilities import JobParameters

Script.setUsageMessage('\n'.join(['Get the parameters (Memory and Number of processors) of a worker node',
                                  'Usage:',
                                  ' %s [option]... [cfgfile]' % Script.scriptName,
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


gLogger.info("Getting number of processors")
numberOfProcessor = JobParameters.getNumberOfProcessors(Site, ceName, Queue)


gLogger.info("Getting memory (RAM) from MJF")
maxRAM = JobParameters.getMemoryFromMJF()
if not maxRAM:
  gLogger.info("maxRAM could not be found in MJF, using JobParameters.getMemoryFromProc()")
  maxRAM = JobParameters.getMemoryFromProc()

# just communicating it back
gLogger.notice(numberOfProcessor, maxRAM)
