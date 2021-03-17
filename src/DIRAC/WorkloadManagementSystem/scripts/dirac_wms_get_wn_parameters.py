#!/usr/bin/env python
"""
Determine number of processors and memory for the worker node
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

from DIRAC import gLogger
from DIRAC.Core.Utilities.DIRACScript import DIRACScript
from DIRAC.WorkloadManagementSystem.Utilities import JobParameters


class WMSGetWNParameters(DIRACScript):

  def initParameters(self):
    self.ceName = ''
    self.Site = ''
    self.Queue = ''

  def setCEName(self, args):
    self.ceName = args

  def setSite(self, args):
    self.Site = args

  def setQueue(self, args):
    self.Queue = args


@WMSGetWNParameters()
def main(self):
  self.registerSwitch("N:", "Name=", "Computing Element Name (Mandatory)", self.setCEName)
  self.registerSwitch("S:", "Site=", "Site Name (Mandatory)", self.setSite)
  self.registerSwitch("Q:", "Queue=", "Queue Name (Mandatory)", self.setQueue)
  self.parseCommandLine(ignoreErrors=True)

  gLogger.info("Getting number of processors")
  numberOfProcessor = JobParameters.getNumberOfProcessors(self.Site, self.ceName, self.Queue)

  gLogger.info("Getting memory (RAM) from MJF")
  maxRAM = JobParameters.getMemoryFromMJF()
  if not maxRAM:
    gLogger.info("maxRAM could not be found in MJF, using JobParameters.getMemoryFromProc()")
    maxRAM = JobParameters.getMemoryFromProc()

  gLogger.info("Getting number of GPUs")
  numberOfGPUs = JobParameters.getNumberOfGPUs(Site, ceName, Queue)

  # just communicating it back
  gLogger.notice(" ".join(str(wnPar) for wnPar in [numberOfProcessor, maxRAM, numberOfGPUs]))


if __name__ == "__main__":
  main()  # pylint: disable=no-value-for-parameter
