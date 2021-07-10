#!/usr/bin/env python
"""
Determine number of processors and memory for the worker node
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

from DIRAC.Core.Utilities.DIRACScript import DIRACScript
from DIRAC import gLogger
from DIRAC.WorkloadManagementSystem.Utilities import JobParameters


class Params(object):
  ceName = ''
  Site = ''
  Queue = ''

  def setCEName(self, args):
    self.ceName = args

  def setSite(self, args):
    self.Site = args

  def setQueue(self, args):
    self.Queue = args


@DIRACScript()
def main(self):
  params = Params()
  self.registerSwitch("N:", "Name=", "Computing Element Name (Mandatory)", params.setCEName)
  self.registerSwitch("S:", "Site=", "Site Name (Mandatory)", params.setSite)
  self.registerSwitch("Q:", "Queue=", "Queue Name (Mandatory)", params.setQueue)
  self.parseCommandLine(ignoreErrors=True)

  gLogger.info("Getting number of processors")
  numberOfProcessor = JobParameters.getNumberOfProcessors(params.Site, params.ceName, params.Queue)

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
