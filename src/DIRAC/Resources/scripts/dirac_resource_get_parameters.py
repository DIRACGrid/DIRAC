#!/usr/bin/env python
"""
Get parameters assigned to the CE

Usage:
  dirac-resource-get-parameters [option]... [cfgfile]
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import json
from DIRAC.Core.Utilities.DIRACScript import DIRACScript


class ResourceGetParameters(DIRACScript):

  def initParameters(self):
    self.ceName = ''
    self.Queue = ''
    self.Site = ''

  def setCEName(self, args):
    self.ceName = args

  def setSite(self, args):
    self.Site = args

  def setQueue(self, args):
    self.Queue = args


@ResourceGetParameters()
def main(self):
  from DIRAC import gLogger, exit as DIRACExit
  from DIRAC.ConfigurationSystem.Client.Helpers import Resources

  self.registerSwitch("N:", "Name=", "Computing Element Name (Mandatory)",
                      self.setCEName)
  self.registerSwitch("S:", "Site=", "Site Name (Mandatory)", self.setSite)
  self.registerSwitch("Q:", "Queue=", "Queue Name (Mandatory)", self.setQueue)

  Script.parseCommandLine(ignoreErrors=True)

  result = Resources.getQueue(self.Site, self.ceName, self.Queue)

  if not result['OK']:
    gLogger.error("Could not retrieve resource parameters", ": " + result['Message'])
    DIRACExit(1)
  gLogger.notice(json.dumps(result['Value']))


if __name__ == "__main__":
  main()  # pylint: disable=no-value-for-parameter
