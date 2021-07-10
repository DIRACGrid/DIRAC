#!/usr/bin/env python
"""
Get parameters assigned to the CE
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import json
from DIRAC.Core.Utilities.DIRACScript import DIRACScript


class Params(object):
  ceName = ''
  Queue = ''
  Site = ''

  def setCEName(self, args):
    self.ceName = args

  def setSite(self, args):
    self.Site = args

  def setQueue(self, args):
    self.Queue = args


@DIRACScript()
def main(self):
  params = Params()

  from DIRAC import gLogger, exit as DIRACExit
  from DIRAC.ConfigurationSystem.Client.Helpers import Resources

  self.registerSwitch("N:", "Name=", "Computing Element Name (Mandatory)",
                      params.setCEName)
  self.registerSwitch("S:", "Site=", "Site Name (Mandatory)", params.setSite)
  self.registerSwitch("Q:", "Queue=", "Queue Name (Mandatory)", params.setQueue)

  self.parseCommandLine(ignoreErrors=True)

  result = Resources.getQueue(params.Site, params.ceName, params.Queue)

  if not result['OK']:
    gLogger.error("Could not retrieve resource parameters", ": " + result['Message'])
    DIRACExit(1)
  gLogger.notice(json.dumps(result['Value']))


if __name__ == "__main__":
  main()  # pylint: disable=no-value-for-parameter
