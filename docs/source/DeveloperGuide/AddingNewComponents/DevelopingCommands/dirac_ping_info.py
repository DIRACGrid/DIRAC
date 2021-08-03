#!/usr/bin/env python
"""
Ping a list of services and show the result

Example:
  $ dirac-ping-info MySystem
  Ping MySystem!
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import sys

from DIRAC import S_OK, S_ERROR, gLogger, exit as DIRACExit
from DIRAC.Core.Utilities.DIRACScript import DIRACScript as Script


# Define a simple class to hold the script parameters
class Params(object):

  def __init__(self):
    self.raw = False
    self.pingsToDo = 1

  def setRawResult(self, value):
    self.raw = True
    return S_OK()

  def setNumOfPingsToDo(self, value):
    try:
      self.pingsToDo = max(1, int(value))
    except ValueError:
      return S_ERROR("Number of pings to do has to be a number")
    return S_OK()


@Script()
def main():
  # Instantiate the params class
  cliParams = Params()

  # Register accepted switches and their callbacks
  Script.registerSwitch("r", "showRaw", "show raw result from the query", cliParams.setRawResult)
  Script.registerSwitch("p:", "numPings=", "Number of pings to do (by default 1)", cliParams.setNumOfPingsToDo)
  Script.registerArgument(['System: system names'])

  # Parse the command line and initialize DIRAC
  switches, servicesList = Script.parseCommandLine(ignoreErrors=False)

  # Get the list of services
  servicesList = Script.getPositionalArgs()

  # Do something!
  gLogger.notice('Ping %s!' % ', '.join(servicesList))

if __name__ == "__main__":
  main()
