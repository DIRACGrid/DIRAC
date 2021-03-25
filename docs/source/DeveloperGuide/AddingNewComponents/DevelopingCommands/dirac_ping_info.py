#!/usr/bin/env python
"""
Ping a list of services and show the result

Usage:
  dirac-ping-info [options] ... System ...

Arguments:
  System:   system name(mandatory)
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import sys
from DIRAC import exit as DIRACExit
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.DIRACScript import DIRACScript


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


@DIRACScript()
def main():
  # Instantiate the params class
  cliParams = Params()

  # Register accepted switches and their callbacks
  Script.registerSwitch("r", "showRaw", "show raw result from the query", cliParams.setRawResult)
  Script.registerSwitch("p:", "numPings=", "Number of pings to do (by default 1)", cliParams.setNumOfPingsToDo)

  # Parse the command line and initialize DIRAC
  Script.parseCommandLine(ignoreErrors=False)

  # Get the list of services
  servicesList = Script.getPositionalArgs()

  # Check and process the command line switches and options
  if not servicesList:
    Script.showHelp(exitCode=1)

  # Do something!


if __name__ == "__main__":
  main()
