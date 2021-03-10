#!/usr/bin/env python
"""
This script prints out how great is it, shows raw queries and sets the
number of pings.

Example:
  $ dirac-my-great-script MyService
  Hello MyService!
  We are done
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = '$Id$'

from DIRAC import S_OK, S_ERROR, gLogger, exit as DIRACExit
from DIRAC.Core.Utilities.DIRACScript import DIRACScript
from DIRAC.Core.Base import Script


class Params(object):
  '''
    Class holding the parameters raw and pingsToDo, and callbacks for their
    respective switches.
  '''

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


def registerSwitches():
  '''
    Registers all switches that can be used while calling the script from the
    command line interface.
  '''

  # Some of the switches have associated a callback, defined on Params class.
  cliParams = Params()

  switches = [
      ('', 'text=', 'Text to be printed'),
      ('u', 'upper', 'Print text on upper case'),
      ('r', 'showRaw', 'Show raw result from the query', cliParams.setRawResult),
      ('p:', 'numPings=', 'Number of pings to do (by default 1)', cliParams.setNumOfPingsToDo)
  ]

  # Register switches
  for switch in switches:
    Script.registerSwitch(*switch)

  # Register positional arguments
  Script.registerArgument(["Service: service names"])


def parseSwitches():
  '''
    Parse switches and positional arguments given to the script
  '''

  # Parse the command line and initialize DIRAC
  Script.parseCommandLine(ignoreErrors=False)

  # Get the list of services
  servicesList = Script.getPositionalArgs()

  gLogger.info('This is the servicesList %s:' % servicesList)

  # Gets the rest of the
  switches = dict(Script.getUnprocessedSwitches())

  gLogger.debug("The switches used are:")
  map(gLogger.debug, switches.iteritems())

  switches['servicesList'] = servicesList

  return switches


# IMPORTANT: Make sure to add the console-scripts entry to setup.cfg as well!
@DIRACScript()
def main():
  '''
    This is the script main method, which will hold all the logic.
  '''
  # Script initialization
  registerSwitches()
  switchDict = parseSwitches()

  # Import the required DIRAC modules
  from DIRAC.Interfaces.API.Dirac import Dirac

  # let's do something
  for s in switchDict['servicesList']:
    gLogger.notice('Hello %s!' % s)

  gLogger.notice('We are done')

  DIRACExit(0)


if __name__ == "__main__":
  main()
