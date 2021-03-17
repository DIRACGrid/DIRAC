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


class MyGreatScript(DIRACScript):
  '''
    Class holding the parameters raw and pingsToDo, and callbacks for their
    respective switches.
  '''

  def __init__(self):
    self.raw = False
    self.pingsToDo = 1

    # Registers all switches that can be used while calling the script.
    self.switches = [
        ('', 'text=', 'Text to be printed'),
        ('u', 'upper', 'Print text on upper case'),
        # Some of the switches have associated a callback, defined on Params class.
        ('r', 'showRaw', 'Show raw result from the query', self.setRawResult),
        ('p:', 'numPings=', 'Number of pings to do (by default 1)', self.setNumOfPingsToDo)
    ]

  def setRawResult(self, value):
    self.raw = True
    return S_OK()

  def setNumOfPingsToDo(self, value):
    try:
      self.pingsToDo = max(1, int(value))
    except ValueError:
      return S_ERROR("Number of pings to do has to be a number")
    return S_OK()


# IMPORTANT: Make sure to add the console-scripts entry to setup.cfg as well!
@MyGreatScript()
def main(self):
  '''
    This is the script main method, which will hold all the logic.
  '''

  # Script initialization
  self.registerSwitches(self.switches)

  # Register positional arguments
  self.registerArgument(["Service: service names"])

  # Parse the command line and initialize DIRAC
  unprogressSwitches, args = self.parseCommandLine(ignoreErrors=False)

  # Gets the rest of the
  servicesList = args
  switches = dict(unprogressSwitches)

  gLogger.info('This is the servicesList %s:' % servicesList)
  gLogger.debug("The switches used are:")
  map(gLogger.debug, switches.iteritems())

  # Import the required DIRAC modules
  from DIRAC.Interfaces.API.Dirac import Dirac

  gLogger.notice('We are done')

  DIRACExit(0)


if __name__ == "__main__":
  main()  # pylint: disable=no-value-for-parameter
