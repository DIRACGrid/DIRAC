#!/usr/bin/env python
"""
This script prints out how great is it, shows raw queries and sets the
number of pings.

Example:
  $ dirac-my-great-script detail Bob MyService
  Your name is: Bob
  This is the servicesList: MyService
  We are done with detail report.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = '$Id$'

from DIRAC import S_OK, S_ERROR, gLogger, exit as DIRACExit
from DIRAC.Core.Utilities.DIRACScript import DIRACScript
# from DIRAC.Core.Base import Script


# Define a simple class to hold the script parameters
class Params(object):
  """
    Class holding the parameters raw and pingsToDo, and callbacks for their respective switches.
  """

  def __init__(self):
    """ C'or """
    self.raw = False
    self.pingsToDo = 1

  def setRawResult(self, _):
    """ ShowRaw option callback function, no option argument.

        :return: S_OK()
    """
    self.raw = True
    return S_OK()

  def setNumOfPingsToDo(self, value):
    """ NumPings option callback function

        :param value: option argument

        :return: S_OK()/S_ERROR()
    """
    try:
      self.pingsToDo = max(1, int(value))
    except ValueError:
      return S_ERROR("Number of pings to do has to be a number")
    return S_OK()


def registerSwitches():
  """
    Registers all switches that can be used while calling the script from the command line interface.
  """

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


def registerArguments():
  """
    Registers a positional arguments that can be used while calling the script from the command line interface.
  """

  # it is important to add a colon after the name of the argument in the description
  Script.registerArgument(' ReportType: report type', values=['short', 'detail'])
  Script.registerArgument(('Name:  user name', 'DN: user DN'))
  Script.registerArgument(['Service: list of services'], default='no elements', mandatory=False)


def parseSwitchesAndPositionalArguments():
  """
    Parse switches and positional arguments given to the script
  """

  # Parse the command line and initialize DIRAC
  Script.parseCommandLine(ignoreErrors=False)

  # Get arguments
  allArgs = Script.getPositionalArgs()
  gLogger.debug('All arguments: %s' % ', '.join(allArgs))

  # Get unprocessed switches
  switches = dict(Script.getUnprocessedSwitches())

  gLogger.info('This is the servicesList %s:' % servicesList)  
  gLogger.debug("The switches used are:")
  map(gLogger.debug, switches.iteritems())

  # Get grouped positional arguments
  repType, user, services = Script.getPositionalArgs(group=True)
  gLogger.debug("The positional arguments are:")
  gLogger.debug("Report type:", repType)
  gLogger.debug("Name or DN:", user)
  gLogger.debug("Services:", services)

  return switches, repType, user, services


# IMPORTANT: Make sure to add the console-scripts entry to setup.cfg as well!
@DIRACScript()
def main():
  """
    This is the script main method, which will hold all the logic.
  """

  # Script initialization
  registerSwitches()
  registerArguments()
  switchDict, repType, user, services = parseSwitchesAndPositionalArguments()

  # Import the required DIRAC modules
  from DIRAC.Interfaces.API.Dirac import Dirac

  # let's do something
  if services == 'no elements':
    gLogger.error('No services defined')
    DIRACExit(1)
  gLogger.notice('Your %s is:' % ('DN' if user.startswith('/') else "name"), user)
  gLogger.notice('This is the servicesList:', ', '.join(services))
  gLogger.notice('We are done with %s report.' % repType)

  DIRACExit(0)


if __name__ == "__main__":
  main()
