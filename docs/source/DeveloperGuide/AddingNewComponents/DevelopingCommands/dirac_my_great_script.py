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


# Define a simple class to hold the script parameters
class Params(object):
  """ Class holding the parameters raw and pingsToDo, and callbacks for their respective switches.
  """

  def __init__(self):
    """ C'or """
    self.raw = False
    self.pingsToDo = 1
    # Defined all switches that can be used while calling the script from the command line interface.
    self.switches = [
        ('', 'text=', 'Text to be printed'),
        ('u', 'upper', 'Print text on upper case'),
        ('r', 'showRaw', 'Show raw result from the query', self.setRawResult),
        ('p:', 'numPings=', 'Number of pings to do (by default 1)', self.setNumOfPingsToDo)
    ]

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


# IMPORTANT: Make sure to add the console-scripts entry to setup.cfg as well!
@DIRACScript()
def main(self):
  """
    This is the script main method, which will hold all the logic.
  """
  params = Params()

  # Script initialization
  self.registerSwitches(params.switches)

  # it is important to add a colon after the name of the argument in the description
  self.registerArgument(' ReportType: report type', values=['short', 'detail'])
  self.registerArgument(('Name:  user name', 'DN: user DN'))
  self.registerArgument(['Service: list of services'], default='no elements', mandatory=False)

  # Parse the command line and initialize DIRAC
  self.parseCommandLine(ignoreErrors=False)

  # Get arguments
  allArgs = self.getPositionalArgs()
  gLogger.debug('All arguments: %s' % ', '.join(allArgs))

  # Get unprocessed switches
  switches = dict(self.getUnprocessedSwitches())

  gLogger.debug("The switches used are:")
  map(gLogger.debug, switches.iteritems())

  # Get grouped positional arguments
  repType, user, services = self.getPositionalArgs(group=True)
  gLogger.debug("The positional arguments are:")
  gLogger.debug("Report type:", repType)
  gLogger.debug("Name or DN:", user)
  gLogger.debug("Services:", services)

  # let's do something
  if services == 'no elements':
    gLogger.error('No services defined')
    DIRACExit(1)
  gLogger.notice('Your %s is:' % ('DN' if user.startswith('/') else "name"), user)
  gLogger.notice('This is the servicesList:', ', '.join(services))
  gLogger.notice('We are done with %s report.' % repType)

  DIRACExit(0)


if __name__ == "__main__":
  main()  # pylint: disable=no-value-for-parameter
