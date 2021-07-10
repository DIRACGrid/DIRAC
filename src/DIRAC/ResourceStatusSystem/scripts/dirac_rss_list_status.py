#!/usr/bin/env python
"""
Script that dumps the DB information for the elements into the standard output.
If returns information concerning the StatusType and Status attributes.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from DIRAC import gLogger, exit as DIRACExit, version
from DIRAC.Core.Utilities.DIRACScript import DIRACScript as _DIRACScript
from DIRAC.ResourceStatusSystem.Client import ResourceStatusClient
from DIRAC.Core.Utilities.PrettyPrint import printTable

__RCSID__ = '$Id$'


class DIRACScript(_DIRACScript):

  def initParameters(self):
    self.subLogger = gLogger.getSubLogger(__file__)
    self.switchDict = {}

  def registerSwitches(self):
    """
      Registers all switches that can be used while calling the script from the
      command line interface.
    """

    switches = (
        ('element=', 'Element family to be Synchronized ( Site, Resource or Node )'),
        ('elementType=', 'ElementType narrows the search; None if default'),
        ('name=', 'ElementName; None if default'),
        ('tokenOwner=', 'Owner of the token; None if default'),
        ('statusType=', 'StatusType; None if default'),
        ('status=', 'Status; None if default'),
        ('VO=', 'Virtual organisation; None if default')
    )

    for switch in switches:
      self.registerSwitch('', switch[0], switch[1])

  def registerUsageMessage(self):
    """
      Takes the script __doc__ and adds the DIRAC version to it
    """
    usageMessage = '  DIRAC %s\n' % version
    usageMessage += __doc__

    self.setUsageMessage(usageMessage)

  def parseSwitches(self):
    """
      Parses the arguments passed by the user
    """

    switches, args = self.parseCommandLine(ignoreErrors=True)
    if args:
      self.subLogger.error("Found the following positional args '%s', but we only accept switches" % args)
      self.subLogger.error("Please, check documentation below")
      self.showHelp(exitCode=1)

    switches = dict(switches)
    # Default values
    switches.setdefault('elementType', None)
    switches.setdefault('name', None)
    switches.setdefault('tokenOwner', None)
    switches.setdefault('statusType', None)
    switches.setdefault('status', None)
    switches.setdefault('VO', None)

    if 'element' not in switches:
      self.subLogger.error("element Switch missing")
      self.subLogger.error("Please, check documentation below")
      self.showHelp(exitCode=1)

    if not switches['element'] in ('Site', 'Resource', 'Node'):
      self.subLogger.error("Found %s as element switch" % switches['element'])
      self.subLogger.error("Please, check documentation below")
      self.showHelp(exitCode=1)

    self.subLogger.debug("The switches used are:")
    map(self.subLogger.debug, switches.items())

    return switches

  def getElements(self):
    """
      Given the switches, gets a list of elements with their respective statustype
      and status attributes.
    """

    rssClient = ResourceStatusClient.ResourceStatusClient()

    meta = {'columns': []}
    for key in ('Name', 'StatusType', 'Status', 'ElementType', 'TokenOwner'):
      # Transforms from upper lower case to lower upper case
      if self.switchDict[key[0].lower() + key[1:]] is None:
        meta['columns'].append(key)

    elements = rssClient.selectStatusElement(
        self.switchDict['element'], 'Status',
        name=self.switchDict['name'].split(',') if self.switchDict['name'] else None,
        statusType=self.switchDict['statusType'].split(',') if self.switchDict['statusType'] else None,
        status=self.switchDict['status'].split(',') if self.switchDict['status'] else None,
        elementType=self.switchDict['elementType'].split(',') if self.switchDict['elementType'] else None,
        tokenOwner=self.switchDict['tokenOwner'].split(',') if self.switchDict['tokenOwner'] else None,
        meta=meta)

    return elements

  def tabularPrint(self, elementsList):
    """
      Prints the list of elements on a tabular
    """

    self.subLogger.notice('')
    self.subLogger.notice('Selection parameters:')
    self.subLogger.notice('  %s: %s' % ('element'.ljust(15), self.switchDict['element']))
    titles = []
    for key in ('Name', 'StatusType', 'Status', 'ElementType', 'TokenOwner'):

      # Transforms from upper lower case to lower upper case
      keyT = key[0].lower() + key[1:]

      if self.switchDict[keyT] is None:
        titles.append(key)
      else:
        self.subLogger.notice('  %s: %s' % (key.ljust(15), self.switchDict[keyT]))
    self.subLogger.notice('')

    self.subLogger.notice(printTable(titles, elementsList, printOut=False,
                          numbering=False, columnSeparator=' | '))

  def run(self):
    """
      Main function of the script
    """

    elements = self.getElements()
    if not elements['OK']:
      self.subLogger.error(elements)
      DIRACExit(1)
    elements = elements['Value']

    self.tabularPrint(elements)


@DIRACScript()
def main(self):
  # Script initialization
  self.registerSwitches()
  self.registerUsageMessage()
  self.parseSwitches()

  # Run script
  self.run()

  # Bye
  DIRACExit(0)


if __name__ == "__main__":
  main()  # pylint: disable=no-value-for-parameter
