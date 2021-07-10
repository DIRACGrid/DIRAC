#!/usr/bin/env python
"""
Script that helps setting the token of the elements in RSS.
It can acquire or release the token.

If the releaseToken switch is used, no matter what was the previous token, it will be set to rs_svc (RSS owns it).
If not set, the token will be set to whatever username is defined on the proxy loaded while issuing
this command. In the second case, the token lasts one day.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = '$Id$'

from datetime import datetime, timedelta

# DIRAC
from DIRAC import gLogger, exit as DIRACExit, S_OK, version
from DIRAC.Core.Utilities.DIRACScript import DIRACScript as _DIRACScript
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient


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
        ('name=', 'Name, name of the element where the change applies'),
        ('statusType=', 'StatusType, if none applies to all possible statusTypes'),
        ('reason=', 'Reason to set the Status'),
        ('days=', 'Number of days the token is acquired'),
        ('releaseToken', 'Release the token and let the RSS take control'),
        ('VO=', 'VO to set a token for (obligatory)')
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
    switches.setdefault('statusType', None)
    switches.setdefault('days', 1)
    if 'releaseToken' in switches:
      switches['releaseToken'] = True
    else:
      switches['releaseToken'] = False

    for key in ('element', 'name', 'reason', 'VO'):

      if key not in switches:
        self.subLogger.error("%s Switch missing" % key)
        self.subLogger.error("Please, check documentation above")
        self.showHelp(exitCode=1)

    if not switches['element'] in ('Site', 'Resource', 'Node'):
      self.subLogger.error("Found %s as element switch" % switches['element'])
      self.subLogger.error("Please, check documentation above")
      self.showHelp(exitCode=1)

    self.subLogger.debug("The switches used are:")
    map(self.subLogger.debug, switches.items())

    return switches

  def proxyUser(self):
    """
    Read proxy to get username.
    """

    res = getProxyInfo()
    if not res['OK']:
      return res

    return S_OK(res['Value']['username'])

  def setToken(self, user):
    '''
      Function that gets the user token, sets the validity for it. Gets the elements
      in the database for a given name and statusType(s). Then updates the status
      of all them adding a reason and the token.
    '''

    rssClient = ResourceStatusClient()

    # This is a little bit of a nonsense, and certainly needs to be improved.
    # To modify a list of elements, we have to do it one by one. However, the
    # modify method does not discover the StatusTypes ( which in this script is
    # an optional parameter ). So, we get them from the DB and iterate over them.
    elements = rssClient.selectStatusElement(self.switchDict['element'], 'Status',
                                             name=self.switchDict['name'],
                                             statusType=self.switchDict['statusType'],
                                             vO=switchDict['VO'],
                                             meta={'columns': ['StatusType', 'TokenOwner']})

    if not elements['OK']:
      return elements
    elements = elements['Value']

    # If there list is empty they do not exist on the DB !
    if not elements:
      self.subLogger.warn('Nothing found for %s, %s, %s %s' % (self.switchDict['element'],
                                                               self.switchDict['name'], switchDict['VO'],
                                                               self.switchDict['statusType']))
      return S_OK()

    # If we want to release the token
    if self.switchDict['releaseToken']:
      tokenExpiration = datetime.max
      newTokenOwner = 'rs_svc'
    else:
      tokenExpiration = datetime.utcnow().replace(microsecond=0) + timedelta(days=int(self.switchDict['days']))
      newTokenOwner = user

    self.subLogger.always('New token: %s --- until %s' % (newTokenOwner, tokenExpiration))

    for statusType, tokenOwner in elements:

      # If a user different than the one issuing the command and RSS
      if tokenOwner != user and tokenOwner != 'rs_svc':
        self.subLogger.info('%s(%s) belongs to the user: %s' % (self.switchDict['name'], statusType, tokenOwner))

      # does the job
      result = rssClient.modifyStatusElement(self.switchDict['element'], 'Status',
                                             name=self.switchDict['name'],
                                             statusType=statusType,
                                             reason=self.switchDict['reason'],
                                             tokenOwner=newTokenOwner,
                                             vO=switchDict['VO'],
                                             tokenExpiration=tokenExpiration)
      if not result['OK']:
        return result

      if tokenOwner == newTokenOwner:
        msg = '(extended)'
      elif newTokenOwner == 'rs_svc':
        msg = '(released)'
      else:
        msg = '(aquired from %s)' % tokenOwner

      self.subLogger.info('name:%s, VO:%s statusType:%s %s' % (self.switchDict['name'],
                                                               self.switchDict['VO'], statusType, msg))
    return S_OK()


@DIRACScript()
def main(self):
  """
  Main function of the script. Gets the username from the proxy loaded and sets
  the token taking into account that user and the switchDict parameters.
  """
  # Script initialization
  self.registerSwitches()
  self.registerUsageMessage()
  self.switchDict = self.parseSwitches()

  user = self.proxyUser()
  if not user['OK']:
    self.subLogger.error(user['Message'])
    DIRACExit(1)
  user = user['Value']

  res = self.setToken(user)
  if not res['OK']:
    self.subLogger.error(res['Message'])
    DIRACExit(1)

  DIRACExit(0)


if __name__ == "__main__":
  main()  # pylint: disable=no-value-for-parameter
