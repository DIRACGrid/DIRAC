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
from DIRAC.Core.Utilities.DIRACScript import DIRACScript
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient


class RSSSetToken(DIRACScript):

  def initParameters(self):
    self.subLogger = gLogger.getSubLogger(__file__)
    self.switchDict = {}


  switches = (
      ('element=', 'Element family to be Synchronized ( Site, Resource or Node )'),
      ('name=', 'Name, name of the element where the change applies'),
      ('statusType=', 'StatusType, if none applies to all possible statusTypes'),
      ('reason=', 'Reason to set the Status'),
      ('days=', 'Number of days the token is acquired'),
      ('releaseToken', 'Release the token and let the RSS take control'),
      ('VO=', 'VO to set a token for (obligatory)')
  )

    switches = (
        ('element=', 'Element family to be Synchronized ( Site, Resource or Node )'),
        ('name=', 'Name, name of the element where the change applies'),
        ('statusType=', 'StatusType, if none applies to all possible statusTypes'),
        ('reason=', 'Reason to set the Status'),
        ('days=', 'Number of days the token is acquired'),
        ('releaseToken', 'Release the token and let the RSS take control'),
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


  self.parseCommandLine(ignoreErrors=True)
  args = self.getPositionalArgs()
  if args:
    subLogger.error("Found the following positional args '%s', but we only accept switches" % args)
    subLogger.error("Please, check documentation below")
    self.showHelp(exitCode=1)

  switches = dict(self.getUnprocessedSwitches())
  switches.setdefault('statusType', None)
  switches.setdefault('days', 1)
  if 'releaseToken' in switches:
    switches['releaseToken'] = True
  else:
    switches['releaseToken'] = False

  for key in ('element', 'name', 'reason', 'VO'):

    if key not in switches:
      subLogger.error("%s Switch missing" % key)
      subLogger.error("Please, check documentation above")
      self.showHelp(exitCode=1)

    subLogger.debug("The switches used are:")
    map(subLogger.debug, switches.items())

    return switches


  def proxyUser(self):
    """
    Read proxy to get username.
    """

    res = getProxyInfo()
    if not res['OK']:
      return res

  res = getProxyInfo()
  if not res['OK']:
    return res

  return S_OK(res['Value']['username'])


def setToken(user):
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
  elements = rssClient.selectStatusElement(switchDict['element'], 'Status',
                                           name=switchDict['name'],
                                           statusType=switchDict['statusType'],
                                           vO=switchDict['VO'],
                                           meta={'columns': ['StatusType', 'TokenOwner']})

  if not elements['OK']:
    return elements
  elements = elements['Value']

  # If there list is empty they do not exist on the DB !
  if not elements:
    subLogger.warn('Nothing found for %s, %s, %s %s' % (switchDict['element'],
                                                        switchDict['name'], switchDict['VO'],
                                                        switchDict['statusType']))
    return S_OK()

  # If we want to release the token
  if switchDict['releaseToken']:
    tokenExpiration = datetime.max
    newTokenOwner = 'rs_svc'
  else:
    tokenExpiration = datetime.utcnow().replace(microsecond=0) + timedelta(days=int(switchDict['days']))
    newTokenOwner = user

  subLogger.always('New token: %s --- until %s' % (newTokenOwner, tokenExpiration))

  for statusType, tokenOwner in elements:

    # If a user different than the one issuing the command and RSS
    if tokenOwner != user and tokenOwner != 'rs_svc':
      subLogger.info('%s(%s) belongs to the user: %s' % (switchDict['name'], statusType, tokenOwner))

    # does the job
    result = rssClient.modifyStatusElement(switchDict['element'], 'Status',
                                           name=switchDict['name'],
                                           statusType=statusType,
                                           reason=switchDict['reason'],
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

    subLogger.info('name:%s, VO:%s statusType:%s %s' % (switchDict['name'], switchDict['VO'], statusType, msg))
  return S_OK()

    if not elements['OK']:
      return elements
    elements = elements['Value']

    # If there list is empty they do not exist on the DB !
    if not elements:
      subLogger.warn('Nothing found for %s, %s, %s' % (self.switchDict['element'],
                                                      self.switchDict['name'],
                                                      self.switchDict['statusType']))
      return S_OK()

    # If we want to release the token
    if self.switchDict['releaseToken']:
      tokenExpiration = datetime.max
      newTokenOwner = 'rs_svc'
    else:
      tokenExpiration = datetime.utcnow().replace(microsecond=0) + timedelta(days=int(self.switchDict['days']))
      newTokenOwner = user

    subLogger.always('New token: %s --- until %s' % (newTokenOwner, tokenExpiration))

    for statusType, tokenOwner in elements:

      # If a user different than the one issuing the command and RSS
      if tokenOwner != user and tokenOwner != 'rs_svc':
        subLogger.info('%s(%s) belongs to the user: %s' % (self.switchDict['name'], statusType, tokenOwner))

      # does the job
      result = rssClient.modifyStatusElement(self.switchDict['element'], 'Status',
                                            name=self.switchDict['name'],
                                            statusType=statusType,
                                            reason=self.switchDict['reason'],
                                            tokenOwner=newTokenOwner,
                                            tokenExpiration=tokenExpiration)
      if not result['OK']:
        return result

      if tokenOwner == newTokenOwner:
        msg = '(extended)'
      elif newTokenOwner == 'rs_svc':
        msg = '(released)'
      else:
        msg = '(aquired from %s)' % tokenOwner

      subLogger.info('%s:%s %s' % (self.switchDict['name'], statusType, msg))
    return S_OK()


@RSSSetToken()
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
    subLogger.error(user['Message'])
    DIRACExit(1)
  user = user['Value']

  res = self.setToken(user)
  if not res['OK']:
    subLogger.error(res['Message'])
    DIRACExit(1)

  DIRACExit(0)


if __name__ == "__main__":
  main()  # pylint: disable=no-value-for-parameter
