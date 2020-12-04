'''RabbitMQSynchronizer

  RabbitMQSynchronizer keeps the RabbitMQ user database  synchronized with the CS
  RabbitMQ user database is updated according to changes in CS.
'''
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from DIRAC import S_OK
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import hostHasProperties
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getDNsInGroup
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getDNForHost
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getHosts
from DIRAC.Core.Utilities.RabbitMQAdmin import getAllUsers, deleteUsers
from DIRAC.Core.Utilities.RabbitMQAdmin import setUsersPermissions, addUsersWithoutPasswords
from DIRAC.ResourceStatusSystem.Utilities import CSHelpers
from DIRAC import gLogger
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations  # for getValidPilotGroupName()


class RabbitMQSynchronizer(object):

  def __init__(self):

    # Warm up local CS
    # I am not sure whether it is needed but
    # it was used in DIRAC.ResourceStatusSystem.Utilities.Synchronizer
    CSHelpers.warmUp()
    # only users belonging to group with this property are allowed to connect
    self._accessUserGroup = getAllowedGroupName()
    self._accessProperty = getAllowedHostProperty()  # only host with this property are allowed to connect

  def sync(self, _eventName, _params):
    '''Synchronizes the internal RabbitMQ user database with the current content of CS.

    Args:
      _eventName: any value, this parameter is ignored, but needed by caller function.
      _params: - any value, this parameter is ignored, but needed by caller function.
    Returns:
      S_OK:
    Example:
       s.sync( None, None )
    '''

    valid_users = getDNsInGroup(self._accessUserGroup)
    valid_hosts = getDNsForValidHosts(self._accessProperty)
    updateRabbitMQDatabase(valid_users + valid_hosts)
    return S_OK()


def getAllowedGroupName():
  """Returns name of the group of which users are allowed
     to connect to RabbitMQ server.

  Returns:
    str: group name
  """
  return Operations().getValue("Pilot/GenericPilotGroup", "")


def getAllowedHostProperty():
  """Returns property. The hosts that contain it
     are allowed to connect to RabbitMQ server.

  Returns:
    str: property name
  """
  # It should be taken from CS
  return 'GenericPilot'


def getDNsForValidHosts(accessProperty):
  """Returns DN of hosts which contains accessProperty based on current
     CS settings.

  Args:
    accessProperty(str):

  Returns:
    list of hosts with accessProperty set.

  :rtype: python:list

  """

  retVal = getHosts()
  if not retVal['OK']:
    return []
  hosts = retVal['Value']
  DNs = []
  for host in hosts:
    if hostHasProperties(host, [accessProperty]):
      retVal = getDNForHost(host)
      if retVal['OK']:
        DNs.extend(retVal['Value'])
      else:
        gLogger.error('Could not find a correct DN for host: %s. It will be ignored.' % host)
  return DNs


def updateRabbitMQDatabase(newUsers, specialUsers=None):
  """Updates the internal user database of RabbitMQ server.
     The current user list from the database is compared to newUsers list.
     The users that are not present in the database, but are in newUsers list
     are added. The users that are present in the database, but are not in
     newUsers list are deleted. The specialUser list contains logins that
     will not be processed at all.

  Args:
    newUsers(list): user logins to be processed.
    specialUsers(list): special users that will not be processed.

  :type newUsers: python:list
  :type specialUsers: python:list

  """
  if specialUsers is None:
    # I think specialUsers should be read from CS and not taken as the argument
    # but I will leave it till we decide it.
    specialUsers = getSpecialUsersForRabbitMQDatabase()
  ret = getAllUsers()
  if not ret['OK']:
    gLogger.error("Some problem with getting all users from RabbitMQ DB")
    return ret
  currentUsersInRabbitMQ = ret['Value']
  # special users should not be taken into account
  currentUsersInRabbitMQ = listDifference(currentUsersInRabbitMQ, specialUsers)
  usersToRemove = listDifference(currentUsersInRabbitMQ, newUsers)
  usersToAdd = listDifference(newUsers, currentUsersInRabbitMQ)
  if usersToAdd:
    addUsersWithoutPasswords(usersToAdd)
    setUsersPermissions(usersToAdd)
  if usersToRemove:
    deleteUsers(usersToRemove)
  return S_OK()


def getSpecialUsersForRabbitMQDatabase():
  """Returns a list of special users
     that will not be processed (e.g. removed)
     while updating the RabbitMQ database.

  Returns:
    list of user logins.

  :rtype: python:list

  """
  # For a moment it is hardcoded but should be read from
  # some location in CS
  return ['admin', 'dirac']


def listDifference(list1, list2):
  """Calculates differences between two lists.
     The original order of the list is not preserved.

  :rtype: python:list
  """
  return list(set(list1) - set(list2))
