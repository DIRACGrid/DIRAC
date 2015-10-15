'''RabbitMQSynchronizer

  RabbitMQSynchronizer keeps the RabbitMQ user database  synchronized with the CS
  RabbitMQ user database is updated according to changes in CS.
'''

from DIRAC import S_OK
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import hostHasProperties
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getDNsInGroup
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getDNForHost
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getHosts
from DIRAC.Core.Utilities.RabbitMQAdmin import getAllUsers, deleteUsers
from DIRAC.Core.Utilities.RabbitMQAdmin import setUsersPermissions, addUsersWithoutPasswords

class RabbitMQSynchronizer(object):

  def __init__( self ):

    # Warm up local CS
    # I am not sure whether it is needed but
    # it was used in DIRAC.ResourceStatusSystem.Utilities.Synchronizer
    warmUp()
    self._accessUserGroup = 'lhcb_pilot'
    self._accessProperty = 'GenericPilot'

  def sync( self, _eventName, _params ):
    '''
    examples:
      >>> s.sync( None, None )
          S_OK()

    :Parameters:
      **_eventName** - any
        this parameter is ignored, but needed by caller function.
      **_params** - any
        this parameter is ignored, but needed by caller function.
    :return: S_OK
    '''

    valid_users = getDNsInGroup(self._accessUserGroup)
    valid_hosts = getDNsForValidHosts(self._accessProperty)
    updateRabbitMQDatabase(valid_users + valid_hosts)
    return S_OK()

def getDNsForValidHosts(accessProperty):
  """Returns DN of hosts which contains accessProperty based on current
     CS settings.
  Args:
    accessProperty(str):
  Returns:
    list: of hosts with accessProperty set.
  """
  retVal = getHosts()
  if not retVal[ 'OK' ]:
    return []
  hosts = retVal['Value']
  DNs = []
  for host in hosts:
    if hostHasProperties(host, [accessProperty]):
      retVal = getDNForHost(host)
      if not retVal[ 'OK' ]:
        return []
      DNs.extend(retVal['Value'])
  return DNs


def updateRabbitMQDatabase(newUsers, specialUsers = None):
  """Updates the internal user database of RabbitMQ server.
     The current user list from the database is compared to newUsers list.
     The users that are not present in the database, but are in newUsers list
     are added. The users that are present in the database, but are not in
     newUsers list are deleted. The specialUser list contains logins that
     will not be processed at all.
  Args:
    newUsers(list): user logins to be processed.
    specialUsers(list): special users that will not be processed.
  """
  if specialUsers is None:
    specialUsers = ['ala', 'admin', 'O=client,CN=kamyk']
  currentUsersInRabbitMQ = getAllUsers()
  #special users should not be taken into account
  currentUsersInRabbitMQ = listDifference(currentUsersInRabbitMQ, specialUsers)
  usersToRemove = listDifference(currentUsersInRabbitMQ, newUsers)
  usersToAdd = listDifference(newUsers, currentUsersInRabbitMQ)
  if usersToAdd:
    addUsersWithoutPasswords(usersToAdd)
    setUsersPermissions(usersToAdd)
  if usersToRemove:
    deleteUsers(usersToRemove)

def listDifference(list1, list2):
  """Calculates differences between two lists.
     The original order of the list is not preserved.
  Returns:
    list:
  """
  return list(set(list1) - set(list2))

def warmUp():
  '''
    gConfig has its own dark side, it needs some warm up phase.
    This function was copied from
    from DIRAC.ResourceStatusSystem.Utilities import CSHelpers.
  '''
  from DIRAC.ConfigurationSystem.private.Refresher import gRefresher
  gRefresher.refreshConfigurationIfNeeded()

