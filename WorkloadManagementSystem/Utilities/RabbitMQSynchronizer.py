'''RabbitMQSynchronizer

  RabbitMQSynchronizer keeps the RabbitMQ user database  synchronized with the CS
  RabbitMQ user database is updated according to changes in CS.
'''

from DIRAC import S_OK
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getDNsInGroup
from DIRAC.Core.Utilities.RabbitMQAdmin import getAllUsers, addUsers, deleteUsers
from DIRAC.Core.Utilities.RabbitMQAdmin import setUsersPermissions

class RabbitMQSynchronizer(object):
  pass

  def __init__( self ):

    # Warm up local CS
    # I am not sure whether it is needed but
    # it was used in DIRAC.ResourceStatusSystem.Utilities.Synchronizer
    warmUp()
    self._access_group = 'lhcb_pilot'

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

    valid_users = getDNsInGroup(self._access_group)
    updateRabbitMQDatabase(valid_users)
    return S_OK()

def updateRabbitMQDatabase(newUsers):
  print newUsers
  currentUsers = getAllUsers()
  usersToRemove = listDifference(currentUsers, newUsers)
  usersToAdd = listDifference(newUsers, currentUsers)
  if usersToAdd:
    addUsers(usersToAdd)
    setUsersPermissions(usersToAdd, None)
  if usersToRemove:
    deleteUsers(usersToRemove)

def getCurrentUserList():
  return ['1','2']


def listDifference(list1, list2):
  return list(set(list1) - set(list2))

def warmUp():
  '''
    gConfig has its own dark side, it needs some warm up phase.
    This function was copied from
    from DIRAC.ResourceStatusSystem.Utilities import CSHelpers
  '''
  from DIRAC.ConfigurationSystem.private.Refresher import gRefresher
  gRefresher.refreshConfigurationIfNeeded()
