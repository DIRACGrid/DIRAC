'''RabbitMQSynchronizer

  RabbitMQSynchronizer keeps the RabbitMQ user database  synchronized with the CS
  RabbitMQ user database is updated according to changes in CS.
'''

from DIRAC import S_OK
from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getDNsInGroup
from subprocess import call

class RabbitMQSynchronizer(object):
  pass

  def __init__( self ):

    # Warm up local CS
    warmUp()

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
    valid_users = getDNsInGroup('lhcb_pilot')
    updateRabbitMQUsers(valid_users)
    #ret = call(['rabbitmqctl', 'list_users'])
    #print ret
    print valid_users 
    return S_OK()

def updateRabbitMQUsers(users):
  rabbitList = getCurrentUserList()
  users_to_remove = compareLists(users, rabbitList)
  users_to_add = compareLists(users, rabbitList)
  #del_users
  #add_users
  #set_permissions

def getCurrentUserList():
  return ['1','2']

def compareLists(list1, list2):
  return list(set(list1) - set(list2))

def warmUp():
  '''
    gConfig has its own dark side, it needs some warm up phase.
  '''
  from DIRAC.ConfigurationSystem.private.Refresher import gRefresher
  gRefresher.refreshConfigurationIfNeeded()
