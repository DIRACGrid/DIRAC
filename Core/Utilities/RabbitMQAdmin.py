"""RabbitMQAdmin module serves for the management of the internal RabbitMQ
   users database. It uses rabbitmqctl command. Only the user with the right
   permissions can execute those commands.
"""
import re
from DIRAC import S_OK, DError
import errno
from DIRAC.Core.Utilities import Subprocess

def executeRabbitmqctl(arg, *argv):
  """Executes RabbitMQ administration command.
    It uses rabbitmqctl command line interface.
    For every command the -q argument ("quit mode")
    is used, since in some cases the output must be processed,
    so we don't want any additional informations printed.
  Args:
    arg(str): command recognized by the rabbitmqctl.
    argv(list): optional list of string parameters.
  Returns:
    S_OK:
    Derror:

  """
  command =['sudo','/usr/sbin/rabbitmqctl','-q', arg] + list(argv)
  timeOut = 30
  result = Subprocess.systemCall(timeout = timeOut, cmdSeq = command)
  if result['OK']:
    returncode, cmd_out, cmd_err = result['Value']
  else:
    return DError(errno.EPERM, "%r failed, status code: %s stdout: %r stderr: %r" %
                                (command, returncode, cmd_out, cmd_err) )
  if returncode != 0:
    # No idea what errno code should be used here.
    # Maybe we should define some specific for rabbitmqctl
    return DError(errno.EPERM, "%r failed, status code: %s stdout: %r stderr: %r" %
                                (command, returncode, cmd_out, cmd_err) )
  return S_OK(cmd_out)

def addUserWithoutPassword(user):
  '''Adds user to the internal RabbitMQ database
    and clears its password.
    This should be done for all users, that
    will be using SSL authentication. They do not
    need any password.
  '''
  ret = addUser(user)
  if not ret['OK']:
    return ret
  return clearUserPassword(user)

def addUser(user, password = 'password'):
  '''Adds user to the internal RabbitMQ database
    Function also sets user password.
    User still cannot access to any resources, without
    having permissions set.
  '''
  return executeRabbitmqctl('add_user', user, password)

def deleteUser(user):
  return executeRabbitmqctl('delete_user', user)

def getAllUsers():
  '''Returns all existing users in the internal RabbitMQ database.
  Returns:
    S_OK: with a list of all users
  '''
  ret = executeRabbitmqctl('list_users')
  if not ret['OK']:
    return ret
  users = ret['Value']
  users = users.split('\n')
  # the rabbitMQ user list is given in the format:
  # user_name [usr_tag]
  # I remove [usr_tag] part.
  # Also only non-empty users are proceeded further.
  # Empty users can appear, cause every new line was
  # treated as a new user.
  users = [ re.sub('\\t\[\w*\]$','',u) for u in users if u]
  return S_OK(users)

def setUserPermission(user):
  return executeRabbitmqctl('set_permissions','-p','/', user, '\".*\"','\".*\"','\".*\"')

def clearUserPassword(user):
  return executeRabbitmqctl('clear_password', user)

def setUsersPermissions(users):
  successful = {}
  failed = {}
  for u in users:
    ret = setUserPermission(u)
    if ret['OK']:
      successful[u] = ret['Value']
    else:
      print "Problem with permissions:%s" % ret['Message']
      failed[u] = "Permission not set"
  return S_OK({'Successful': successful, 'Failed': failed})

def addUsersWithoutPasswords(users):
  successful = {}
  failed = {}
  for u in users:
    ret = addUserWithoutPassword(u)
    if ret['OK']:
      successful[u] = ret['Value']
    else:
      print "Problem with adding user:%s" % ret['Message']
      failed[u] = "User not added"
  return S_OK({'Successful': successful, 'Failed': failed})

def addUsers(users):
  """Adds users to the RabbitMQ internal database.
  """
  successful = {}
  failed = {}
  for u in users:
    ret = addUser(u)
    if ret['OK']:
      successful[u] = ret['Value']
    else:
      print "Problem with adding user:%s" % ret['Message']
      failed[u] = "User not added"
  return S_OK({'Successful': successful, 'Failed': failed})

def deleteUsers(users):
  """Deletes users from the RabbitMQ internal database.
  """
  successful = {}
  failed = {}
  for u in users:
    ret = deleteUser(u)
    if ret['OK']:
      successful[u] = ret['Value']
    else:
      print "Problem with adding user:%s" % ret['Message']
      failed[u] = "User not added"
  return S_OK({'Successful': successful, 'Failed': failed})

