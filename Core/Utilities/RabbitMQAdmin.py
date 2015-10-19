"""RabbitMQAdmin module serves for the management of the internal RabbitMQ
   users database. It uses rabbitmqctl command. Only the user with the right
   permissions can execute those commands.
"""
import subprocess
import re

def executeRabbitmqctl(arg, *argv):
  command =['sudo','/usr/sbin/rabbitmqctl','-q', arg] + list(argv)
  res = subprocess.Popen(command, stdout = subprocess.PIPE)
  cmd_out, cmd_err = res.communicate()
  return cmd_out

def addUserWithoutPassword(user):
  addUser(user)
  clearUserPassword(user)
def addUser(user, password = 'password'):
  ret = executeRabbitmqctl('add_user', user, password)

def deleteUser(user):
  ret = executeRabbitmqctl('delete_user', user)

def getAllUsers():
  users = executeRabbitmqctl('list_users')
  users = users.split('\n')
  # the rabbitMQ user list is given in the format:
  # user_name [usr_tag]
  # I remove [usr_tag] part.
  # Also only non-empty users are proceeded further.
  # Empty users can appear, cause every new line was
  # treated as a new user.
  users = [ re.sub('\\t\[\w*\]$','',u) for u in users if u]
  return users

def setUserPermission(user):
  ret = executeRabbitmqctl('set_permissions','-p','/', user, '\".*\"','\".*\"','\".*\"')

def clearUserPassword(user):
  ret = executeRabbitmqctl('clear_password', user)


def setUsersPermissions(users):
  for u in users:
    setUserPermission(u)

def addUsersWithoutPasswords(users):
  for u in users:
    addUserWithoutPassword(u)

def addUsers(users):
  for u in users:
    addUser(u)

def deleteUsers(users):
  for u in users:
    deleteUser(u)

