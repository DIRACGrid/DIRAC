"""RabbitMQAdmin module serves for the management of the internal RabbitMQ
   users database. It uses rabbitmqctl command. Only the user with the right
   permissions can execute those commands
"""
from subprocess import call

def executeCommand(command, args):
  #ret = call(['rabbitmqctl', 'list_users'])
  #print ret
  pass 

def addUser(user):
  pass 

def deleteUser(user):
  pass

def getAllUsers():
  return []

def addUsers(users):
  pass

def deleteUsers(users):
  pass

def setUsersPermissions(users, permissions):
  pass 
