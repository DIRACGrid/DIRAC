########################################################################
# $HeadURL$
########################################################################

""" DIRAC FileCatalog mix-in class to manage users and groups
"""

__RCSID__ = "$Id$"

import time
from types import *
from DIRAC.Core.Security import Properties
from DIRAC import S_OK, S_ERROR, gConfig
from DIRAC.ConfigurationSystem.Client.CSAPI import CSAPI

class UserAndGroupManagerBase:

  def __init__(self,database=None):
    self.db = database
    
  def setDatabase(self,database):
    self.db = database

  def getUserAndGroupRight(self, credDict):
    """ Evaluate rights for user and group operations """
    if Properties.FC_MANAGEMENT in credDict[ 'properties' ]:
      return S_OK(True)
    return S_OK(False)

class UserAndGroupManagerDB(UserAndGroupManagerBase):

  def getUserAndGroupID(self, credDict):
    """ Get a uid, gid tuple for the given Credentials """
    s_uid = credDict.get('username','anon')
    s_gid = credDict.get('group','anon') 
    # Get the user (create it if it doesn't exist)      
    res = self.getUserID(s_uid)
    if res['OK']:
      uid = res['Value']
    elif res['Message'] != 'User not found':
      return res
    else:
      res = self.addUser(s_uid)
      if not res['OK']:
        return res
      uid = res['Value']

    # Get the group (create it if it doesn't exist)      
    res = self.getGroupID(s_gid)
    if res['OK']:
      gid = res['Value']
    elif res['Message'] != 'Group not found':
      return res
    else:
      res = self.addGroup(s_gid)
      if not res['OK']:
        return res
      gid = res['Value']
    return S_OK( ( uid, gid ) )
  
#####################################################################
#
#  User related methods
#
#####################################################################

  def addUser(self,uname):
    """ Add a new user with a name 'name' """
    res = self.getUserID(uname)
    if res['OK']:
      return res
    if res['Message'] != 'User not found':
      return res
    res = self.db._insert('FC_Users',['UserName'],[uname])
    if not res['OK']:
      return res
    uid = res['Value']
    self.db.uids[uid] = uname
    self.db.users[uname] = uid
    return S_OK(uid)

  def deleteUser(self,uname,force=True):
    """ Delete a user specified by its name """
    if not force:
      # ToDo: Check first if there are files belonging to the user
      pass
    req = "DELETE FROM FC_Users WHERE UserName='%s'" % uname
    return self.db._update(req)
  
  def getUsers(self):
    return self.__getUsers()  

  def findUser(self,user):
    return self.getUserID(user)

  def getUserID(self,user):
    """ Get ID for a user specified by its name """
    if type(user) in [IntType,LongType]:
      return S_OK(user)
    if user in self.db.users.keys():
      return S_OK(self.db.users[user])
    res = self.__getUsers()
    if not res['OK']:
      return res
    if not user in self.db.users.keys():
      return S_ERROR('User not found')
    return S_OK(self.db.users[user])

  def getUserName(self,uid):
    """ Get user name for the given id """   
    if uid in self.db.uids.keys():
      return S_OK(self.db.uids[uid])
    res = self.__getUsers()
    if not res['OK']:
      return res
    if not uid in self.db.uids.keys():
      return S_ERROR('User id %d not found' % uid)
    return S_OK(self.db.uids[uid])

  def __getUsers(self):
    """ Get the current user IDs and names """
    req = "SELECT UID,UserName from FC_Users"
    res = self.db._query(req)
    if not res['OK']:
      return res
    for uid,uname in res['Value']:
      self.db.users[uname] = uid
      self.db.uids[uid] = uname
    return S_OK()
  
#####################################################################
#
#  Group related methods
#

  def addGroup(self,gname):
    """ Add a new group with a name 'name' """
    res = self.getGroupID(gname)
    if res['OK']:
      return res
    if res['Message'] != 'Group not found':
      return res
    res = self.db._insert('FC_Groups',['GroupName'],[gname])
    if not res['OK']:
      return res
    gid = res['Value']
    self.db.groups[gname] = gid
    self.db.gids[gid] = gname
    return S_OK(gid)

  def deleteGroup(self,gname,force=True):
    """ Delete a group specified by its name """
    if not force:
      # ToDo: Check first if there are files belonging to the group
      pass
    req = "DELETE FROM FC_Groups WHERE GroupName='%s'" % gname
    return self.db._update(req)
  
  def getGroups(self):
    return self.__getGroups()  

  def findGroup(self,group):
    return self.getGroupID(group)

  def getGroupID(self,group):
    """ Get ID for a group specified by its name """
    if type(group) in [IntType,LongType]:
      return S_OK(group)
    if group in self.db.groups.keys():
      return S_OK(self.db.groups[group])
    res = self.__getGroups()
    if not res['OK']:
      return res
    if not group in self.db.groups.keys():
      return S_ERROR('Group not found')
    return S_OK(self.db.groups[group])

  def getGroupName(self,gid):
    """ Get group name for the given id """   
    if gid in self.db.gids.keys():
      return S_OK(self.db.gids[gid])
    res = self.__getGroups()
    if not res['OK']:
      return res
    if not gid in self.db.gids.keys():
      return S_ERROR('Group id %d not found' % gid)
    return S_OK(self.db.gids[gid])

  def __getGroups(self):
    """ Get the current group IDs and names """
    req = "SELECT GID,GroupName from FC_Groups"
    res = self.db._query(req)
    if not res['OK']:
      return res
    for gid,gname in res['Value']:
      self.db.groups[gname] = gid
      self.db.gids[gid] = gname
    return S_OK()

class UserAndGroupManagerCS(UserAndGroupManagerBase):

  def getUserAndGroupID(self,credDict):
    user = credDict.get('username','anon') 
    group = credDict.get('group','anon')
    return S_OK((user,group))

  #####################################################################
  #
  #  User related methods
  #
  #####################################################################

  def addUser(self,name):
    return S_OK(name)

  def deleteUser(self,name,force=True):
    return S_OK()

  def getUsers(self):
    res = gConfig.getSections('/Registry/Users')    
    if not res['OK']:
      return res
    userDict = {}
    for user in res['Value']:
      userDict[user] = user
    return S_OK(userDict)

  def getUserName(self,uid):
    return S_OK(uid)

  def findUser(self,user):
    return S_OK(user)

  #####################################################################
  #
  #  Group related methods
  #
  #####################################################################

  def addGroup(self,gname):
    return S_OK(gname)

  def deleteGroup(self,gname,force=True):
    return S_OK()

  def getGroups(self):
    res = gConfig.getSections('/Registry/Groups')
    if not res['OK']:
      return res
    groupDict = {}
    for group in res['Value']: 
      groupDict[group] = group
    return S_OK(groupDict)

  def getGroupName(self,gid):
    return S_OK(gid)

  def findGroup(self,group):
    return S_OK(group)
  