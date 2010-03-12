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

  def setUsers(self,users):
    self.users = users

  def setGroups(self,groups):
    self.groups = groups

  def getUserAndGroupRight(self, credDict):
    """ Evaluate rights for user and group operations
    """
    if Properties.FC_MANAGEMENT in credDict[ 'properties' ]:
      return S_OK(True)
    else:
      return S_OK(False)

class UserAndGroupManagerDB(UserAndGroupManagerBase):

  def getUserAndGroupID(self, credDict):
    """ Get a uid, gid tuple for the given Credentials
    """
    s_uid = credDict.get('username','anon')
    s_gid = credDict.get('group','anon') 
    # Get the user (create it if it doesn't exist)      
    res = self.findUser(s_uid)
    if not res['OK']:
      if res['Message'] != 'User not found':
        return res
      res = self.addUser(s_uid)
      if not res['OK']:
        return res
      uid = res['Value']
    else:
      uid = res['Value']
    # Get the group (create it if it doesn't exist)      
    res = self.findGroup(s_gid)
    if not res['OK']:
      if res['Message'] != 'Group not found':
        return res
      res = self.addGroup(s_gid)
      if not res['OK']:
        return res
      gid = res['Value']
    else:
      gid = res['Value']
    return S_OK( ( uid, gid ) )
  
#####################################################################
#
#  User related methods
#
#####################################################################

  def registerUsersAndGroupsFromCS(self):
    """ Query the CS and create in DB entries for all users and groups register there
    """
    
    csAPI = CSAPI()
    users = csAPI.listUsers()
    if users['OK']:
      for user in users['Value']:
        self.addUser( user )
    groups = csAPI.listGroups()
    if groups['OK']:
      for group in groups['Value']:
        self.addGroup( group )
    return S_OK()

  def addUser(self,name):
    """ Add a new user with a nickname 'name'  """
    userID = 0
    result = self.findUser(name)
    if not result['OK']:
      if result['Message'].find('not found') == -1:
        return result
    else:  
      userID = result['Value']
      
    if userID:
      return S_OK(userID)
    
    # Get the user ID
    req = "SELECT MAX(UID) FROM FC_Users"
    result = self.db._query(req)
    if not result['OK']:
      return result
    if result['Value'] and result['Value'][0][0]:
      uid = result['Value'][0][0]+1
    else:
      uid = 1
    
    result = self.db._insert('FC_Users',['UserName','UID'],[name,uid])
    if not result['OK']:
      return result
    
    result = self.findUser(name)
    if not result['OK']:
      return result
    return S_OK(result['Value'])

#####################################################################
  def deleteUser(self,name,force=True):
    """ Delete a user specified by its nickname
    """
    if not force:
      # ToDo: Check first if there are files belonging to the user
      pass
    req = "DELETE FROM FC_Users WHERE UserName='%s'" % name
    resUpdate = self.db._update(req)
    return resUpdate

#####################################################################
  def getUsers(self):
    return self.__getUsers()  

#####################################################################
  def __getUsers(self):
    """ Get the current user IDs and names
    """
    resDict = {}
    query = "SELECT UID,UserName from FC_Users"
    resQuery = self.db._query(query)
    if resQuery['OK']:
      if resQuery['Value']:
        for uid,name in resQuery['Value']:
          resDict[name] = (uid)
      else:
        return S_ERROR('No users defined')
    else:
      return resQuery  

    return S_OK(resDict)

#####################################################################
  def findUser(self,user):
    """ Get ID for a user specified by his name
    """
    if type(user) in [IntType,LongType]:
      return S_OK(user)

    query = "SELECT UID from FC_Users WHERE UserName='%s'" % user
    resQuery = self.db._query(query)
    if not resQuery['OK']:
      return resQuery
    if not resQuery['Value']:
      return S_ERROR('User not found')
    return S_OK(resQuery['Value'][0][0])

#####################################################################
  def getUserName(self,uid):
    """ Get user name for the given id
    """   
    
    if uid in self.users:
      return S_OK(self.users[uid])
    else:
      result = self.__getUsers()
      uDict = result['Value']
      self.users = {}
      uname = ''
      for name,id in uDict.items():
        self.users[id] = name
        if id == uid:
          uname = name
    if uname:
      return S_OK(uname)
    else:
      return S_ERROR('User id %d not found' % uid)    

#####################################################################
#
#  Group related methods
#
#####################################################################
  def addGroup(self,gname):
    """ Add a new group with a name 'name'
    """
    groupID = 0
    result = self.findGroup(gname)
    if not result['OK']:
      if result['Message'].find('not found') == -1:
        return result
    else:  
      groupID = result['Value']
    
    if groupID:
      result = S_OK(groupID)
      result['Message'] = "Group "+gname+" already exists"
      return result

    # Get the new group ID
    req = "SELECT MAX(GID) FROM FC_Groups"
    result = self.db._query(req)
    if not result['OK']:
      return result
    if result['Value'] and result['Value'][0][0]:
      gid = result['Value'][0][0]+1
    else:
      gid = 1

    result = self.db._insert('FC_Groups',['GroupName','GID'],[gname,gid])
    if not result['OK']:
      return result
    
    result = self.findGroup(gname)
    if not result['OK']:
      return result
    return S_OK(result['Value'])


#####################################################################
  def deleteGroup(self,gname):
    """ Delete a group specified by its name
    """
    req = "DELETE FROM FC_Groups WHERE GroupName='%s'" % gname
    resUpdate = self.db._update(req)
    return resUpdate
  
#####################################################################
  def getGroups(self):
    return self.__getGroups()  
 
#####################################################################
  def __getGroups(self):
    """ Get the current group IDs and names
    """
    resDict = {}
    query = "SELECT GID, GroupName from FC_Groups"
    resQuery = self.db._query(query)
    if resQuery['OK']:
      if resQuery['Value']:
        for gid,gname in resQuery['Value']:
          resDict[gname] = gid
      else:
        return S_ERROR('No groups defined')
    else:
      return resQuery  

    return S_OK(resDict)

#####################################################################
  def findGroup(self,group):
    """ Get ID for a group specified by its name """
    if type(group) in [IntType,LongType]:
      return S_OK(group)
    
    query = "SELECT GID from FC_Groups WHERE GroupName='%s'" % group
    resQuery = self.db._query(query)
    if not resQuery['OK']:
      return resQuery
    if not resQuery['Value']:
      return S_ERROR('Group not found')
    return S_OK(resQuery['Value'][0][0])

#####################################################################
  def getGroupName(self,gid):
    """ Get group name for the given id
    """   
    if gid in self.groups:
      return S_OK(self.groups[gid])
    else:
      result = self.__getGroups()
      gDict = result['Value']
      self.groups = {}
      gname = ''
      for name,id in gDict.items():
        self.groups[id] = name
        if id == gid:
          gname = name
    if gname:
      return S_OK(gname)
    else:
      return S_ERROR('Group id %d not found' % gid)  

class UserAndGroupManagerCS(UserAndGroupManagerBase):

  def getUserAndGroupID(self,credDict):
    user = credDict.get('username','') 
    group = credDict.get('group','')
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
    if not res['Value']:
      return S_ERROR("No users defined")
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

  def deleteGroup(self,gname):
    return S_OK()

  def getGroups(self):
    res = gConfig.getSections('/Registry/Groups')
    if not res['OK']:
      return res
    if not res['Value']:
      return S_ERROR("No groups defined")
    groupDict = {}
    for group in res['Value']: 
      groupDict[group] = group
    return S_OK(groupDict)

  def getGroupName(self,gid):
    return S_OK(gid)

  def findGroup(self,group):
    return S_OK(group)
  
  def registerUsersAndGroupsFromCS(self,ignore):
    return S_OK()

class UserAndGroupManager(UserAndGroupManagerDB):
  pass
