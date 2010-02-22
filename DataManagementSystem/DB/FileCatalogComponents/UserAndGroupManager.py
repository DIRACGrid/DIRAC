########################################################################
# $HeadURL$
########################################################################

""" DIRAC FileCatalog mix-in class to manage users and groups
"""

__RCSID__ = "$Id$"

import time
from types import *
from DIRAC.Core.Security import Properties
from DIRAC import S_OK, S_ERROR

class UserAndGroupManager:
  
  
  def getUserAndGroupRight(self, credDict):
    """ Evaluate rights for user and group operations
    """
    if Properties.FC_MANAGEMENT in credDict[ 'properties' ]:
      return S_OK(True)
    else:
      return S_OK(False)
  
#####################################################################
#
#  User related methods
#
#####################################################################
  def addUser(self,name,credDict):
    """ Add a new user with a nickname 'name' 
    """

    result = self.getUserAndGroupRight(credDict)
    if not result['Value']:
      return S_ERROR('Permission denied')

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
    result = self._query(req)
    if not result['OK']:
      return result
    if result['Value'] and result['Value'][0][0]:
      uid = result['Value'][0][0]+1
    else:
      uid = 1
    
    result = self._insert('FC_Users',['UserName','UID'],[name,uid])
    if not result['OK']:
      return result
    
    result = self.findUser(name)
    if not result['OK']:
      return result
    return S_OK(result['Value'])

#####################################################################
  def deleteUser(self,name,credDict,force=True):
    """ Delete a user specified by its nickname
    """
    
    result = self.getUserAndGroupRight(credDict)
    if not result['Value']:
      return S_ERROR('Permission denied')

    if not force:
      # ToDo: Check first if there are files belonging to the user
      pass

    req = "DELETE FROM FC_Users WHERE UserName='%s'" % name
    resUpdate = self._update(req)
    return resUpdate

#####################################################################
  def getUsers(self,credDict):
    """ Get the current user IDs and names
    """

    result = self.getUserAndGroupRight(credDict)
    if not result['Value']:
      return S_ERROR('Permission denied')

    resDict = {}
    query = "SELECT UID,UserName from FC_Users"
    resQuery = self._query(query)
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
    resQuery = self._query(query)
    if resQuery['OK']:
      if resQuery['Value']:
        return S_OK(resQuery['Value'][0][0])
      else:
        return S_ERROR('User %s not found' % user)
    else:
      return resQuery
    
#####################################################################
  def getUserName(self,uid):
    """ Get user name for the given id
    """   
    
    if uid in self.users:
      return S_OK(self.users[uid])
    else:
      result = self.getUsers(0,0)
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
  def addGroup(self,gname,credDict,gid=0):
    """ Add a new group with a name 'name'
    """
    
    result = self.getUserAndGroupRight(credDict)
    if not result['Value']:
      return S_ERROR('Permission denied')
    
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
    result = self._query(req)
    if not result['OK']:
      return result
    if result['Value'] and result['Value'][0][0]:
      gid = result['Value'][0][0]+1
    else:
      gid = 1

    result = self._insert('FC_Groups',['GroupName','GID'],[gname,gid])
    if not result['OK']:
      return result
    
    result = self.findGroup(gname)
    if not result['OK']:
      return result
    return S_OK(result['Value'])


#####################################################################
  def deleteGroup(self,gname,credDict):
    """ Delete a group specified by its name
    """

    result = self.getUserAndGroupRight(credDict)
    if not result['Value']:
      return S_ERROR('Permission denied')

    req = "DELETE FROM FC_Groups WHERE GroupName='%s'" % gname
    resUpdate = self._update(req)
    return resUpdate
 
#####################################################################
  def getGroups(self,credDict):
    """ Get the current group IDs and names
    """

    result = self.getUserAndGroupRight(credDict)
    if not result['Value']:
      return S_ERROR('Permission denied')

    resDict = {}
    query = "SELECT GID, GroupName from FC_Groups"
    resQuery = self._query(query)
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
    """ Get ID for a group specified by its name
    """
    if type(group) in [IntType,LongType]:
      return S_OK(group)
    
    query = "SELECT GID from FC_Groups WHERE GroupName='%s'" % group
    resQuery = self._query(query)
    if resQuery['OK']:
      if resQuery['Value']:
        return S_OK(resQuery['Value'][0][0])
      else:
        return S_ERROR('Group %s not found' % group)
    else:
      return resQuery

#####################################################################
  def getGroupName(self,gid):
    """ Get group name for the given id
    """   
    
    if gid in self.groups:
      return S_OK(self.groups[gid])
    else:
      result = self.getGroups(0,0)
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
