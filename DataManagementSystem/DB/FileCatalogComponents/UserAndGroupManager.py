########################################################################
# $HeadURL:  $
########################################################################

""" DIRAC FileCatalog mix-in class to manage users and groups
"""

__RCSID__ = "$Id:  $"

import time
from types import *
from DIRAC import S_OK, S_ERROR

class UserAndGroupManager:
  
#####################################################################
#
#  User related methods
#
#####################################################################
  def addUser(self,name):
    """ Add a new user with a nickname 'name' 
    """

    result = self.findUser(name)
    if not result['OK']:
      return result
    userID = result['Value']
    if userID:
      return S_OK(userID)

    result = self._insert('FC_Users',['UserName'],[name])
    if not result['OK']:
      return result
    return S_OK(result['lastRowId'])

#####################################################################
  def deleteUser(self,name):
    """ Delete a user specified by its nickname
    """

    req = "DELETE FROM FC_Users WHERE UserName='%s'" % name
    resUpdate = self._update(req)
    return resUpdate

#####################################################################
  def getUsers(self):
    """ Get the current user IDs and names
    """

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
#
#  Group related methods
#
#####################################################################
  def addGroup(self,gname,gid=0):
    """ Add a new group with a name 'name'
    """
    result = self.findGroup(gname)
    if not result['OK']:
      return result
    groupID = result['Value']
    if groupID:
      result = S_OK(groupID)
      result['Message'] = "Group "+gname+" already exists"
      return result

    result = self._insert('FC_Groups',['GroupName'],[gname])
    if not result['OK']:
      return result
    return S_OK(result['lastRowId'])


#####################################################################
  def deleteGroup(self,gname):
    """ Delete a group specified by its name
    """

    req = "DELETE FROM FC_Groups WHERE GroupName='%s'" % gname
    resUpdate = self._update(req)
    return resUpdate
 
#####################################################################
  def getGroups(self):
    """ Get the current group IDs and names
    """

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
