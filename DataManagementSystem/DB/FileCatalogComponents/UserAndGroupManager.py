########################################################################
# $HeadURL$
########################################################################

""" DIRAC FileCatalog mix-in class to manage users and groups
"""

__RCSID__ = "$Id$"

from DIRAC                                      import S_OK, S_ERROR, gConfig, gLogger
from DIRAC.Core.Security                        import Properties
import time,threading
from types import *

class UserAndGroupManagerBase:

  def __init__(self,database=None):
    self.db = database
    self.lock = threading.Lock()
    self._refreshUsers()
    self._refreshGroups()
    
  def _refreshUsers( self ):
    return S_ERROR( 'Should be implemented in a derived class' )  
  
  def _refreshGroups( self ):
    return S_ERROR( 'Should be implemented in a derived class' )    
    
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
    # Get the user
    s_uid = credDict.get('username','anon')
    res = self.getUserID(s_uid)
    if not res['OK']:
      return res
    uid = res['Value']
    # Get the group (create it if it doesn't exist)      
    s_gid = credDict.get('group','anon') 
    res = self.getGroupID(s_gid)
    if not res['OK']:
      return res
    gid = res['Value']
    return S_OK( ( uid, gid ) )
  
#####################################################################
#
#  User related methods
#
#####################################################################

  def getUserID(self,user):
    """ Get ID for a user specified by its name """
    if type(user) in [IntType,LongType]:
      return S_OK(user)
    if user in self.db.users.keys():
      return S_OK(self.db.users[user])
    return self.__addUser(user)

  def addUser(self,uname):
    """ Add a new user with a name 'uname' """
    return self.getUserID(uname)

  def getUsers(self):
    #self.__refreshUsers()  
    return S_OK(self.db.users)

  def findUser(self,user):
    return self.getUserID(user)

  def getUserName(self,uid):
    """ Get user name for the given id """   
    if uid in self.db.uids.keys():
      return S_OK(self.db.uids[uid])
    return S_ERROR('User id %d not found' % uid)

  def deleteUser(self,uname,force=True):
    """ Delete a user specified by its name """
    # ToDo: Check first if there are files belonging to the user
    if not force:
      pass
    return self.__removeUser(uname)
  
  def __addUser(self,uname):
    startTime = time.time()
    self.lock.acquire()
    waitTime = time.time()
    gLogger.debug("UserGroupManager AddUser lock created. Waited %.3f seconds. %s" % (waitTime-startTime,uname))
    if uname in self.db.users.keys():
      uid = self.db.users[uname]
      gLogger.debug("UserGroupManager AddUser lock released. Used %.3f seconds. %s" % (time.time()-waitTime,uname))
      self.lock.release()
      return S_OK(uid)
    res = self.db._insert('FC_Users',['UserName'],[uname])
    if not res['OK']:
      if 'Duplicate entry' in res['Message']:
        res = self.db.getFields("FC_Users",['UID'],{'UserName':uname})
        if res['OK']:
          uid = res['Value'][0][0]
      else:
        gLogger.debug("UserGroupManager AddUser lock released. Used %.3f seconds. %s" % (time.time()-waitTime,uname))
        self.lock.release()
        return res
    else:
      uid = res['lastRowId']
    self.db.uids[uid] = uname
    self.db.users[uname] = uid
    gLogger.debug("UserGroupManager AddUser lock released. Used %.3f seconds. %s" % (time.time()-waitTime,uname))
    self.lock.release()
    return S_OK(uid)

  def __removeUser(self,uname):
    startTime = time.time()
    self.lock.acquire()
    waitTime = time.time()
    gLogger.debug("UserGroupManager RemoveUser lock created. Waited %.3f seconds. %s" % (waitTime-startTime,uname))
    uid = self.db.users.get(uname,'Missing')
    req = "DELETE FROM FC_Users WHERE UserName='%s'" % uname
    res = self.db._update(req)
    if not res['OK']:
      gLogger.debug("UserGroupManager RemoveUser lock released. Used %.3f seconds. %s" % (time.time()-waitTime,uname))
      self.lock.release()
      return res
    if uid != 'Missing':
      self.db.users.pop(uname)
      self.db.uids.pop(uid)
    gLogger.debug("UserGroupManager RemoveUser lock released. Used %.3f seconds. %s" % (time.time()-waitTime,uname))
    self.lock.release()
    return S_OK()

  def _refreshUsers(self):
    """ Get the current user IDs and names """
    startTime = time.time()
    self.lock.acquire()
    waitTime = time.time()
    gLogger.debug("UserGroupManager RefreshUsers lock created. Waited %.3f seconds." % (waitTime-startTime))
    req = "SELECT UID,UserName from FC_Users"
    res = self.db._query(req)
    if not res['OK']:
      gLogger.debug("UserGroupManager RefreshUsers lock released. Used %.3f seconds." % (time.time()-waitTime))
      self.lock.release()
      return res
    self.db.users = {}
    self.db.uids = {}
    for uid,uname in res['Value']:
      self.db.users[uname] = uid
      self.db.uids[uid] = uname
    gLogger.debug("UserGroupManager RefreshUsers lock released. Used %.3f seconds." % (time.time()-waitTime))
    self.lock.release()
    return S_OK()
  
#####################################################################
#
#  Group related methods
#

  def getGroupID(self,group):
    """ Get ID for a group specified by its name """
    if type(group) in [IntType,LongType]:
      return S_OK(group)
    if group in self.db.groups.keys():
      return S_OK(self.db.groups[group])
    return self.__addGroup(group)

  def addGroup(self,gname):
    """ Add a new group with a name 'name' """
    return self.getGroupID(gname)

  def getGroups(self):
    #self.__refreshGroups()
    return S_OK(self.db.groups)  

  def findGroup(self,group):
    return self.getGroupID(group)

  def getGroupName(self,gid):
    """ Get group name for the given id """   
    if gid in self.db.gids.keys():
      return S_OK(self.db.gids[gid])
    return S_ERROR('Group id %d not found' % gid)
  
  def deleteGroup(self,gname,force=True):
    """ Delete a group specified by its name """
    if not force:
      # ToDo: Check first if there are files belonging to the group
      pass
    return self.__removeGroup(gname)

  def __addGroup(self,group):
    startTime = time.time()
    self.lock.acquire()
    waitTime = time.time()
    gLogger.debug("UserGroupManager AddGroup lock created. Waited %.3f seconds. %s" % (waitTime-startTime,group))
    if group in self.db.groups.keys():
      gid = self.db.groups[group]
      gLogger.debug("UserGroupManager AddGroup lock released. Used %.3f seconds. %s" % (time.time()-waitTime,group))
      self.lock.release()
      return S_OK(gid)
    res = self.db._insert('FC_Groups',['GroupName'],[group])
    if not res['OK']:
      gLogger.debug("UserGroupManager AddGroup lock released. Used %.3f seconds. %s" % (time.time()-waitTime,group))
      self.lock.release()
      return res
    gid = res['lastRowId']
    self.db.gids[gid] = group
    self.db.groups[group] = gid
    gLogger.debug("UserGroupManager AddGroup lock released. Used %.3f seconds. %s" % (time.time()-waitTime,group))
    self.lock.release()
    return S_OK(gid)

  def __removeGroup(self,group):
    startTime = time.time()
    self.lock.acquire()
    waitTime = time.time()
    gLogger.debug("UserGroupManager RemoveGroup lock created. Waited %.3f seconds. %s" % (waitTime-startTime,group))
    gid = self.db.groups.get(group,'Missing')
    req = "DELETE FROM FC_Groups WHERE GroupName='%s'" % group
    res = self.db._update(req)
    if not res['OK']:
      gLogger.debug("UserGroupManager RemoveGroup lock released. Used %.3f seconds. %s" % (time.time()-waitTime,group))
      self.lock.release()
      return res
    if gid != 'Missing':
      self.db.groups.pop(group)
      self.db.gids.pop(gid)
    gLogger.debug("UserGroupManager RemoveGroup lock released. Used %.3f seconds. %s" % (time.time()-waitTime,group))
    self.lock.release()
    return S_OK()

  def _refreshGroups(self):
    """ Get the current group IDs and names """
    req = "SELECT GID,GroupName from FC_Groups"
    startTime = time.time()
    self.lock.acquire()
    waitTime = time.time()
    gLogger.debug("UserGroupManager RefreshGroups lock created. Waited %.3f seconds." % (waitTime-startTime))
    res = self.db._query(req)
    if not res['OK']:
      gLogger.debug("UserGroupManager RefreshGroups lock released. Used %.3f seconds." % (time.time()-waitTime))
      self.lock.release()  
      return res
    self.db.groups = {}
    self.db.gids = {}
    for gid,gname in res['Value']:
      self.db.groups[gname] = gid
      self.db.gids[gid] = gname
    gLogger.debug("UserGroupManager RefreshGroups lock released. Used %.3f seconds." % (time.time()-waitTime))
    self.lock.release()
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
  