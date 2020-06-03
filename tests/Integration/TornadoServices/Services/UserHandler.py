""" Dummy Service is a service for testing new dirac protocol
  This file must be copied in FrameworkSystem/Service to run tests
"""

from DIRAC.Core.Tornado.Server.TornadoService import TornadoService
from DIRAC import S_OK, gLogger
# You need to copy ../DB/UserDB in DIRAC/FrameworkSystem/DB
from DIRAC.FrameworkSystem.DB.UserDB import UserDB  # pylint: disable=no-name-in-module, import-error
from DIRAC import gConfig


class UserHandler(TornadoService):

  """
    A handler designed for testing Tornado by implementing a basic access to database
  """

  @classmethod
  def initializeHandler(cls, serviceInfoDict):
    cls.userDB = UserDB()
    return S_OK()

  auth_addUser = ['all']

  def export_addUser(self, whom):
    """
    Add a user

      :param str whom: The name of the user we want to add
      :return: S_OK with S_OK['Value'] = The_ID_of_the_user or S_ERROR
    """
    newUser = self.userDB.addUser(whom)
    if newUser['OK']:
      return S_OK(newUser['lastRowId'])
    return newUser

  auth_editUser = ['all']

  def export_editUser(self, uid, value):
    """
      Edit a user

      :param int uid: The Id of the user in database
      :param str value: New user name
      :return: S_OK or S_ERROR
    """
    return self.userDB.editUser(uid, value)

  auth_getUserName = ['all']

  def export_getUserName(self, uid):
    """
      Get a user

      :param int uid: The Id of the user in database
      :return: S_OK with S_OK['Value'] = TheUserName or S_ERROR if not found
    """
    return self.userDB.getUserName(uid)

  auth_listUsers = ['all']

  def export_listUsers(self):
    """
      List all users

      :return: S_OK with S_OK['Value'] list of [UserId, UserName]
    """
    return self.userDB.listUsers()

  auth_unauthorized = ['nobody']

  def export_unauthorized(self):
    return S_OK()

  auth_getValue = ['all']

  def export_getTestValue(self):
    return S_OK(gConfig.getValue('/DIRAC/Configuration/TestUpdateValue'))
