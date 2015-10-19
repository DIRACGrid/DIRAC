""" Unit tests for RabbitMQAdmin clasRabbitMQAdmin class.
"""

import unittest
import os
import mock
from DIRAC import S_OK, DError
from DIRAC.Core.Utilities.RabbitMQAdmin import executeRabbitmqctl
from DIRAC.Core.Utilities.RabbitMQAdmin import addUser
from DIRAC.Core.Utilities.RabbitMQAdmin import deleteUser
from DIRAC.Core.Utilities.RabbitMQAdmin import clearUserPassword
from DIRAC.Core.Utilities.RabbitMQAdmin import addUserWithoutPassword
from DIRAC.Core.Utilities.RabbitMQAdmin import getAllUsers
from DIRAC.Core.Utilities.RabbitMQAdmin import setUserPermission
from DIRAC.Core.Utilities.RabbitMQAdmin import setUsersPermissions
from DIRAC.Core.Utilities.RabbitMQAdmin import addUsers
from DIRAC.Core.Utilities.RabbitMQAdmin import deleteUsers

class TestRabbitMQAdmin( unittest.TestCase ):

  def setUp( self ):
    self._testUser = "testUser"
    executeRabbitmqctl('delete_user',self._testUser)
  def tearDown( self ):
    executeRabbitmqctl('delete_user',self._testUser)


class TestAddUser( TestRabbitMQAdmin ):

  def test_success( self ):
    ret = addUser("testUser")
    self.assertTrue(ret['OK'])

  def test_failureUserAlreadyExists( self ):

    ret = addUser("testUser")
    self.assertTrue(ret['OK'])
    ret = addUser("testUser")
    self.assertFalse(ret['OK'])
    msg = ret['Message']
    self.assertTrue("Error: user_already_exists: testUser" in msg)

class TestGetAllUsers( TestRabbitMQAdmin ):

  def test_success( self ):
    ret = getAllUsers()
    self.assertTrue(ret['OK'])
    self.assertTrue(ret['Value'])

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( TestRabbitMQAdmin )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestAddUser ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestGetAllUsers ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
