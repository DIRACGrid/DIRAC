""" Basic unit tests for AuthManager
"""

import unittest

from DIRAC import gConfig
from DIRAC.Core.Utilities.CFG import CFG
from DIRAC.Core.DISET.AuthManager import AuthManager

__RCSID__ = "$Id$"

testSystemsCFG = """
Systems
{
  Service
  {
    Authorization
    {
      Method = NormalUser
      MethodAll = Any
      MethodAuth = Authenticated
      MethodGroup = NormalUser,group:group_test
      MethodAllGroup = Any,group:group_test
      MethodAuthGroup = Authenticated,group:group_test
      MethodVO = NormalUser,vo:testVO
      MethodAllVO = Any,vo:testVO
      MethodAuthVO = Authenticated,vo:testVO
      MethodHost = group:hosts
      MethodTrustedHost = TrustedHost,group:hosts
    }
  }
}
"""

testRegistryCFG = """
Registry
{
  Users
  {
    userA
    {
      DN = /User/test/DN/CN=userA
    }
    userB
    {
      DN = /User/test/DN/CN=userB
    }
  }
  Hosts
  {
    test.hostA.ch
    {
      DN = /User/test/DN/CN=test.hostA.ch
      Properties = TrustedHost
    }
    test.hostB.ch
    {
      DN = /User/test/DN/CN=test.hostB.ch
      Properties = NoTrustedHost
    }
  }
  Groups
  {
    group_test
    {
      Users = userA
      VO = testVO
      Properties = NormalUser
    }
    group_bad
    {
      Users = userB
      VO = testVOBad
      Properties = NoProperties
    }
  }
}
"""

class AuthManagerTest( unittest.TestCase ):
  """ Base class for the Modules test cases
  """
  def setUp( self ):
    self.authMgr = AuthManager( '/Systems/Service/Authorization' )
    cfg = CFG()
    cfg.loadFromBuffer( testSystemsCFG )
    gConfig.loadCFG( cfg )
    cfg.loadFromBuffer( testRegistryCFG )
    gConfig.loadCFG( cfg )

    self.emptyCredDict = { }

    self.noAuthCredDict = { 'group': 'group_test' }

    self.unregistredCredDict = { 'DN': '/User/test/DN/CN=userC' }

    self.userCredDict = { 'DN': '/User/test/DN/CN=userA',
                          'group': 'group_test' }

    self.badUserCredDict = { 'DN': '/User/test/DN/CN=userB',
                             'group': 'group_bad' }
    self.hostCredDict = { 'DN': '/User/test/DN/CN=test.hostA.ch',
                          'group': 'hosts' }
    self.badHostCredDict = { 'DN': '/User/test/DN/CN=test.hostB.ch',
                             'group': 'hosts' }

  def tearDown( self ):
    pass

  def test_userProperties( self ):

    # MethodAll accepts everybody
    result = self.authMgr.authQuery( 'MethodAll', self.emptyCredDict )
    self.assertTrue( result )
    result = self.authMgr.authQuery( 'MethodAll', self.unregistredCredDict )
    self.assertTrue( result )
    result = self.authMgr.authQuery( 'MethodAll', self.userCredDict )
    self.assertTrue( result )
    result = self.authMgr.authQuery( 'MethodAll', self.noAuthCredDict )
    self.assertTrue( result )
    result = self.authMgr.authQuery( 'MethodAll', self.badUserCredDict )
    self.assertTrue( result )

    # MethodAuth requires DN to be identified
    result = self.authMgr.authQuery( 'MethodAuth', self.emptyCredDict )
    self.assertFalse( result )
    result = self.authMgr.authQuery( 'MethodAuth', self.unregistredCredDict )
    self.assertFalse( result )
    result = self.authMgr.authQuery( 'MethodAuth', self.userCredDict )
    self.assertTrue( result )
    result = self.authMgr.authQuery( 'MethodAuth', self.noAuthCredDict )
    self.assertFalse( result )
    result = self.authMgr.authQuery( 'MethodAuth', self.badUserCredDict )
    self.assertTrue( result )

    # Method requires NormalUser property
    result = self.authMgr.authQuery( 'Method', self.emptyCredDict )
    self.assertFalse( result )
    result = self.authMgr.authQuery( 'Method', self.unregistredCredDict )
    self.assertFalse( result )
    result = self.authMgr.authQuery( 'Method', self.userCredDict )
    self.assertTrue( result )
    result = self.authMgr.authQuery( 'Method', self.badUserCredDict )
    self.assertFalse( result )
    result = self.authMgr.authQuery( 'Method', self.noAuthCredDict )
    self.assertFalse( result )

  def test_userGroup( self ):

    # MethodAllGroup accepts everybody from the right group
    result = self.authMgr.authQuery( 'MethodAllGroup', self.emptyCredDict )
    self.assertFalse( result )
    result = self.authMgr.authQuery( 'MethodAllGroup', self.unregistredCredDict )
    self.assertFalse( result )
    result = self.authMgr.authQuery( 'MethodAllGroup', self.userCredDict )
    self.assertTrue( result )
    result = self.authMgr.authQuery( 'MethodAllGroup', self.noAuthCredDict )
    self.assertFalse( result )
    result = self.authMgr.authQuery( 'MethodAllGroup', self.badUserCredDict )
    self.assertFalse( result )

    # MethodAuthGroup requires DN to be identified from the right group
    result = self.authMgr.authQuery( 'MethodAuthGroup', self.emptyCredDict )
    self.assertFalse( result )
    result = self.authMgr.authQuery( 'MethodAuthGroup', self.unregistredCredDict )
    self.assertFalse( result )
    result = self.authMgr.authQuery( 'MethodAuthGroup', self.userCredDict )
    self.assertTrue( result )
    result = self.authMgr.authQuery( 'MethodAuthGroup', self.noAuthCredDict )
    self.assertFalse( result )
    result = self.authMgr.authQuery( 'MethodAuthGroup', self.badUserCredDict )
    self.assertFalse( result )

    # Method requires NormalUser property and the right group
    result = self.authMgr.authQuery( 'MethodGroup', self.emptyCredDict )
    self.assertFalse( result )
    result = self.authMgr.authQuery( 'MethodGroup', self.unregistredCredDict )
    self.assertFalse( result )
    result = self.authMgr.authQuery( 'MethodGroup', self.userCredDict )
    self.assertTrue( result )
    result = self.authMgr.authQuery( 'MethodGroup', self.badUserCredDict )
    self.assertFalse( result )
    result = self.authMgr.authQuery( 'MethodGroup', self.noAuthCredDict )
    self.assertFalse( result )

  def test_userVO( self ):

    # MethodAllGroup accepts everybody from the right group
    result = self.authMgr.authQuery( 'MethodAllVO', self.emptyCredDict )
    self.assertFalse( result )
    result = self.authMgr.authQuery( 'MethodAllVO', self.unregistredCredDict )
    self.assertFalse( result )
    result = self.authMgr.authQuery( 'MethodAllVO', self.userCredDict )
    self.assertTrue( result )
    result = self.authMgr.authQuery( 'MethodAllVO', self.noAuthCredDict )
    self.assertFalse( result )
    result = self.authMgr.authQuery( 'MethodAllVO', self.badUserCredDict )
    self.assertFalse( result )

    # MethodAuthGroup requires DN to be identified from the right group
    result = self.authMgr.authQuery( 'MethodAuthVO', self.emptyCredDict )
    self.assertFalse( result )
    result = self.authMgr.authQuery( 'MethodAuthVO', self.unregistredCredDict )
    self.assertFalse( result )
    result = self.authMgr.authQuery( 'MethodAuthVO', self.userCredDict )
    self.assertTrue( result )
    result = self.authMgr.authQuery( 'MethodAuthVO', self.noAuthCredDict )
    self.assertFalse( result )
    result = self.authMgr.authQuery( 'MethodAuthVO', self.badUserCredDict )
    self.assertFalse( result )

    # Method requires NormalUser property and the right group
    result = self.authMgr.authQuery( 'MethodVO', self.emptyCredDict )
    self.assertFalse( result )
    result = self.authMgr.authQuery( 'MethodVO', self.unregistredCredDict )
    self.assertFalse( result )
    result = self.authMgr.authQuery( 'MethodVO', self.userCredDict )
    self.assertTrue( result )
    result = self.authMgr.authQuery( 'MethodVO', self.badUserCredDict )
    self.assertFalse( result )
    result = self.authMgr.authQuery( 'MethodVO', self.noAuthCredDict )
    self.assertFalse( result )

  def test_hostProperties( self ):

    # MethodAll accepts everybody
    result = self.authMgr.authQuery( 'MethodAll', self.hostCredDict )
    self.assertTrue( result )
    result = self.authMgr.authQuery( 'MethodAll', self.badHostCredDict )
    self.assertTrue( result )

    # MethodAuth requires DN to be identified
    result = self.authMgr.authQuery( 'MethodAuth', self.hostCredDict )
    self.assertTrue( result )
    result = self.authMgr.authQuery( 'MethodAuth', self.badHostCredDict )
    self.assertTrue( result )

    # Method requires NormalUser property
    result = self.authMgr.authQuery( 'Method', self.hostCredDict )
    self.assertFalse( result )

    # MethodHost requires hosts group
    result = self.authMgr.authQuery( 'MethodHost', self.hostCredDict )
    self.assertTrue( result )
    result = self.authMgr.authQuery( 'MethodHost', self.badHostCredDict )
    self.assertTrue( result )

    # MethodTrustedHost requires hosts group and TrustedHost property
    result = self.authMgr.authQuery( 'MethodTrustedHost', self.hostCredDict )
    self.assertTrue( result )
    result = self.authMgr.authQuery( 'MethodTrustedHost', self.badHostCredDict )
    self.assertFalse( result )

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( AuthManagerTest )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )

