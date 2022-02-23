""" Basic unit tests for AuthManager
"""
import unittest

from diraccfg import CFG
from DIRAC import gConfig
from DIRAC.Core.DISET.AuthManager import AuthManager

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
      MethodGroupOther = NormalUser,group:group_test_other
      MethodAllGroup = Any,group:group_test
      MethodAllGroupOther = Any,group:group_test_other
      MethodAuthGroup = Authenticated,group:group_test
      MethodAuthGroupOther = Authenticated,group:group_test_other
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
  VO
  {
    testVO
    {
      VOAdmin = userA
    }
    testVOBad
    {
      VOAdmin = userB
    }
    testVOOther
    {
      VOAdmin = userA
    }
  }
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
    userS
    {
      DN = /User/test/DN/CN=userS
      Suspended = testVO
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
      Users = userA, userS
      VO = testVO
      Properties = NormalUser
    }
    group_test_other
    {
      Users = userS
      VO = testVOOther
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


class AuthManagerTest(unittest.TestCase):
    """Base class for the Modules test cases"""

    def setUp(self):
        self.authMgr = AuthManager("/Systems/Service/Authorization")
        cfg = CFG()
        cfg.loadFromBuffer(testSystemsCFG)
        gConfig.loadCFG(cfg)
        cfg.loadFromBuffer(testRegistryCFG)
        gConfig.loadCFG(cfg)

        self.noAuthCredDict = {"group": "group_test"}

        self.userCredDict = {"DN": "/User/test/DN/CN=userA", "group": "group_test"}
        self.suspendedOtherVOUserCredDict = {"DN": "/User/test/DN/CN=userS", "group": "group_test_other"}
        self.badUserCredDict = {"DN": "/User/test/DN/CN=userB", "group": "group_bad"}
        self.suspendedUserCredDict = {"DN": "/User/test/DN/CN=userS", "group": "group_test"}
        self.hostCredDict = {"DN": "/User/test/DN/CN=test.hostA.ch", "group": "hosts"}
        self.badHostCredDict = {"DN": "/User/test/DN/CN=test.hostB.ch", "group": "hosts"}

    def tearDown(self):
        pass

    def test_userProperties(self):

        # MethodAll accepts everybody
        result = self.authMgr.authQuery("MethodAll", self.userCredDict)
        self.assertTrue(result)
        result = self.authMgr.authQuery("MethodAll", self.noAuthCredDict)
        self.assertTrue(result)
        result = self.authMgr.authQuery("MethodAll", self.badUserCredDict)
        self.assertTrue(result)
        result = self.authMgr.authQuery("MethodAll", self.suspendedUserCredDict)
        self.assertTrue(result)
        result = self.authMgr.authQuery("MethodAll", self.suspendedOtherVOUserCredDict)
        self.assertTrue(result)

        # MethodAuth requires DN to be identified
        result = self.authMgr.authQuery("MethodAuth", self.userCredDict)
        self.assertTrue(result)
        result = self.authMgr.authQuery("MethodAuth", self.noAuthCredDict)
        self.assertFalse(result)
        result = self.authMgr.authQuery("MethodAuth", self.badUserCredDict)
        self.assertTrue(result)
        result = self.authMgr.authQuery("MethodAuth", self.suspendedUserCredDict)
        self.assertFalse(result)
        result = self.authMgr.authQuery("MethodAuth", self.suspendedOtherVOUserCredDict)
        self.assertTrue(result)

        # Method requires NormalUser property
        result = self.authMgr.authQuery("Method", self.userCredDict)
        self.assertTrue(result)
        result = self.authMgr.authQuery("Method", self.badUserCredDict)
        self.assertFalse(result)
        result = self.authMgr.authQuery("Method", self.noAuthCredDict)
        self.assertFalse(result)
        result = self.authMgr.authQuery("Method", self.suspendedUserCredDict)
        self.assertFalse(result)
        result = self.authMgr.authQuery("Method", self.suspendedOtherVOUserCredDict)
        self.assertTrue(result)

    def test_userGroup(self):

        # MethodAllGroup accepts everybody from the right group
        result = self.authMgr.authQuery("MethodAllGroup", self.userCredDict)
        self.assertTrue(result)
        result = self.authMgr.authQuery("MethodAllGroup", self.noAuthCredDict)
        self.assertFalse(result)
        result = self.authMgr.authQuery("MethodAllGroup", self.badUserCredDict)
        self.assertFalse(result)
        result = self.authMgr.authQuery("MethodAllGroup", self.suspendedUserCredDict)
        self.assertFalse(result)
        result = self.authMgr.authQuery("MethodAllGroup", self.suspendedOtherVOUserCredDict)
        self.assertFalse(result)

        # MethodAllGroupOther accepts everybody from the right group
        result = self.authMgr.authQuery("MethodAllGroupOther", self.userCredDict)
        self.assertFalse(result)
        result = self.authMgr.authQuery("MethodAllGroupOther", self.noAuthCredDict)
        self.assertFalse(result)
        result = self.authMgr.authQuery("MethodAllGroupOther", self.badUserCredDict)
        self.assertFalse(result)
        result = self.authMgr.authQuery("MethodAllGroupOther", self.suspendedUserCredDict)
        self.assertFalse(result)
        result = self.authMgr.authQuery("MethodAllGroupOther", self.suspendedOtherVOUserCredDict)
        self.assertTrue(result)

        # MethodAuthGroup requires DN to be identified from the right group
        result = self.authMgr.authQuery("MethodAuthGroup", self.userCredDict)
        self.assertTrue(result)
        result = self.authMgr.authQuery("MethodAuthGroup", self.noAuthCredDict)
        self.assertFalse(result)
        result = self.authMgr.authQuery("MethodAuthGroup", self.badUserCredDict)
        self.assertFalse(result)
        result = self.authMgr.authQuery("MethodAuthGroup", self.suspendedUserCredDict)
        self.assertFalse(result)
        result = self.authMgr.authQuery("MethodAuthGroup", self.suspendedOtherVOUserCredDict)
        self.assertFalse(result)

        # MethodAuthGroupOther requires DN to be identified from the right group
        result = self.authMgr.authQuery("MethodAuthGroupOther", self.userCredDict)
        self.assertFalse(result)
        result = self.authMgr.authQuery("MethodAuthGroupOther", self.noAuthCredDict)
        self.assertFalse(result)
        result = self.authMgr.authQuery("MethodAuthGroupOther", self.badUserCredDict)
        self.assertFalse(result)
        result = self.authMgr.authQuery("MethodAuthGroupOther", self.suspendedUserCredDict)
        self.assertFalse(result)
        result = self.authMgr.authQuery("MethodAuthGroupOther", self.suspendedOtherVOUserCredDict)
        self.assertTrue(result)

        # MethodGroup requires NormalUser property and the right group
        result = self.authMgr.authQuery("MethodGroup", self.userCredDict)
        self.assertTrue(result)
        result = self.authMgr.authQuery("MethodGroup", self.badUserCredDict)
        self.assertFalse(result)
        result = self.authMgr.authQuery("MethodGroup", self.noAuthCredDict)
        self.assertFalse(result)
        result = self.authMgr.authQuery("MethodGroup", self.suspendedUserCredDict)
        self.assertFalse(result)
        result = self.authMgr.authQuery("MethodGroup", self.suspendedOtherVOUserCredDict)
        self.assertFalse(result)

        # MethodGroupOther requires NormalUser property and the right group
        result = self.authMgr.authQuery("MethodGroupOther", self.userCredDict)
        self.assertFalse(result)
        result = self.authMgr.authQuery("MethodGroupOther", self.badUserCredDict)
        self.assertFalse(result)
        result = self.authMgr.authQuery("MethodGroupOther", self.noAuthCredDict)
        self.assertFalse(result)
        result = self.authMgr.authQuery("MethodGroupOther", self.suspendedUserCredDict)
        self.assertFalse(result)
        result = self.authMgr.authQuery("MethodGroupOther", self.suspendedOtherVOUserCredDict)
        self.assertTrue(result)

    def test_userVO(self):

        # MethodAllGroup accepts everybody from the right group
        result = self.authMgr.authQuery("MethodAllVO", self.userCredDict)
        self.assertTrue(result)
        result = self.authMgr.authQuery("MethodAllVO", self.noAuthCredDict)
        self.assertFalse(result)
        result = self.authMgr.authQuery("MethodAllVO", self.badUserCredDict)
        self.assertFalse(result)
        result = self.authMgr.authQuery("MethodAllVO", self.suspendedUserCredDict)
        self.assertFalse(result)
        result = self.authMgr.authQuery("MethodAllVO", self.suspendedOtherVOUserCredDict)
        self.assertFalse(result)

        # MethodAuthGroup requires DN to be identified from the right group
        result = self.authMgr.authQuery("MethodAuthVO", self.userCredDict)
        self.assertTrue(result)
        result = self.authMgr.authQuery("MethodAuthVO", self.noAuthCredDict)
        self.assertFalse(result)
        result = self.authMgr.authQuery("MethodAuthVO", self.badUserCredDict)
        self.assertFalse(result)
        result = self.authMgr.authQuery("MethodAuthVO", self.suspendedUserCredDict)
        self.assertFalse(result)
        result = self.authMgr.authQuery("MethodAuthVO", self.suspendedOtherVOUserCredDict)
        self.assertFalse(result)

        # Method requires NormalUser property and the right group
        result = self.authMgr.authQuery("MethodVO", self.userCredDict)
        self.assertTrue(result)
        result = self.authMgr.authQuery("MethodVO", self.badUserCredDict)
        self.assertFalse(result)
        result = self.authMgr.authQuery("MethodVO", self.noAuthCredDict)
        self.assertFalse(result)
        result = self.authMgr.authQuery("MethodVO", self.suspendedUserCredDict)
        self.assertFalse(result)
        result = self.authMgr.authQuery("MethodVO", self.suspendedOtherVOUserCredDict)
        self.assertFalse(result)

    def test_hostProperties(self):

        # MethodAll accepts everybody
        result = self.authMgr.authQuery("MethodAll", self.hostCredDict)
        self.assertTrue(result)
        result = self.authMgr.authQuery("MethodAll", self.badHostCredDict)
        self.assertTrue(result)

        # MethodAuth requires DN to be identified
        result = self.authMgr.authQuery("MethodAuth", self.hostCredDict)
        self.assertTrue(result)
        result = self.authMgr.authQuery("MethodAuth", self.badHostCredDict)
        self.assertTrue(result)

        # Method requires NormalUser property
        result = self.authMgr.authQuery("Method", self.hostCredDict)
        self.assertFalse(result)

        # MethodHost requires hosts group
        result = self.authMgr.authQuery("MethodHost", self.hostCredDict)
        self.assertTrue(result)
        result = self.authMgr.authQuery("MethodHost", self.badHostCredDict)
        self.assertTrue(result)

        # MethodTrustedHost requires hosts group and TrustedHost property
        result = self.authMgr.authQuery("MethodTrustedHost", self.hostCredDict)
        self.assertTrue(result)
        result = self.authMgr.authQuery("MethodTrustedHost", self.badHostCredDict)
        self.assertFalse(result)


if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(AuthManagerTest)
    testResult = unittest.TextTestRunner(verbosity=2).run(suite)
