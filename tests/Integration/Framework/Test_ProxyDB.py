""" This is a test of the ProxyDB

    It supposes that the DB is present and installed in DIRAC
"""

# pylint: disable=invalid-name,wrong-import-position,protected-access
import os
import sys
from sqlalchemy import create_engine
import unittest

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC import gLogger, gConfig
from DIRAC.Core.Utilities.CFG import CFG
from DIRAC.FrameworkSystem.DB.ProxyDB import ProxyDB
from DIRAC.ConfigurationSystem.Client.Utilities import getDBParameters

# Create needed for tests rows
result = getDBParameters('Framework/ProxyDB')
if not result['OK']:
  raise RuntimeError('Cannot get database parameters: %s' % result['Message'])
dbParam = result['Value']
engine = create_engine('mysql://%s:%s@%s:%s/%s' % (dbParam['User'],
                                                   dbParam['Password'],
                                                   dbParam['Host'],
                                                   dbParam['Port'],
                                                   dbParam['DBName']))
connection = engine.connect()

diracTestCACFG = """
Resources
{
  ProxyProviders
  {
    DIRAC_TEST_CA
    {
      ProxyProviderType = DIRACCA
      CertFile = ../../../Core/Security/test/certs/ca/ca.cert.pem
      KeyFile = ../../../Core/Security/test/certs/ca/ca.key.pem
      C = FR
      O = DIRAC
      OU = DIRAC TEST
    }
  }
}
"""

userCFG = """
Registry
{
  Users
  {
    # In dirac_user group
    userWithDN_WithGroupAndProxyProviderInDNProperties
    {
      DN = /C=FR/O=DIRAC/OU=DIRAC TEST/CN=DIRAC test user/emailAddress=testuser@diracgrid.org
      DNProperties
      {
        -C_FR-O_DIRAC-OU_DIRAC TEST-CN_DIRAC test user-emailAddress_testuser@diracgrid.org
        {
          ProxyProviders = DIRAC_TEST_CA
          Groups = dirac_user
        }
      }
    }
    userWithDN_WithGroupInDNProperties
    {
      DN = /C=FR/O=DIRAC/OU=DIRAC TEST/CN=DIRAC test user/emailAddress=testuser@diracgrid.org
      DNProperties
      {
        -C_FR-O_DIRAC-OU_DIRAC TEST-CN_DIRAC test user-emailAddress_testuser@diracgrid.org
        {
          ProxyProviders = 
          Groups = dirac_user
        }
      }
    }
    userWithDN_WithProxyProviderInDNProperties
    {
      DN = /C=FR/O=DIRAC/OU=DIRAC TEST/CN=DIRAC test user/emailAddress=testuser@diracgrid.org
      DNProperties
      {
        -C_FR-O_DIRAC-OU_DIRAC TEST-CN_DIRAC test user-emailAddress_testuser@diracgrid.org
        {
          ProxyProviders = DIRAC_TEST_CA
          Groups = 
        }
      }
    }
    userWithDN_WithEmptyDNProperties
    {
      DN = /C=FR/O=DIRAC/OU=DIRAC TEST/CN=DIRAC test user/emailAddress=testuser@diracgrid.org
      DNProperties
      {
        -C_FR-O_DIRAC-OU_DIRAC TEST-CN_DIRAC test user-emailAddress_testuser@diracgrid.org
        {
        }
      }
    }
    userWithDNAndID
    {
      DN = /C=FR/O=DIRAC/OU=DIRAC TEST/CN=DIRAC test user/emailAddress=testuser@diracgrid.org
      ID = 1234567890
    }
    userWithDN
    {
      DN = /C=FR/O=DIRAC/OU=DIRAC TEST/CN=DIRAC test user/emailAddress=testuser@diracgrid.org
    }

    # Not in dirac_user group
    userWithDN_WithGroupAndProxyProviderInDNProperties_NotInGroup
    {
      DN = /C=FR/O=DIRAC/OU=DIRAC TEST/CN=DIRAC test user/emailAddress=testuser@diracgrid.org
      DNProperties
      {
        -C_FR-O_DIRAC-OU_DIRAC TEST-CN_DIRAC test user-emailAddress_testuser@diracgrid.org
        {
          ProxyProviders = DIRAC_TEST_CA
          Groups = dirac_user
        }
      }
    }
    userWithDN_WithGroupInDNProperties_NotInGroup
    {
      DN = /C=FR/O=DIRAC/OU=DIRAC TEST/CN=DIRAC test user/emailAddress=testuser@diracgrid.org
      DNProperties
      {
        -C_FR-O_DIRAC-OU_DIRAC TEST-CN_DIRAC test user-emailAddress_testuser@diracgrid.org
        {
          ProxyProviders = 
          Groups = dirac_user
        }
      }
    }
    userWithDN_WithProxyProviderInDNProperties_NotInGroup
    {
      DN = /C=FR/O=DIRAC/OU=DIRAC TEST/CN=DIRAC test user/emailAddress=testuser@diracgrid.org
      DNProperties
      {
        -C_FR-O_DIRAC-OU_DIRAC TEST-CN_DIRAC test user-emailAddress_testuser@diracgrid.org
        {
          ProxyProviders = DIRAC_TEST_CA
          Groups = 
        }
      }
    }
    userWithDN_WithEmptyDNProperties_NotInGroup
    {
      DN = /C=FR/O=DIRAC/OU=DIRAC TEST/CN=DIRAC test user/emailAddress=testuser@diracgrid.org
      DNProperties
      {
        -C_FR-O_DIRAC-OU_DIRAC TEST-CN_DIRAC test user-emailAddress_testuser@diracgrid.org
        {
        }
      }
    }
    userWithDNAndID_NotInGroup
    {
      DN = /C=FR/O=DIRAC/OU=DIRAC TEST/CN=DIRAC test user/emailAddress=testuser@diracgrid.org
      ID = 1234567890
    }
    userWithDN_NotInGroup
    {
      DN = /C=FR/O=DIRAC/OU=DIRAC TEST/CN=DIRAC test user/emailAddress=testuser@diracgrid.org
    }
  }
  Groups
  {
    dirac_user
    {
      Users = userWithDN, userWithDNAndID, userWithDN_WithEmptyDNProperties
      Users += userWithDN_WithProxyProviderInDNProperties, userWithDN_WithGroupInDNProperties
      Users += userWithDN_WithGroupAndProxyProviderInDNProperties
    }
  }
}
"""


class ProxyDBTestCase(unittest.TestCase):
  
  @classmethod
  def setUpClass(cls):
    cls.db = ProxyDB()
    cfg = CFG()
    cfg.loadFromBuffer(diracTestCACFG)
    gConfig.loadCFG(cfg)
    cfg.loadFromBuffer(userCFG)
    gConfig.loadCFG(cfg)

    userDN = '/C=FR/O=DIRAC/OU=DIRAC TEST/CN=DIRAC test user/emailAddress=testuser@diracgrid.org'
    userGroup = 'dirac_user'
    requiredLifeTime = 43200

    # Create needed for tests rows
    result = getDBParameters('Framework/ProxyDB')
    if not result['OK']:
      raise RuntimeError('Cannot get database parameters: %s' % result['Message'])
    dbParam = result['Value']
    engine = create_engine('mysql://%s:%s@%s:%s/%s' % (dbParam['User'],
                                                       dbParam['Password'],
                                                       dbParam['Host'],
                                                       dbParam['Port'],
                                                       dbParam['DBName']))
    cls.connection = engine.connect()  

  def setUp(self):
    pass

  def tearDown(self):
    pass

  @classmethod
  def tearDownClass(cls):
    cls.connection.execute('DELETE FROM ProxyDB_Proxies WHERE UserName IN ("user_1", "user_2")')
    cls.connection.close()


class testDB(ProxyDBTestCase):

  def test_connectDB(self):
    """ Try to connect to the ProxyDB
    """
    res = self.db._connect()
    self.assertTrue(res['OK'])

  def test_getUsers(self):
    """ Try to get users from DB with actual proxy
    """
    # Fill the table
    row = []
    for exp in ["UTC_TIMESTAMP()", "TIMESTAMPADD( SECOND, 43200, UTC_TIMESTAMP() )"]:
      for user in ["user_1", "user_2"]:
        for dn in ["/C=DN/FOR=%s/CN=first" % user, "/C=DN/FOR=%s/CN=second" % user]:
          for group in ["group_X", "group_Y"]:
            row.append('( "%s", "%s", "%s", "%s", "%s" )' % (user, dn, group, 'PEM', exp))
    cmd = "INSERT INTO ProxyDB_Proxies(UserName, UserDN, UserGroup, Pem, ExpirationTime) "
    cmd += "VALUES %s ;" % ', '.join(row)
    self.connection.execute(cmd)
    
    gLogger.info('\n Without arguments\n')
    result = db.getUsers()
    self.assertTrue(result['OK'])
    gLogger.info('\n%s\n' % result['Value'])
    self.assertEqual(result['Value'], [])


  # def getRemoveProxy(self):
  #   """ Some test cases
  #   """
  #   gLogger.info('\n Generate proxy\n')
  #   result = self.db.getProxy(self.userDN, self.userGroup, self.requiredLifeTime)
  #   self.assertTrue(result['OK'])

  #   gLogger.info('\n Get proxy from DB\n')
  #   result = self.db.getProxy(self.userDN, self.userGroup, self.requiredLifeTime)
  #   self.assertTrue(result['OK'])

  #   gLogger.info('\n Delete proxy\n')
  #   result = self.db.deleteProxy(self.userDN, self.userGroup)
  #   self.assertTrue(result['OK'])

  #   gLogger.info('\n Get clean proxy from DB and add group\n')
  #   result = self.db.getProxy(self.userDN, self.userGroup, self.requiredLifeTime)
  #   self.assertTrue(result['OK'])

  #   gLogger.info('\n Delete proxy and clean proxy\n')
  #   result = self.db.deleteProxy(self.userDN, self.userGroup)
  #   self.assertTrue(result['OK'])
  #   result = self.db.deleteProxy(self.userDN, self.userGroup, proxyProvider='DIRAC_TEST_CA')
  #   self.assertTrue(result['OK'])


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(ProxyDBTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(testDB))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
  sys.exit(not testResult.wasSuccessful())
