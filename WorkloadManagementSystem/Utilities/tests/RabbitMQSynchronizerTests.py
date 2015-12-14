""" Unit tests for RabbitMQSync
"""

import unittest
from DIRAC.WorkloadManagementSystem.Utilities.RabbitMQSynchronizer import RabbitMQSynchronizer
from DIRAC.WorkloadManagementSystem.Utilities.RabbitMQSynchronizer import getAllowedGroupName
from DIRAC.WorkloadManagementSystem.Utilities.RabbitMQSynchronizer import getAllowedHostProperty
from DIRAC.WorkloadManagementSystem.Utilities.RabbitMQSynchronizer import getSpecialUsersForRabbitMQDatabase

class TestRabbitMQSync( unittest.TestCase ):

  def setUp( self ):
    self.synchronizer =RabbitMQSynchronizer()
    pass
  def tearDown( self ):
    pass

class getAllowedGroupNameTest ( TestRabbitMQSync ):
  def test_success( self ):
    self.assertEqual(getAllowedGroupName(), 'lhcb_pilot')

class getAllowedHostPropertyTest ( TestRabbitMQSync ):
  def test_success( self ):
    self.assertEqual(getAllowedHostProperty(), 'GenericPilot')

class getSpecialUsersForRabbitMQDatabaseTest ( TestRabbitMQSync ):
  def test_success( self ):
    usersResult = getSpecialUsersForRabbitMQDatabase()
    usersSpecial = ['admin', 'dirac', 'ala', 'O=client,CN=kamyk']
    self.assertTrue(set(usersResult) == set(usersSpecial))

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( TestRabbitMQSync  )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( getAllowedHostPropertyTest ))
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( getAllowedGroupNameTest  ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( getSpecialUsersForRabbitMQDatabaseTest  ))
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )


