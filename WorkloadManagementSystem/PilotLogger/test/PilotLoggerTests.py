""" Unit tests for PilotLogger
"""

__RCSID__ = "$Id$"

# imports
import unittest
# sut
import os
from DIRAC.WorkloadManagementSystem.PilotLogger.PilotLogger import PilotLogger, getPilotIdFromFile
from DIRAC.WorkloadManagementSystem.PilotLogger.PilotLoggerTools import generateUniqueIDAndSaveToFile
class TestPilotLogger( unittest.TestCase ):

  def setUp( self ):
    self.testFile = 'UUID_file_to_test'
    generateUniqueIDAndSaveToFile( self.testFile )
    self.logger = PilotLogger(self.testFile)
    self.badFile = '////'
    self.nonExistentFile = 'abrakadabraToCzaryIMagia'
  def tearDown( self ):
    try:
      os.remove( self.testFile )
    except OSError:
      pass


class TestGetPilotIdFromFile( TestPilotLogger ):

  def test_success( self ):
    id = getPilotIdFromFile( self.testFile )
    self.assertTrue( id )

  def test_failureBadFile( self ):
    id = getPilotIdFromFile( self.badFile )
    self.assertFalse( id )

  def test_failureNonExistent( self ):
    id = getPilotIdFromFile( self.nonExistentFile )
    self.assertFalse( id )

class TestPilotLogger_isCorrectFlag( TestPilotLogger ):

  def test_success( self ):
    self.assertTrue( self.logger._isCorrectFlag( 'info' ) )
    self.assertTrue( self.logger._isCorrectFlag( 'warning' ) )
    self.assertTrue( self.logger._isCorrectFlag( 'error' ) )
    self.assertTrue( self.logger._isCorrectFlag( 'debug' ) )

  def test_failure( self ):
    self.assertFalse( self.logger._isCorrectFlag( 'mamma Mia' ) )

  def test_failureEmpty( self ):
    self.assertFalse( self.logger._isCorrectFlag( '' ) )

class TestPilotLogger_sendMessage( TestPilotLogger ):

  # here some mocks needed
  def test_success( self ):
    pass

  def test_NotCorrectFlag( self ):
    self.assertFalse( self.logger._sendMessage( '', 'badFlag' ) )

class TestPilotLoggersendMessage( TestPilotLogger ):

  # here some mocks needed
  def test_success( self ):
    pass

  def test_failure( self ):
    pass

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( TestPilotLogger )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestGetPilotIdFromFile ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestPilotLogger_isCorrectFlag ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestPilotLogger_sendMessage ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestPilotLoggersendMessage ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
