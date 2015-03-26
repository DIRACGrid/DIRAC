# imports
import unittest
# sut
import json
import os
from PilotLoggerTools import ( 
    generateDict,
    encodeMessage,
    decodeMessage,
    isMessageFormatCorrect,
    generateUniqueIDAndSaveToFile
    )

class TestPilotLoggerTools( unittest.TestCase ):

  def setUp( self ):
    self.msg = {
      'status': 'Installing',
      'timestamp': '1427121370.7',
      'minorStatus': 'Uname = Linux localhost 3.10.64-85.cernvm.x86_64',
      'pilotID': '1',
      'pilotUUID': 'eda78924-d169-11e4-bfd2-0800275d1a0a',
      'source': 'pilot'
       }
    self.testFile = 'test_file_to_remove'
    self.badFile = '////'
  def tearDown( self ):
    try:
        os.remove( self.testFile )
    except OSError:
        pass

class TestPilotLoggerToolsGenerateDict( TestPilotLoggerTools ):

  def test_success( self ):
    result = generateDict( 
        'eda78924-d169-11e4-bfd2-0800275d1a0a',
        '1',
        'Installing',
        'Uname = Linux localhost 3.10.64-85.cernvm.x86_64',
        '1427121370.7',
        'pilot'
        )
    self.assertEqual( result, self.msg )
  def test_failure( self ):
    result = generateDict( 
        'eda78924-d169-11e4-bfd2-0800275d1a0a',
        '1',
        'AAA Installation',
        'Uname = Linux localhost 3.10.64-85.cernvm.x86_64',
        '1427121370.7',
        'pilot'
        )
    self.assertNotEqual( result, self.msg )

class TestPilotLoggerToolsEncodeMessage( TestPilotLoggerTools ):

  def test_success( self ):
    result = encodeMessage( self.msg )
    standJSON = json.dumps( self.msg )

    self.assertEqual( result, standJSON )
  def test_failure( self ):
    pass

class TestPilotLoggerToolsDecodeMessage( TestPilotLoggerTools ):

  def test_success( self ):
    standJSON = json.dumps( self.msg )
    result = decodeMessage( standJSON )
    self.assertEqual( result, self.msg )

  def test_cosistency( self ):
    result = decodeMessage( encodeMessage( self.msg ) )
    self.assertEqual( result, self.msg )

  def test_fail( self ):
    self.assertRaises( TypeError, decodeMessage, self.msg )


class TestPilotLoggerIsMessageFormatCorrect( TestPilotLoggerTools ):

  def test_success( self ):
    self.assertTrue( isMessageFormatCorrect( self.msg ) )
    
  def test_successEvenThatpilotIDEmpty( self ):
    goodDict = self.msg.copy()
    goodDict['pilotID'] = ''
    self.assertTrue( isMessageFormatCorrect( goodDict ) )

  def test_notDict( self ):
    self.assertFalse( isMessageFormatCorrect( ['a', 2] ) )

  def test_missingKey( self ):
    badDict = self.msg.copy()
    badDict.pop( 'source', None )  # removing one key
    self.assertFalse( isMessageFormatCorrect( badDict ) )

  def test_valuesNotStrings ( self ):
    badDict = self.msg.copy()
    badDict['source'] = 10
    self.assertFalse( isMessageFormatCorrect( badDict ) )

  def test_someValuesAreEmpty( self ):
    badDict = self.msg.copy()
    badDict['timestamp'] = ''
    self.assertFalse( isMessageFormatCorrect( badDict ) )


class TestPilotLoggerGenerateUniqueIDAndSaveToFile( TestPilotLoggerTools ):
  def test_success( self ):
    self.assertTrue( generateUniqueIDAndSaveToFile( self.testFile ) )

  def test_fail( self ):
    self.assertFalse( generateUniqueIDAndSaveToFile( self.badFile ) )

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( TestPilotLoggerTools )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestPilotLoggerToolsGenerateDict ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestPilotLoggerToolsEncodeMessage ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestPilotLoggerToolsDecodeMessage ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestPilotLoggerIsMessageFormatCorrect ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestPilotLoggerGenerateUniqueIDAndSaveToFile ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )

