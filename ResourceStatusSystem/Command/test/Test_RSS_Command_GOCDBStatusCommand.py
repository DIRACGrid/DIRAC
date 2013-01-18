# $HeadURL:  $
''' Test_RSS_Command_GOCDBStatusCommand

'''

import mock
import unittest

import DIRAC.ResourceStatusSystem.Command.GOCDBStatusCommand as moduleTested 

from datetime import datetime, timedelta

__RCSID__ = '$Id:  $'

################################################################################

class GOCDBStatusCommand_TestCase( unittest.TestCase ):
  
  def setUp( self ):
    '''
    Setup
    '''
    
    # Mock external libraries / modules not interesting for the unit test
    mock_GOCDB                        = mock.Mock()
    mock_GOCDB.getStatus.return_value = { 'OK' : True, 'Value' : None } 
    
    mock_GOCDBClient = mock.Mock()
    mock_GOCDBClient.return_value = mock_GOCDB
    self.mock_GOCDBClient = mock_GOCDBClient
    
    mock_getGOCSiteName              = mock.Mock()
    mock_getGOCSiteName.return_value = { 'OK' : True, 'Value' : 'GOCSiteName' }
    self.mock_getGOCSiteName         = mock_getGOCSiteName      
    
    mock_convertTime              = mock.Mock()
    mock_convertTime.return_value = 2
    self.mock_convertTime         = mock_convertTime
    
    # Add mocks to moduleTested
    moduleTested.GOCDBClient      = self.mock_GOCDBClient
    moduleTested.getGOCSiteName   = self.mock_getGOCSiteName
    moduleTested.convertTime      = self.mock_convertTime
    
    self.moduleTested = moduleTested
    self.testClass    = self.moduleTested.GOCDBStatusCommand
    
  def tearDown( self ):
    '''
    TearDown
    '''
    del self.testClass
    del self.moduleTested
    del self.mock_GOCDBClient
    del self.mock_getGOCSiteName
      
################################################################################
# Tests

class GOCDBStatusCommand_Success( GOCDBStatusCommand_TestCase ):
  
  def test_instantiate( self ):
    ''' tests that we can instantiate one object of the tested class
    '''  
    
    command = self.testClass()
    self.assertEqual( 'GOCDBStatusCommand', command.__class__.__name__ )
  
  def test_init( self ):
    ''' tests that the init method does what it should do
    '''
    
    command = self.testClass()  
    self.assertEqual( {}, command.args )      
    self.assertEqual( {}, command.apis )
    
  def test_doCommand( self ):  
    ''' tests the doCommand method
    '''

    command = self.testClass()  
    res = command.doCommand()
    
    self.assertEqual( False, res[ 'OK' ] )
    
    command = self.testClass( args = { 'element' : 'X' } )
    res = command.doCommand()
    self.assertEqual( False, res[ 'OK' ] )
    
    command = self.testClass( args = { 'element' : 'X', 'name' : 'Y' } )
    res = command.doCommand()
    self.assertEqual( True, res[ 'OK' ] )
    self.assertEqual( { 'DT' : None }, res[ 'Value' ] )
    
    mock_GOCDB = mock.Mock()
    mock_GOCDB.getStatus.return_value = { 'OK'    : True, 
                                          'Value' : { 
                                                     '669 devel.edu.mk': {
                                                       'HOSTED_BY': 'MK-01-UKIM_II', 
                                                       'DESCRIPTION': 'Problem with SE server', 
                                                       'SEVERITY': 'OUTAGE', 
                                                       'HOSTNAME': 'devel.edu.mk', 
                                                       'GOCDB_PORTAL_URL': 'myURL', 
                                                       'FORMATED_END_DATE': '2011-07-20 00:00', 
                                                       'FORMATED_START_DATE': '2011-07-16 00:00'
                                                                         }
                                                      }     
                                        }
    
    self.moduleTested.GOCDBClient.return_value = mock_GOCDB
    command = self.testClass( args = { 'element' : 'X', 'name' : 'Y' } )  
    res = command.doCommand()
 
    self.assertEqual( True, res[ 'OK' ] )
    self.assertEqual( [ 'DT', 'EndDate' ], res[ 'Value' ].keys() )
    self.assertEqual( 'OUTAGE', res[ 'Value' ][ 'DT' ] )
    self.assertEqual( '2011-07-20 00:00', res[ 'Value' ][ 'EndDate' ] ) 

    command = self.testClass( args = { 'element' : 'X', 'name' : 'Y', 'hours' : 1 } )  
    res = command.doCommand()
 
    self.assertEqual( True, res[ 'OK' ] )
    self.assertEqual( [ 'DT', 'EndDate' ], res[ 'Value' ].keys() )
    self.assertEqual( 'OUTAGE', res[ 'Value' ][ 'DT' ] )
    self.assertEqual( '2011-07-20 00:00', res[ 'Value' ][ 'EndDate' ] ) 

    mock_GOCDB = mock.Mock()
    
    newStartDate = datetime.strftime( datetime.now() + timedelta( hours = 1 ), "%Y-%m-%d %H:%M" )
    
    mock_GOCDB.getStatus.return_value = { 'OK'    : True, 
                                          'Value' : { 
                                                     '669 devel.edu.mk': {
                                                       'HOSTED_BY': 'MK-01-UKIM_II', 
                                                       'DESCRIPTION': 'Problem with SE server', 
                                                       'SEVERITY': 'OUTAGE', 
                                                       'HOSTNAME': 'devel.edu.mk', 
                                                       'GOCDB_PORTAL_URL': 'myURL', 
                                                       'FORMATED_END_DATE': '2011-07-20 00:00', 
                                                       'FORMATED_START_DATE': newStartDate
                                                                         }
                                                      }     
                                        }
    
    self.moduleTested.GOCDBClient.return_value = mock_GOCDB
    command = self.testClass( args = { 'element' : 'X', 'name' : 'Y' } )  
    res = command.doCommand()
 
    self.assertEqual( True, res[ 'OK' ] )
    self.assertEqual( [ 'DT', 'EndDate' ], res[ 'Value' ].keys() )
    self.assertEqual( 'OUTAGE in 3:00:00', res[ 'Value' ][ 'DT' ] )
    self.assertEqual( '2011-07-20 00:00', res[ 'Value' ][ 'EndDate' ] )     

    newStartDate2 = datetime.strftime( datetime.now() + timedelta( hours = 2 ), "%Y-%m-%d %H:%M" )

    mock_GOCDB.getStatus.return_value = { 'OK'    : True, 
                                          'Value' : { 
                                                     '669 devel.edu.mk': {
                                                       'HOSTED_BY': 'MK-01-UKIM_II', 
                                                       'DESCRIPTION': 'Problem with SE server', 
                                                       'SEVERITY': 'OUTAGE', 
                                                       'HOSTNAME': 'devel.edu.mk', 
                                                       'GOCDB_PORTAL_URL': 'myURL', 
                                                       'FORMATED_END_DATE': '2011-07-20 00:00', 
                                                       'FORMATED_START_DATE': newStartDate
                                                                         },
                                                     '669 devel.edu.mk 1': {
                                                       'HOSTED_BY': 'MK-01-UKIM_II', 
                                                       'DESCRIPTION': 'Problem with SE server', 
                                                       'SEVERITY': 'OUTAGE', 
                                                       'HOSTNAME': 'devel.edu.mk 1', 
                                                       'GOCDB_PORTAL_URL': 'myURL', 
                                                       'FORMATED_END_DATE': '2013-07-20 00:00', 
                                                       'FORMATED_START_DATE': newStartDate2
                                                                         }
                                                      }     
                                        }
    
    self.moduleTested.GOCDBClient.return_value = mock_GOCDB
    command = self.testClass( args = { 'element' : 'X', 'name' : 'Y' } )  
    res = command.doCommand()
 
    self.assertEqual( True, res[ 'OK' ] )
    self.assertEqual( [ 'DT', 'EndDate' ], res[ 'Value' ].keys() )
    self.assertEqual( 'OUTAGE in 3:00:00', res[ 'Value' ][ 'DT' ] )
    self.assertEqual( '2011-07-20 00:00', res[ 'Value' ][ 'EndDate' ] )  

    newStartDate3 = datetime.strftime( datetime.now() - timedelta( days = 1 ), "%Y-%m-%d %H:%M" )

    mock_GOCDB.getStatus.return_value = { 'OK'    : True, 
                                          'Value' : { 
                                                     '669 devel.edu.mk': {
                                                       'HOSTED_BY': 'MK-01-UKIM_II', 
                                                       'DESCRIPTION': 'Problem with SE server', 
                                                       'SEVERITY': 'OUTAGE', 
                                                       'HOSTNAME': 'devel.edu.mk', 
                                                       'GOCDB_PORTAL_URL': 'myURL', 
                                                       'FORMATED_END_DATE': '2011-07-20 00:00', 
                                                       'FORMATED_START_DATE': newStartDate
                                                                         },
                                                     '669 devel.edu.mk 1': {
                                                       'HOSTED_BY': 'MK-01-UKIM_II', 
                                                       'DESCRIPTION': 'Problem with SE server', 
                                                       'SEVERITY': 'OUTAGE', 
                                                       'HOSTNAME': 'devel.edu.mk 1', 
                                                       'GOCDB_PORTAL_URL': 'myURL', 
                                                       'FORMATED_END_DATE': '2013-07-20 00:00', 
                                                       'FORMATED_START_DATE': newStartDate3
                                                                         }
                                                      }     
                                        }
    
    self.moduleTested.GOCDBClient.return_value = mock_GOCDB
    command = self.testClass( args = { 'element' : 'Site', 'name' : 'Y' } )  
    res = command.doCommand()
 
    self.assertEqual( True, res[ 'OK' ] )
    self.assertEqual( [ 'DT', 'EndDate' ], res[ 'Value' ].keys() )
    self.assertEqual( 'OUTAGE', res[ 'Value' ][ 'DT' ] )
    self.assertEqual( '2013-07-20 00:00', res[ 'Value' ][ 'EndDate' ] ) 
 
    # Restore the module
    self.moduleTested.GOCDBClient = self.mock_GOCDBClient
    reload( self.moduleTested )   
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF