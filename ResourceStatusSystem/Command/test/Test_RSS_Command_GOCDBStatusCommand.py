""" Test_RSS_Command_GOCDBStatusCommand
"""

import mock
import unittest
import importlib

from datetime import datetime, timedelta

from DIRAC.ResourceStatusSystem.Command.DowntimeCommand import DowntimeCommand
from DIRAC import gLogger


__RCSID__ = '$Id:  $'

################################################################################

class GOCDBStatusCommand_TestCase( unittest.TestCase ):
  
  def setUp( self ):
    """
    Setup
    """
    gLogger.setLevel( 'DEBUG' )
    # Mock external libraries / modules not interesting for the unit test

    self.getGOCSiteNameMock = mock.MagicMock()
    self.CSHelpersMock = mock.MagicMock()
    self.CSHelpersMock.getSEHost.return_value = 'aRealName'
    self.dowtimeCommandModule = importlib.import_module( 'DIRAC.ResourceStatusSystem.Command.DowntimeCommand' )
    self.dowtimeCommandModule.getGOCSiteName = self.getGOCSiteNameMock
    self.dowtimeCommandModule.CSHelpers = self.CSHelpersMock
    self.mock_GOCDBClient = mock.MagicMock()
    self.args = {'name':'aName', 'element':'Resource', 'elementType': 'StorageElement'}

#     mock_GOCDB = mock.MagicMock()
#     mock_GOCDB.getStatus.return_value = { 'OK' : True, 'Value' : None }
#

#     mock_getGOCSiteName = mock.Mock()
#     mock_getGOCSiteName.return_value = { 'OK' : True, 'Value' : 'GOCSiteName' }
#     self.mock_getGOCSiteName = mock_getGOCSiteName
#
#     mock_convertTime = mock.Mock()
#     mock_convertTime.return_value = 2
#     self.mock_convertTime         = mock_convertTime
    
    # Add mocks to moduleTested
#     moduleTested.GOCDBClient = self.mock_GOCDBClient
#     moduleTested.getGOCSiteName   = self.mock_getGOCSiteName
#     moduleTested.convertTime      = self.mock_convertTime
    
#     self.moduleTested = moduleTested
#     self.testClass = self.moduleTested.DowntimeCommand


  def tearDown( self ):
    """
    TearDown
    """
#     del self.testClass
#     del self.moduleTested
    del self.mock_GOCDBClient
    del self.dowtimeCommandModule
#     del self.mock_getGOCSiteName
      
################################################################################
# Tests

class GOCDBStatusCommand_Success( GOCDBStatusCommand_TestCase ):
  
  def test_instantiate( self ):
    """ tests that we can instantiate one object of the tested class
    """
    
    command = DowntimeCommand()
    self.assertEqual( 'DowntimeCommand', command.__class__.__name__ )
  
  def test_init( self ):
    """ tests that the init method does what it should do
    """

    command = DowntimeCommand()
    self.assertEqual( {'onlyCache': False}, command.args )
    self.assertEqual( {}, command.apis )
    
    command = DowntimeCommand( clients = {'GOCDBClient': self.mock_GOCDBClient} )
    self.assertEqual( {'onlyCache': False}, command.args )
    self.assertEqual( {'GOCDBClient': self.mock_GOCDBClient}, command.apis )

    command = DowntimeCommand( self.args )
    _args = dict(self.args)
    _args.update( {'onlyCache': False} )
    self.assertEqual( _args, command.args )
    self.assertEqual( {}, command.apis )

  def test_doCache( self ):
    """ tests the doCache method
    """
    self.mock_GOCDBClient.selectDowntimeCache.return_value = {'OK':True, 'Value':{}}
    command = DowntimeCommand( self.args, {'ResourceManagementClient':self.mock_GOCDBClient} )
    res = command.doCache()
    self.assert_( res['OK'] )
    
    now = datetime.utcnow()
    resFromDB = {'OK':True,
                 'Value':( ( now - timedelta( hours = 2 ),
                             '1 aRealName',
                             'https://blah',
                             now + timedelta( hours = 2 ),
                             'aRealName',
                             now - timedelta( hours = 2 ),
                             'maintenance',
                             'OUTAGE',
                             now,
                             'Resource' ),
                             ( now + timedelta( hours = 12 ),
                               '2 aRealName',
                               'https://blah',
                               now + timedelta( hours = 14 ),
                               'aRealName',
                               now + timedelta( hours = 12 ),
                               'maintenance',
                               'OUTAGE',
                               now,
                               'Resource' )
                          ),
                 'Columns': ['StartDate','DowntimeID', 'Link','EndDate', 'Name',
                             'DateEffective', 'Description', 'Severity','LastCheckTime', 'Element']

                 }

    self.mock_GOCDBClient.selectDowntimeCache.return_value = resFromDB
    command = DowntimeCommand( self.args, {'ResourceManagementClient':self.mock_GOCDBClient} )
    res = command.doCache()
    self.assert_( res['OK'] )
    self.assertEqual( res['Value']['DowntimeID'], '1 aRealName' )

    self.mock_GOCDBClient.selectDowntimeCache.return_value = resFromDB
    self.args.update( {'hours':2} )
    command = DowntimeCommand( self.args, {'ResourceManagementClient':self.mock_GOCDBClient} )
    res = command.doCache()
    self.assert_( res['OK'] )
    self.assertEqual( res['Value']['DowntimeID'], '1 aRealName' )
    

    resFromDB = {'OK':True,
                 'Value':( ( now - timedelta( hours = 12 ),
                             '1 aRealName',
                             'https://blah',
                             now - timedelta( hours = 2 ),
                             'aRealName',
                             now - timedelta( hours = 12 ),
                             'maintenance',
                             'OUTAGE',
                             now,
                             'Resource' ),
                             ( now + timedelta( hours = 2 ),
                               '2 aRealName',
                               'https://blah',
                               now + timedelta( hours = 14 ),
                               'aRealName',
                               now + timedelta( hours = 2 ),
                               'maintenance',
                               'OUTAGE',
                               now,
                               'Resource' )
                          ),
                 'Columns': ['StartDate', 'DowntimeID', 'Link', 'EndDate', 'Name',
                             'DateEffective', 'Description', 'Severity', 'LastCheckTime', 'Element']

                 }

    self.mock_GOCDBClient.selectDowntimeCache.return_value = resFromDB
    self.args.update( {'hours':3} )
    command = DowntimeCommand( self.args, {'ResourceManagementClient':self.mock_GOCDBClient} )
    res = command.doCache()
    self.assert_( res['OK'] )
    self.assertEqual( res['Value']['DowntimeID'], '2 aRealName' )


    resFromDB = {'OK':True,
                 'Value':( ( now - timedelta( hours = 12 ),
                             '1 aRealName',
                             'https://blah',
                             now + timedelta( hours = 2 ),
                             'aRealName',
                             now - timedelta( hours = 12 ),
                             'maintenance',
                             'OUTAGE',
                             now,
                             'Resource' ),
                             ( now + timedelta( hours = 2 ),
                               '2 aRealName',
                               'https://blah',
                               now + timedelta( hours = 14 ),
                               'aRealName',
                               now + timedelta( hours = 2 ),
                               'maintenance',
                               'OUTAGE',
                               now,
                               'Resource' )
                          ),
                 'Columns': ['StartDate', 'DowntimeID', 'Link', 'EndDate', 'Name',
                             'DateEffective', 'Description', 'Severity', 'LastCheckTime', 'Element']

                 }

    self.mock_GOCDBClient.selectDowntimeCache.return_value = resFromDB
    self.args.update( {'hours':3} )
    command = DowntimeCommand( self.args, {'ResourceManagementClient':self.mock_GOCDBClient} )
    res = command.doCache()
    self.assert_( res['OK'] )
    self.assertEqual( res['Value']['DowntimeID'], '1 aRealName' )


# FIXME: rest to be reviewed. The test of the doNew is missing and more important of the doMaster

#   def test_doMaster( self ):
#     """ tests the doMaster method
#     """
#
#     command = DowntimeCommand()
#     res = command.doMaster()
#
#     self.assertEqual( False, res['OK'] )
#
#     command = self.testClass( args = { 'element' : 'X' } )
#     res = command.doMaster()
#     self.assertEqual( False, res[ 'OK' ] )
#
#     command = self.testClass( args = { 'element' : 'X', 'name' : 'Y' } )
#     res = command.doMaster()
#     self.assertEqual( True, res[ 'OK' ] )
#     self.assertEqual( { 'DT' : None }, res[ 'Value' ] )
#
#     mock_GOCDB = mock.Mock()
#     mock_GOCDB.getStatus.return_value = { 'OK'    : True,
#                                           'Value' : {
#                                                      '669 devel.edu.mk': {
#                                                        'HOSTED_BY': 'MK-01-UKIM_II',
#                                                        'DESCRIPTION': 'Problem with SE server',
#                                                        'SEVERITY': 'OUTAGE',
#                                                        'HOSTNAME': 'devel.edu.mk',
#                                                        'GOCDB_PORTAL_URL': 'myURL',
#                                                        'FORMATED_END_DATE': '2011-07-20 00:00',
#                                                        'FORMATED_START_DATE': '2011-07-16 00:00'
#                                                                          }
#                                                       }
#                                         }
#
#     self.moduleTested.GOCDBClient.return_value = mock_GOCDB
#     command = self.testClass( args = { 'element' : 'X', 'name' : 'Y' } )
#     res = command.doMaster()
#
#     self.assertEqual( True, res[ 'OK' ] )
#     self.assertEqual( [ 'DT', 'EndDate' ], res[ 'Value' ].keys() )
#     self.assertEqual( 'OUTAGE', res[ 'Value' ][ 'DT' ] )
#     self.assertEqual( '2011-07-20 00:00', res[ 'Value' ][ 'EndDate' ] )
#
#     command = self.testClass( args = { 'element' : 'X', 'name' : 'Y', 'hours' : 1 } )
#     res = command.doMaster()
#
#     self.assertEqual( True, res[ 'OK' ] )
#     self.assertEqual( [ 'DT', 'EndDate' ], res[ 'Value' ].keys() )
#     self.assertEqual( 'OUTAGE', res[ 'Value' ][ 'DT' ] )
#     self.assertEqual( '2011-07-20 00:00', res[ 'Value' ][ 'EndDate' ] )
#
#     mock_GOCDB = mock.Mock()
#
#     newStartDate = datetime.strftime( datetime.now() + timedelta( hours = 1 ), "%Y-%m-%d %H:%M" )
#
#     mock_GOCDB.getStatus.return_value = { 'OK'    : True,
#                                           'Value' : {
#                                                      '669 devel.edu.mk': {
#                                                        'HOSTED_BY': 'MK-01-UKIM_II',
#                                                        'DESCRIPTION': 'Problem with SE server',
#                                                        'SEVERITY': 'OUTAGE',
#                                                        'HOSTNAME': 'devel.edu.mk',
#                                                        'GOCDB_PORTAL_URL': 'myURL',
#                                                        'FORMATED_END_DATE': '2011-07-20 00:00',
#                                                        'FORMATED_START_DATE': newStartDate
#                                                                          }
#                                                       }
#                                         }
#
#     self.moduleTested.GOCDBClient.return_value = mock_GOCDB
#     command = self.testClass( args = { 'element' : 'X', 'name' : 'Y' } )
#     res = command.doMaster()
#
#     self.assertEqual( True, res[ 'OK' ] )
#     self.assertEqual( [ 'DT', 'EndDate' ], res[ 'Value' ].keys() )
#     self.assertEqual( 'OUTAGE in 3:00:00', res[ 'Value' ][ 'DT' ] )
#     self.assertEqual( '2011-07-20 00:00', res[ 'Value' ][ 'EndDate' ] )
#
#     newStartDate2 = datetime.strftime( datetime.now() + timedelta( hours = 2 ), "%Y-%m-%d %H:%M" )
#
#     mock_GOCDB.getStatus.return_value = { 'OK'    : True,
#                                           'Value' : {
#                                                      '669 devel.edu.mk': {
#                                                        'HOSTED_BY': 'MK-01-UKIM_II',
#                                                        'DESCRIPTION': 'Problem with SE server',
#                                                        'SEVERITY': 'OUTAGE',
#                                                        'HOSTNAME': 'devel.edu.mk',
#                                                        'GOCDB_PORTAL_URL': 'myURL',
#                                                        'FORMATED_END_DATE': '2011-07-20 00:00',
#                                                        'FORMATED_START_DATE': newStartDate
#                                                                          },
#                                                      '669 devel.edu.mk 1': {
#                                                        'HOSTED_BY': 'MK-01-UKIM_II',
#                                                        'DESCRIPTION': 'Problem with SE server',
#                                                        'SEVERITY': 'OUTAGE',
#                                                        'HOSTNAME': 'devel.edu.mk 1',
#                                                        'GOCDB_PORTAL_URL': 'myURL',
#                                                        'FORMATED_END_DATE': '2013-07-20 00:00',
#                                                        'FORMATED_START_DATE': newStartDate2
#                                                                          }
#                                                       }
#                                         }
#
#     self.moduleTested.GOCDBClient.return_value = mock_GOCDB
#     command = self.testClass( args = { 'element' : 'X', 'name' : 'Y' } )
#     res = command.doMaster()
#
#     self.assertEqual( True, res[ 'OK' ] )
#     self.assertEqual( [ 'DT', 'EndDate' ], res[ 'Value' ].keys() )
#     self.assertEqual( 'OUTAGE in 3:00:00', res[ 'Value' ][ 'DT' ] )
#     self.assertEqual( '2011-07-20 00:00', res[ 'Value' ][ 'EndDate' ] )
#
#     newStartDate3 = datetime.strftime( datetime.now() - timedelta( days = 1 ), "%Y-%m-%d %H:%M" )
#
#     mock_GOCDB.getStatus.return_value = { 'OK'    : True,
#                                           'Value' : {
#                                                      '669 devel.edu.mk': {
#                                                        'HOSTED_BY': 'MK-01-UKIM_II',
#                                                        'DESCRIPTION': 'Problem with SE server',
#                                                        'SEVERITY': 'OUTAGE',
#                                                        'HOSTNAME': 'devel.edu.mk',
#                                                        'GOCDB_PORTAL_URL': 'myURL',
#                                                        'FORMATED_END_DATE': '2011-07-20 00:00',
#                                                        'FORMATED_START_DATE': newStartDate
#                                                                          },
#                                                      '669 devel.edu.mk 1': {
#                                                        'HOSTED_BY': 'MK-01-UKIM_II',
#                                                        'DESCRIPTION': 'Problem with SE server',
#                                                        'SEVERITY': 'OUTAGE',
#                                                        'HOSTNAME': 'devel.edu.mk 1',
#                                                        'GOCDB_PORTAL_URL': 'myURL',
#                                                        'FORMATED_END_DATE': '2013-07-20 00:00',
#                                                        'FORMATED_START_DATE': newStartDate3
#                                                                          }
#                                                       }
#                                         }
#
#     self.moduleTested.GOCDBClient.return_value = mock_GOCDB
#     command = self.testClass( args = { 'element' : 'Site', 'name' : 'Y' } )
#     res = command.doMaster()
#
#     self.assertEqual( True, res[ 'OK' ] )
#     self.assertEqual( [ 'DT', 'EndDate' ], res[ 'Value' ].keys() )
#     self.assertEqual( 'OUTAGE', res[ 'Value' ][ 'DT' ] )
#     self.assertEqual( '2013-07-20 00:00', res[ 'Value' ][ 'EndDate' ] )
#
#     # Restore the module
#     self.moduleTested.GOCDBClient = self.mock_GOCDBClient
#     reload( self.moduleTested )
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
