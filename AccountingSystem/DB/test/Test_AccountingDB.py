""" Test class for AccountingDB 
"""

# imports
import unittest
import importlib
import mock
from mock import MagicMock

import DIRAC.AccountingSystem.DB.AccountingDB as moduleTested

class TestCase( unittest.TestCase ):
  """ Base class for the EmailAction / EmailAgent test cases
  """

  def setUp( self ):

    self.moduleTested = moduleTested
    self.testClass = self.moduleTested.AccountingDB
    # self.moduleTested.OracleDB = mock_OracleDB
    
    self.moduleTested.DB = MagicMock()
    self.moduleTested.DB.fullname = 'AccountingDB'
   
    mock_getDatabaseSection = MagicMock()
    mock_getDatabaseSection.return_value = '/Systems/AccountingSystem/Certification/Databases/AccountingDB'
    self.moduleTested.DB.getDatabaseSection = mock_getDatabaseSection
        
    self.moduleTested.AccountingDB.fullname = MagicMock()
    
    mock_getCSOption = MagicMock()
    mock_getCSOption.return_value = '10'
    self.moduleTested.AccountingDB.getCSOption = mock_getCSOption
    
    mock_ThreadPool = MagicMock() 
    self.moduleTested.ThreadPool = mock_ThreadPool        
    
    mock_createTables = MagicMock()
    mock_createTables.return_value = { 'OK' : True }
    self.moduleTested.AccountingDB._createTables = mock_createTables
    
    mock_loadCatalogFromDB = MagicMock()
    mock_loadCatalogFromDB.return_value = {'OK': True}
    self.moduleTested.AccountingDB._AccountingDB__loadCatalogFromDB = mock_loadCatalogFromDB
    
    mock_registerTypes = MagicMock()
    mock_registerTypes.return_value = {'OK': True}
    self.moduleTested.AccountingDB._AccountingDB__registerTypes = mock_registerTypes
    mock_log = MagicMock()
    mock_log.return_value = {'OK': True}
    self.moduleTested.AccountingDB.log = mock_log
    
class MakeQuery( TestCase ):

  
  def query( self, cmd, conn ):
    """Because we are not able to execute the query, the method returns the query"""
    return cmd
  
  ################################################################################
  def test_instantiate( self ):
    ''' tests that we can instantiate one object of the tested class
    '''

    module = self.testClass()
    self.assertEqual( 'AccountingDB', module.__class__.__name__ )

  def test__queryType1( self ):
    """
    Test the query creation for a given condition
    """
    module = self.testClass()
    module.dbCatalog = {"LHCb-Certification_DataOperation": {
                                                             'definition': {
                                                                            'keys': [( 'OperationType', 'VARCHAR(32)' ),
                                                                                     ( 'User', 'VARCHAR(32)' ),
                                                                                     ( 'ExecutionSite', 'VARCHAR(32)' ),
                                                                                     ( 'Source', 'VARCHAR(32)' ),
                                                                                     ( 'Destination', 'VARCHAR(32)' ),
                                                                                     ( 'Protocol', 'VARCHAR(32)' ),
                                                                                     ( 'FinalStatus', 'VARCHAR(32)' )],
                                                                            'values': [( 'TransferSize', 'BIGINT UNSIGNED' ),
                                                                                       ( 'TransferTime', 'FLOAT' ),
                                                                                       ( 'RegistrationTime', 'FLOAT' ),
                                                                                       ( 'TransferOK', 'INT UNSIGNED' ),
                                                                                       ( 'TransferTotal', 'INT UNSIGNED' ),
                                                                                       ( 'RegistrationOK', 'INT UNSIGNED' ),
                                                                                       ( 'RegistrationTotal', 'INT UNSIGNED' )]},
                                                             'bucketFields': ['OperationType', 'User', 'ExecutionSite', 'Source',
                                                                              'Destination', 'Protocol', 'FinalStatus', 'TransferSize',
                                                                              'TransferTime', 'RegistrationTime', 'TransferOK',
                                                                              'TransferTotal', 'RegistrationOK', 'RegistrationTotal',
                                                                              'entriesInBucket', 'startTime', 'bucketLength'],
                                                             'keys': ['OperationType', 'User', 'ExecutionSite', 'Source',
                                                                      'Destination', 'Protocol', 'FinalStatus'],
                                                             'typeFields': ['OperationType', 'User', 'ExecutionSite',
                                                                            'Source', 'Destination', 'Protocol', 'FinalStatus',
                                                                            'TransferSize', 'TransferTime', 'RegistrationTime',
                                                                            'TransferOK', 'TransferTotal', 'RegistrationOK',
                                                                            'RegistrationTotal', 'startTime', 'endTime'],
                                                             'values': ['TransferSize', 'TransferTime', 'RegistrationTime',
                                                                        'TransferOK', 'TransferTotal', 'RegistrationOK', 'RegistrationTotal'],
                                                              'dataTimespan': 0}}

    module.dbBucketsLength['LHCb-Certification_DataOperation'] = [( 259200, 900 ), ( 691200, 3600 ), ( 15552000, 86400 ), ( 31104000, 604800 )]
    module._query = self.query
    retVal = module._AccountingDB__queryType( "LHCb-Certification_DataOperation",
                                1495324800,
                                1497960715,
                                ( '%s, %s, %s, SUM(%s), SUM(%s)-SUM(%s)', ['Source', 'startTime', 'bucketLength', 'TransferOK', 'TransferTotal', 'TransferOK'] ),
                                {},
                                ( '%s, %s', ['startTime', 'Source'] ),
                                ( '%s', ['startTime'] ),
                                'bucket' )

    self.assertTrue( retVal )
    self.assertEqual( retVal, "SELECT `ac_key_LHCb-Certification_DataOperation_Source`.`value`, `ac_bucket_LHCb-Certification_DataOperation`.`startTime`, `ac_bucket_LHCb-Certification_DataOperation`.`bucketLength`, SUM(`ac_bucket_LHCb-Certification_DataOperation`.`TransferOK`), SUM(`ac_bucket_LHCb-Certification_DataOperation`.`TransferTotal`)-SUM(`ac_bucket_LHCb-Certification_DataOperation`.`TransferOK`) FROM `ac_bucket_LHCb-Certification_DataOperation`, `ac_key_LHCb-Certification_DataOperation_Source` WHERE `ac_bucket_LHCb-Certification_DataOperation`.`startTime` >= 1495324800 AND `ac_bucket_LHCb-Certification_DataOperation`.`startTime` <= 1497963600 AND `ac_bucket_LHCb-Certification_DataOperation`.`Source` = `ac_key_LHCb-Certification_DataOperation_Source`.`id` GROUP BY startTime, `ac_key_LHCb-Certification_DataOperation_Source`.Value, `ac_bucket_LHCb-Certification_DataOperation`.`bucketLength`, TransferTotal, TransferOK ORDER BY startTime" )
  
  def test__queryType2( self ):
    """Test the query creation for a given condition"""
    module = self.testClass()
    module.dbCatalog = {"LHCb-Certification_DataOperation": {
                                                             'definition': {
                                                                            'keys': [( 'OperationType', 'VARCHAR(32)' ),
                                                                                     ( 'User', 'VARCHAR(32)' ),
                                                                                     ( 'ExecutionSite', 'VARCHAR(32)' ),
                                                                                     ( 'Source', 'VARCHAR(32)' ),
                                                                                     ( 'Destination', 'VARCHAR(32)' ),
                                                                                     ( 'Protocol', 'VARCHAR(32)' ),
                                                                                     ( 'FinalStatus', 'VARCHAR(32)' )],
                                                                            'values': [( 'TransferSize', 'BIGINT UNSIGNED' ),
                                                                                       ( 'TransferTime', 'FLOAT' ),
                                                                                       ( 'RegistrationTime', 'FLOAT' ),
                                                                                       ( 'TransferOK', 'INT UNSIGNED' ),
                                                                                       ( 'TransferTotal', 'INT UNSIGNED' ),
                                                                                       ( 'RegistrationOK', 'INT UNSIGNED' ),
                                                                                       ( 'RegistrationTotal', 'INT UNSIGNED' )]},
                                                             'bucketFields': ['OperationType', 'User', 'ExecutionSite', 'Source',
                                                                              'Destination', 'Protocol', 'FinalStatus', 'TransferSize',
                                                                              'TransferTime', 'RegistrationTime', 'TransferOK',
                                                                              'TransferTotal', 'RegistrationOK', 'RegistrationTotal',
                                                                              'entriesInBucket', 'startTime', 'bucketLength'],
                                                             'keys': ['OperationType', 'User', 'ExecutionSite', 'Source',
                                                                      'Destination', 'Protocol', 'FinalStatus'],
                                                             'typeFields': ['OperationType', 'User', 'ExecutionSite',
                                                                            'Source', 'Destination', 'Protocol', 'FinalStatus',
                                                                            'TransferSize', 'TransferTime', 'RegistrationTime',
                                                                            'TransferOK', 'TransferTotal', 'RegistrationOK',
                                                                            'RegistrationTotal', 'startTime', 'endTime'],
                                                             'values': ['TransferSize', 'TransferTime', 'RegistrationTime',
                                                                        'TransferOK', 'TransferTotal', 'RegistrationOK', 'RegistrationTotal'],
                                                              'dataTimespan': 0}}

    module.dbBucketsLength['LHCb-Certification_DataOperation'] = [( 259200, 900 ), ( 691200, 3600 ), ( 15552000, 86400 ), ( 31104000, 604800 )]
    module._query = self.query
    retVal = module._AccountingDB__queryType( "LHCb-Certification_DataOperation",
                                1495411200,
                                1498043438,
                                ('%s, %s, %s, SUM(%s), SUM(%s)-SUM(%s)', ['Source', 'startTime', 'bucketLength', 'TransferOK', 'TransferTotal', 'TransferOK']),
                                {},
                                ( '%s, %s', ['startTime', 'Source'] ),
                                ( '%s', ['startTime'] ),
                                'bucket' )

    self.assertTrue( retVal )
    self.assertEqual( retVal, "SELECT `ac_key_LHCb-Certification_DataOperation_Source`.`value`, `ac_bucket_LHCb-Certification_DataOperation`.`startTime`, `ac_bucket_LHCb-Certification_DataOperation`.`bucketLength`, SUM(`ac_bucket_LHCb-Certification_DataOperation`.`TransferOK`), SUM(`ac_bucket_LHCb-Certification_DataOperation`.`TransferTotal`)-SUM(`ac_bucket_LHCb-Certification_DataOperation`.`TransferOK`) FROM `ac_bucket_LHCb-Certification_DataOperation`, `ac_key_LHCb-Certification_DataOperation_Source` WHERE `ac_bucket_LHCb-Certification_DataOperation`.`startTime` >= 1495411200 AND `ac_bucket_LHCb-Certification_DataOperation`.`startTime` <= 1498046400 AND `ac_bucket_LHCb-Certification_DataOperation`.`Source` = `ac_key_LHCb-Certification_DataOperation_Source`.`id` GROUP BY startTime, `ac_key_LHCb-Certification_DataOperation_Source`.Value, `ac_bucket_LHCb-Certification_DataOperation`.`bucketLength`, TransferTotal, TransferOK ORDER BY startTime" )

#############################################################################
# Test Suite run
#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( TestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( MakeQuery ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
