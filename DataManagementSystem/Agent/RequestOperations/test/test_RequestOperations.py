import unittest
import datetime
from mock import Mock

from DIRAC.DataManagementSystem.Agent.RequestOperations.ReplicateAndRegister import ReplicateAndRegister
from DIRAC.RequestManagementSystem.Client.File import File

class ReqOpsTestCase( unittest.TestCase ):
  """ Base class for the clients test cases
  """
  def setUp( self ):
    self.rr = ReplicateAndRegister()
    fcMock = Mock()
    self.rr.fc = fcMock

  def tearDown( self ):
    pass

#############################################################################

class ReplicateAndRegisterSuccess( ReqOpsTestCase ):

  def test__addMetadataToFiles( self ):
    resMeta = {'OK': True,
     'Value': {'Failed': {},
               'Successful': {'/lhcb/1.dst': {'CheckSumType': 'AD',
                                              'Checksum': '123456',
                                              'CreationDate': datetime.datetime( 2013, 12, 11, 20, 20, 21 ),
                                              'GUID': '92F9CE97-7A62-E311-8401-0025907FD430',
                                              'Mode': 436,
                                              'ModificationDate': datetime.datetime( 2013, 12, 11, 20, 20, 21 ),
                                              'NumberOfLinks': 1,
                                              'Size': 5846023777,
                                              'Status': '-'},
                              '/lhcb/2.dst': {'CheckSumType': 'AD',
                                              'Checksum': '987654',
                                              'CreationDate': datetime.datetime( 2013, 12, 12, 6, 26, 52 ),
                                              'GUID': 'DAE4933A-C162-E311-8A6B-003048FEAF04',
                                              'Mode': 436,
                                              'ModificationDate': datetime.datetime( 2013, 12, 12, 6, 26, 52 ),
                                              'NumberOfLinks': 1,
                                              'Size': 5893396937,
                                              'Status': '-'}}}}
    
    self.rr.fc.getFileMetadata.return_value = resMeta

    file1 = File()
    file1.LFN = '/lhcb/1.dst'
    file2 = File()
    file2.LFN = '/lhcb/2.dst'
    
    toSchedule = {'/lhcb/1.dst': [file1, ['SE1'], ['SE2', 'SE3']],
                  '/lhcb/2.dst': [file2, ['SE4'], ['SE5', 'SE6']]}
    
    res = self.rr._addMetadataToFiles( toSchedule )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'][0][0]['LFN'], resMeta['Value']['Successful'].keys()[0] )
    self.assertEqual( res['Value'][0][0]['Size'], str( resMeta['Value']['Successful'].values()[0]['Size'] ) )

    self.assertEqual( res['Value'][1][0]['LFN'], resMeta['Value']['Successful'].keys()[1] )
    self.assertEqual( res['Value'][1][0]['Size'], str( resMeta['Value']['Successful'].values()[1]['Size'] ) )


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( ReqOpsTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ReplicateAndRegisterSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
