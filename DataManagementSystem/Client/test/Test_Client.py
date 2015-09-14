import unittest
from mock import Mock



from DIRAC.DataManagementSystem.Client.ConsistencyChecks import ConsistencyChecks




class UtilitiesTestCase( unittest.TestCase ):

  def setUp( self ):
    self.bkClientMock = Mock()
    self.bkClientMock.getFileDescendants.return_value = {'OK': True,
                                                         'Value': {'Failed': [],
                                                                   'NotProcessed': [],
                                                                   'Successful': {'aa.raw': ['bb.raw', 'bb.log']},
                                                                   'WithMetadata': {'aa.raw': {'bb.raw': {'FileType': 'RAW',
                                                                                                          'RunNumber': 97019,
                                                                                                          'GotReplica':'Yes'},
                                                                                               'bb.log': {'FileType': 'LOG',
                                                                                                          'GotReplica':'Yes'}
                                                                                               }
                                                                                    }
                                                                   }
                                                         }
    self.bkClientMock.getFileMetadata.return_value = {'OK': True,
                                                      'Value': {'aa.raw': {'FileType': 'RAW',
                                                                           'RunNumber': 97019},
                                                                'bb.raw': {'FileType': 'RAW',
                                                                           'RunNumber': 97019},
                                                                'dd.raw': {'FileType': 'RAW',
                                                                           'RunNumber': 97019},
                                                                'bb.log': {'FileType': 'LOG'},
                                                                '/bb/pippo/aa.dst':{'FileType': 'DST'},
                                                                '/lhcb/1_2_1.Semileptonic.dst':{'FileType': 'SEMILEPTONIC.DST'},
                                                                '/lhcb/1_1.semileptonic.dst':{'FileType': 'SEMILEPTONIC.DST'}
                                                                }
                                                      }

    self.dmMock = Mock()
    self.dmMock.getReplicas.return_value = {'OK': True, 'Value':{'Successful':{'bb.raw':'metadataPippo'},
                                                                  'Failed':{}}}

    self.cc = ConsistencyChecks( transClient = Mock(), dm = self.dmMock )
    self.cc.fileType = ['SEMILEPTONIC.DST', 'LOG', 'RAW']
    self.cc.fileTypesExcluded = ['LOG']
    self.cc.prod = 0
    self.maxDiff = None

class ConsistencyChecksSuccess( UtilitiesTestCase ):

  def test_getReplicasPresence(self):
    lfnDict = {'aa.raw': {'bb.raw':{'FileType': 'RAW', 'RunNumber': 97019},
                          'bb.log':{'FileType': 'LOG'},
                          '/bb/pippo/aa.dst':{'FileType': 'DST'},
                          '/lhcb/1_2_1.Semileptonic.dst':{'FileType': 'SEMILEPTONIC.DST'}},
               'cc.raw': {'dd.raw':{'FileType': 'RAW', 'RunNumber': 97019},
                          'bb.log':{'FileType': 'LOG'},
                          '/bb/pippo/aa.dst':{'FileType': 'LOG'},
                          '/lhcb/1_1.semileptonic.dst':{'FileType': 'SEMILEPTONIC.DST'}}
               }
    res = self.cc.getReplicasPresence(lfnDict)
    
    print res
    
    lfnDictExpected = {'aa.raw':
                       {'/lhcb/1_2_1.Semileptonic.dst': {'FileType': 'SEMILEPTONIC.DST'},
                        'bb.raw': {'RunNumber': 97019, 'FileType': 'RAW'}},
                       'cc.raw':
                       {'dd.raw': {'RunNumber': 97019, 'FileType': 'RAW'},
                        '/lhcb/1_1.semileptonic.dst': {'FileType': 'SEMILEPTONIC.DST'}}}
    print res
    print lfnDictExpected
    self.assertEqual( res, lfnDictExpected )

    lfnDict = {'aa.raw': {'/bb/pippo/aa.dst':{'FileType': 'LOG'},
                          'bb.log':{'FileType': 'LOG'}
                          }
               }
    res = self.cc._selectByFileType( lfnDict )
    lfnDictExpected = {}
    self.assertEqual( res, lfnDictExpected )
  
  def test__selectByFileType( self ):
    lfnDict = {'aa.raw': {'bb.raw':{'FileType': 'RAW', 'RunNumber': 97019},
                          'bb.log':{'FileType': 'LOG'},
                          '/bb/pippo/aa.dst':{'FileType': 'DST'},
                          '/lhcb/1_2_1.Semileptonic.dst':{'FileType': 'SEMILEPTONIC.DST'}},
               'cc.raw': {'dd.raw':{'FileType': 'RAW', 'RunNumber': 97019},
                          'bb.log':{'FileType': 'LOG'},
                          '/bb/pippo/aa.dst':{'FileType': 'LOG'},
                          '/lhcb/1_1.semileptonic.dst':{'FileType': 'SEMILEPTONIC.DST'}}
               }

    res = self.cc._selectByFileType( lfnDict )

    print res

    lfnDictExpected = {'aa.raw':
                       {'/lhcb/1_2_1.Semileptonic.dst': {'FileType': 'SEMILEPTONIC.DST'},
                        'bb.raw': {'RunNumber': 97019, 'FileType': 'RAW'}},
                       'cc.raw':
                       {'dd.raw': {'RunNumber': 97019, 'FileType': 'RAW'},
                        '/lhcb/1_1.semileptonic.dst': {'FileType': 'SEMILEPTONIC.DST'}}}
    self.assertEqual( res, lfnDictExpected )

    lfnDict = {'aa.raw': {'/bb/pippo/aa.dst':{'FileType': 'LOG'},
                          'bb.log':{'FileType': 'LOG'}
                          }
               }
    res = self.cc._selectByFileType( lfnDict )
    lfnDictExpected = {}
    self.assertEqual( res, lfnDictExpected )

  def test__getFileTypesCount( self ):
    lfnDict = {'aa.raw': {'bb.log':{'FileType': 'LOG'},
                          '/bb/pippo/aa.dst':{'FileType': 'DST'}}}
    res = self.cc._getFileTypesCount( lfnDict )
    resExpected = {'aa.raw': {'DST':1, 'LOG':1}}
    self.assertEqual( res, resExpected )
    
    lfnDict = {'aa.raw': {'bb.log':{'FileType': 'LOG'},
                          '/bb/pippo/aa.dst':{'FileType': 'DST'},
                          '/bb/pippo/cc.dst':{'FileType': 'DST'}}}
    res = self.cc._getFileTypesCount( lfnDict )
    resExpected = {'aa.raw': {'DST':2, 'LOG':1}}
    self.assertEqual( res, resExpected )

        
  def test__catalogDirectoryToSE(self):
    lfnDict = {'aa.raw': {'bb.raw':{'FileType': 'RAW', 'RunNumber': 97019},
                          'bb.log':{'FileType': 'LOG'},
                          '/bb/pippo/aa.dst':{'FileType': 'DST'},
                          '/lhcb/1_2_1.Semileptonic.dst':{'FileType': 'SEMILEPTONIC.DST'}},
               'cc.raw': {'dd.raw':{'FileType': 'RAW', 'RunNumber': 97019},
                          'bb.log':{'FileType': 'LOG'},
                          '/bb/pippo/aa.dst':{'FileType': 'LOG'},
                          '/lhcb/1_1.semileptonic.dst':{'FileType': 'SEMILEPTONIC.DST'}}
               }
    
    res = self.cc.catalogDirectoryToSE(lfnDict)   
    print "compareChecksum", res

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( UtilitiesTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ConsistencyChecksSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 3 ).run( suite )

