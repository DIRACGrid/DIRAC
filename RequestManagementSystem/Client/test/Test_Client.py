"""
  wrong directory and OBSOLETE
  K.C.

"""
import unittest, types, time, os
from DIRAC.RequestManagementSystem.Client.RequestContainer import RequestContainer

class reqContainerTestCase( unittest.TestCase ):
  """ Base class for the Request Client test cases
  """
  def setUp( self ):
    self.reqContainer = RequestContainer()

  def tearDown( self ):
    try:
      os.remove( 'testRequest.xml' )
    except OSError:
      pass
    
class testBasic(reqContainerTestCase):
  
  def test__getLastOrder(self):
    # no files
    req = RequestContainer()
    res = req._getLastOrder()
    self.assertEqual( res, 0 )

    self.assertEqual( req.subRequests, {} )

    req.addSubRequest( {'Attributes':{'Operation':'replicateAndRegister',
                                      'TargetSE':'SE', 'ExecutionOrder': 0}},
                      'transfer' )
    res = req._getLastOrder()
    self.assertEqual( res, 0 )

    req.addSubRequest( {'Attributes':{'Operation':'replicateAndRegister',
                                      'TargetSE':'SE', 'ExecutionOrder': 1}},
                      'transfer' )
    res = req._getLastOrder()
    self.assertEqual( res, 1 )

    del( req )

    # with files
    req = RequestContainer()
    res = req._getLastOrder( 'foo' )
    self.assertEqual( res, 0 )

    req.addSubRequest( {'Attributes':{'Operation':'replicateAndRegister',
                                      'TargetSE':'SE', 'ExecutionOrder': 1}},
                      'transfer' )
    res = req._getLastOrder( 'foo' )
    self.assertEqual( res, 0 )

    req.setSubRequestFiles( 0, 'transfer', [{'LFN':'foo', 'Status':'Waiting'}] )
    res = req._getLastOrder( 'foo' )
    self.assertEqual( res, 1 )


    req.addSubRequest( {'Attributes':{'Operation':'replicateAndRegister',
                                      'TargetSE':'SE', 'ExecutionOrder': 2}},
                      'removal' )
    res = req._getLastOrder( 'foo' )
    self.assertEqual( res, 1 )

    req.setSubRequestFiles( 0, 'removal', [{'LFN':'foo', 'Status':'Waiting'}] )
    res = req._getLastOrder( 'foo' )
    self.assertEqual( res, 2 )



class GetSetTestCase( reqContainerTestCase ):

  def test_setgetJobID( self ):
    jobID = 999
    self.reqContainer.setJobID( jobID )
    testJobID = self.reqContainer.getJobID()
    self.assertEqual( jobID, testJobID['Value'] )

  def test_setgetOwnerDN( self ):
    dn = '/C=UK/O=eScience/OU=Edinburgh/L=NeSC/CN=andrew cameron smith'
    self.reqContainer.setOwnerDN( dn )
    testDn = self.reqContainer.getOwnerDN()
    self.assertEqual( dn, testDn['Value'] )

  def test_setgetDIRACSetup( self ):
    instance = 'testInstance'
    self.reqContainer.setDIRACSetup( instance )
    testInstance = self.reqContainer.getDIRACSetup()
    self.assertEqual( instance, testInstance['Value'] )

  def test_getNumberOfOperations( self ):
    transfers = self.reqContainer.getNumSubRequests( 'transfer' )
    self.assertEqual( 0, transfers['Value'] )
    registers = self.reqContainer.getNumSubRequests( 'register' )
    self.assertEqual( 0, registers['Value'] )
    removals = self.reqContainer.getNumSubRequests( 'removal' )
    self.assertEqual( 0, removals['Value'] )
    stages = self.reqContainer.getNumSubRequests( 'stage' )
    self.assertEqual( 0, stages['Value'] )

  def test_isEmpty( self ):
    result = self.reqContainer.isEmpty()
    self.assert_( result )

class AddOperationsTestCase( reqContainerTestCase ):

  def test_addSubRequest( self ):
    rc_o = RequestContainer()
    op1_Index = rc_o.addSubRequest( {'Attributes': {'SubRequestID': 'x', 'CreationTime': '2012-06-06 14:53:43.763743', 'Operation': 'op1'}},
                                    'someType' )
    op1_Index = op1_Index['Value']
    subRequestExpected = {'someType': [{'Files': [], 'Attributes': {'Status': 'Waiting', 'LastUpdate': '', 'TargetSE': '', 'ExecutionOrder': 0,
                                                                    'SubRequestID': 'x', 'CreationTime': '2012-06-06 14:53:43.763743',
                                                                    'Catalogue': '', 'Error': '', 'Operation': 'op1'},
                                        'Datasets': []}]}
    self.assertEqual( rc_o.subRequests, subRequestExpected )

    op2_index = rc_o.addSubRequest( {'Attributes': {'SubRequestID': 'x', 'CreationTime': '2012-06-06 14:53:43.763743', 'Operation': 'op2'}},
                                    'someType' )
    op2_index = op2_index['Value']
    subRequestExpected = {
                          'someType':
                                      [
                                       {
                                        'Files': [],
                                        'Attributes': {'Status': 'Waiting', 'LastUpdate': '', 'TargetSE': '', 'ExecutionOrder': 0,
                                                       'SubRequestID': 'x', 'CreationTime': '2012-06-06 14:53:43.763743',
                                                       'Catalogue': '', 'Error': '', 'Operation': 'op1'},
                                        'Datasets': []
                                        },
                                       {
                                        'Files': [],
                                        'Attributes': {'Status': 'Waiting', 'LastUpdate': '', 'TargetSE': '', 'ExecutionOrder': 0,
                                                       'SubRequestID': 'x', 'CreationTime': '2012-06-06 14:53:43.763743',
                                                       'Catalogue': '', 'Error': '', 'Operation': 'op2'},
                                        'Datasets': []
                                        }
                                      ]
                          }
    self.assertEqual( rc_o.subRequests, subRequestExpected )

    rc_o.addSubRequest( {'Attributes': {'SubRequestID': 'x', 'CreationTime': '2012-06-06 14:53:43.763743', 'ExecutionOrder': 'last'}},
                        'someType' )
    subRequestExpected = {
                          'someType':
                                      [
                                       {
                                        'Files': [],
                                        'Attributes': {'Status': 'Waiting', 'LastUpdate': '', 'TargetSE': '', 'ExecutionOrder': 0,
                                                       'SubRequestID': 'x', 'CreationTime': '2012-06-06 14:53:43.763743',
                                                       'Catalogue': '', 'Error': '', 'Operation': 'op1'},
                                        'Datasets': []
                                        },
                                       {
                                        'Files': [],
                                        'Attributes': {'Status': 'Waiting', 'LastUpdate': '', 'TargetSE': '', 'ExecutionOrder': 0,
                                                       'SubRequestID': 'x', 'CreationTime': '2012-06-06 14:53:43.763743',
                                                       'Catalogue': '', 'Error': '', 'Operation': 'op2'},
                                        'Datasets': []
                                        },
                                       {
                                        'Files': [],
                                        'Attributes': {'Status': 'Waiting', 'LastUpdate': '', 'TargetSE': '', 'ExecutionOrder': 1,
                                                       'SubRequestID': 'x', 'CreationTime': '2012-06-06 14:53:43.763743',
                                                       'Catalogue': '', 'Error': '', 'Operation': ''},
                                        'Datasets': []
                                        }
                                      ]
                          }
    self.assertEqual( rc_o.subRequests, subRequestExpected )


    rc_o.addSubRequest( {'Attributes': {'SubRequestID': 'x', 'CreationTime': '2012-06-06 14:53:43.763743', 'ExecutionOrder': 'last'}},
                        'someOtherType' )
    subRequestExpected = {
                          'someType':
                                      [
                                       {
                                        'Files': [],
                                        'Attributes': {'Status': 'Waiting', 'LastUpdate': '', 'TargetSE': '', 'ExecutionOrder': 0,
                                                       'SubRequestID': 'x', 'CreationTime': '2012-06-06 14:53:43.763743',
                                                       'Catalogue': '', 'Error': '', 'Operation': 'op1'},
                                        'Datasets': []
                                        },
                                       {
                                        'Files': [],
                                        'Attributes': {'Status': 'Waiting', 'LastUpdate': '', 'TargetSE': '', 'ExecutionOrder': 0,
                                                       'SubRequestID': 'x', 'CreationTime': '2012-06-06 14:53:43.763743',
                                                       'Catalogue': '', 'Error': '', 'Operation': 'op2'},
                                        'Datasets': []
                                        },
                                       {
                                        'Files': [],
                                        'Attributes': {'Status': 'Waiting', 'LastUpdate': '', 'TargetSE': '', 'ExecutionOrder': 1,
                                                       'SubRequestID': 'x', 'CreationTime': '2012-06-06 14:53:43.763743',
                                                       'Catalogue': '', 'Error': '', 'Operation': ''},
                                        'Datasets': []
                                        }
                                      ],
                          'someOtherType':
                                      [
                                       {
                                        'Files': [],
                                        'Attributes': {'Status': 'Waiting', 'LastUpdate': '', 'TargetSE': '', 'ExecutionOrder': 2,
                                                       'SubRequestID': 'x', 'CreationTime': '2012-06-06 14:53:43.763743',
                                                       'Catalogue': '', 'Error': '', 'Operation': ''},
                                        'Datasets': []
                                        },
                                       ]
                          }
    self.assertEqual( rc_o.subRequests, subRequestExpected )

    fileDict = {'LFN':'foo', 'Status':'Waiting'}
    rc_o.setSubRequestFiles( op1_Index, 'someType', [fileDict] )

    subRequestExpected = {
                          'someType':
                                      [
                                       {
                                        'Files': [{'LFN':'foo', 'Status':'Waiting'}],
                                        'Attributes': {'Status': 'Waiting', 'LastUpdate': '', 'TargetSE': '', 'ExecutionOrder': 0,
                                                       'SubRequestID': 'x', 'CreationTime': '2012-06-06 14:53:43.763743',
                                                       'Catalogue': '', 'Error': '', 'Operation': 'op1'},
                                        'Datasets': []
                                        },
                                       {
                                        'Files': [],
                                        'Attributes': {'Status': 'Waiting', 'LastUpdate': '', 'TargetSE': '', 'ExecutionOrder': 0,
                                                       'SubRequestID': 'x', 'CreationTime': '2012-06-06 14:53:43.763743',
                                                       'Catalogue': '', 'Error': '', 'Operation': 'op2'},
                                        'Datasets': []
                                        },
                                       {
                                        'Files': [],
                                        'Attributes': {'Status': 'Waiting', 'LastUpdate': '', 'TargetSE': '', 'ExecutionOrder': 1,
                                                       'SubRequestID': 'x', 'CreationTime': '2012-06-06 14:53:43.763743',
                                                       'Catalogue': '', 'Error': '', 'Operation': ''},
                                        'Datasets': []
                                        }
                                      ],
                          'someOtherType':
                                      [
                                       {
                                        'Files': [],
                                        'Attributes': {'Status': 'Waiting', 'LastUpdate': '', 'TargetSE': '', 'ExecutionOrder': 2,
                                                       'SubRequestID': 'x', 'CreationTime': '2012-06-06 14:53:43.763743',
                                                       'Catalogue': '', 'Error': '', 'Operation': ''},
                                        'Datasets': []
                                        },
                                       ]
                          }
    self.assertEqual( rc_o.subRequests, subRequestExpected )

    fileLastOp = rc_o._getLastOrder( 'foo' )
    rc_o.addSubRequest( {'Attributes': {'SubRequestID': 'x', 'CreationTime': '2012-06-06 14:53:43.763743', 'ExecutionOrder': fileLastOp + 1}},
                        'someOtherType' )

    subRequestExpected = {
                          'someType':
                                      [
                                       {
                                        'Files': [{'LFN':'foo', 'Status':'Waiting'}],
                                        'Attributes': {'Status': 'Waiting', 'LastUpdate': '', 'TargetSE': '', 'ExecutionOrder': 0,
                                                       'SubRequestID': 'x', 'CreationTime': '2012-06-06 14:53:43.763743',
                                                       'Catalogue': '', 'Error': '', 'Operation': 'op1'},
                                        'Datasets': []
                                        },
                                       {
                                        'Files': [],
                                        'Attributes': {'Status': 'Waiting', 'LastUpdate': '', 'TargetSE': '', 'ExecutionOrder': 0,
                                                       'SubRequestID': 'x', 'CreationTime': '2012-06-06 14:53:43.763743',
                                                       'Catalogue': '', 'Error': '', 'Operation': 'op2'},
                                        'Datasets': []
                                        },
                                       {
                                        'Files': [],
                                        'Attributes': {'Status': 'Waiting', 'LastUpdate': '', 'TargetSE': '', 'ExecutionOrder': 1,
                                                       'SubRequestID': 'x', 'CreationTime': '2012-06-06 14:53:43.763743',
                                                       'Catalogue': '', 'Error': '', 'Operation': ''},
                                        'Datasets': []
                                        }
                                      ],
                          'someOtherType':
                                      [
                                       {
                                        'Files': [],
                                        'Attributes': {'Status': 'Waiting', 'LastUpdate': '', 'TargetSE': '', 'ExecutionOrder': 2,
                                                       'SubRequestID': 'x', 'CreationTime': '2012-06-06 14:53:43.763743',
                                                       'Catalogue': '', 'Error': '', 'Operation': ''},
                                        'Datasets': []
                                        },
                                       {
                                        'Files': [],
                                        'Attributes': {'Status': 'Waiting', 'LastUpdate': '', 'TargetSE': '', 'ExecutionOrder': 1,
                                                       'SubRequestID': 'x', 'CreationTime': '2012-06-06 14:53:43.763743',
                                                       'Catalogue': '', 'Error': '', 'Operation': ''},
                                        'Datasets': []
                                        },
                                       ]
                          }
    self.assertEqual( rc_o.subRequests, subRequestExpected )


  def test_addTransfer( self ):

    # Set up dummy request
    lfn = '/lhcb/production/test/case.lfn'
    reqDic = {'Files':[{'LFN':lfn, 'Status': 'Waiting', 'Attempt': 1, 'PFN': '', 'Size': 1231231,
                        'GUID': '7E9CED5A-295B-ED88-CE9A-CF41A62D2175', 'Addler': '', 'Md5': ''}],
              'Datasets':{'Dataset1':'DC06Stripping'},
              'Attributes':{'TargetSE':'CERN-tape', 'Operation':'MoveAndRegister', 'SourceSE':'RAL-tape'}}
    # Add this to transfer type list
    self.reqContainer.addSubRequest( reqDic, 'transfer' )
    # Only added one transfer so this should be 1
    transfers = self.reqContainer.getNumSubRequests( 'transfer' )
    self.assertEqual( 1, transfers['Value'] )
    ind = 0
    # Get the only transfer operation in the request
    testReqDic = self.reqContainer.getSubRequest( ind, 'transfer' )
    # Make sure it is a dictionary
    self.assertEqual( type( testReqDic ), types.DictType )
    # Make sure that the status is waiting
    self.assertEqual( testReqDic['Value']['Attributes']['Status'], 'Waiting' )
    # Check that the request is not empty
    result = self.reqContainer.isEmpty()
    self.assertFalse( result['Value'] )
    # Check that all the keys/value pairs we put in are the ones we get back
    #for key in testReqDic:
      #if reqDic.has_key(key):
        #self.assertEqual(reqDic[key],testReqDic[key])

    # Set the status = 'Done'
    self.reqContainer.setSubRequestStatus( ind, 'transfer', 'Done' )
    testReqDic = self.reqContainer.getSubRequest( ind, 'transfer' )
    # Check that it was set to done.
    self.assertEqual( testReqDic['Value']['Attributes']['Status'], 'Done' )
    # Check again that it is empty (which it now should be)
    result = self.reqContainer.isEmpty()
    self.assertTrue( result['Value'] )
    #Now set the file status to done
    self.reqContainer.setSubRequestFileAttributeValue( ind, 'transfer', lfn, 'Status', 'Done' )
    result = self.reqContainer.isEmpty()
    self.assertTrue( result )

  def test_addRegister( self ):

    # Set up dummy request
    lfn = '/lhcb/production/test/case.lfn'
    reqDic = {'Files':[{'LFN':lfn, 'Status': 'Waiting', 'Attempt': 1,
                        'PFN': 'srm://srm.cern.ch/castor/cern.ch/grid/lhcb/production/test/case.lfn',
                        'Size': 1231231, 'GUID': '7E9CED5A-295B-ED88-CE9A-CF41A62D2175',
                        'Addler': 'addler32', 'Md5': 'md5'}],
              'Datasets':[{'Dataset1':'DC06Stripping'}],
              'Attributes':{'TargetSE':'CERN-tape', 'Operation':'RegisterFile', 'Status':'Waiting'}}
    # Add this to transfer type list
    self.reqContainer.addSubRequest( reqDic, 'register' )
    # Only added one transfer so this should be 1
    transfers = self.reqContainer.getNumSubRequests( 'register' )
    self.assertEqual( 1, transfers['Value'] )
    ind = 0
    # Get the only transfer operation in the request
    testReqDic = self.reqContainer.getSubRequest( ind, 'register' )
    # Make sure it is a dictionary
    self.assertEqual( type( testReqDic ), types.DictType )
    # Make sure that the status is waiting
    self.assertEqual( testReqDic['Value']['Attributes']['Status'], 'Waiting' )
    # Check that the request is not empty
    result = self.reqContainer.isEmpty()
    self.assertFalse( result['Value'] )
    # Check that all the keys/value pairs we put in are the ones we get back
    #for key in testReqDic:
    #  if reqDic.has_key(key):
    #    self.assertEqual(reqDic[key],testReqDic[key])

    # Set the status = 'Done'
    self.reqContainer.setSubRequestStatus( ind, 'register', 'Done' )
    testReqDic = self.reqContainer.getSubRequest( ind, 'register' )
    # Check that it was set to done.
    self.assertEqual( testReqDic['Value']['Attributes']['Status'], 'Done' )
    # Check again that it is empty (which it now should be)
    result = self.reqContainer.isEmpty()
    self.assertTrue( result['Value'] )
    #Now set the file status to done
    self.reqContainer.setSubRequestFileAttributeValue( ind, 'register', lfn, 'Status', 'Done' )
    result = self.reqContainer.isEmpty()
    self.assertTrue( result )

  def test_addRemoval( self ):
    # Set up dummy request
    lfn = '/lhcb/production/test/case.lfn'
    reqDic = {'Files':[{'LFN':lfn, 'Status': 'Waiting', 'Attempt': 1,
                        'PFN': 'srm://srm.cern.ch/castor/cern.ch/grid/lhcb/production/test/case.lfn',
                        'Size': 1231231, 'GUID': '7E9CED5A-295B-ED88-CE9A-CF41A62D2175',
                        'Addler': 'addler32', 'Md5': 'md5'}],
                       'Datasets':[{'Dataset1':'DC06Stripping'}],
                       'Attributes':{'TargetSE':'CERN-tape', 'Operation':'RemoveReplica', 'Catalogue':'LFC'}}
    # Add this to transfer type list
    self.reqContainer.addSubRequest( reqDic, 'removal' )
    # Only added one transfer so this should be 1
    result = self.reqContainer.getNumSubRequests( 'removal' )
    self.assertEqual( 1, result['Value'] )
    ind = 0
    # Get the only transfer operation in the request
    testReqDic = self.reqContainer.getSubRequest( ind, 'removal' )
    # Make sure it is a dictionary
    self.assertEqual( type( testReqDic ), types.DictType )
    # Make sure that the status is waiting
    self.assertEqual( testReqDic['Value']['Attributes']['Status'], 'Waiting' )
    # Check that the request is not empty
    result = self.reqContainer.isEmpty()
    self.assertFalse( result['Value'] )
    # Check that all the keys/value pairs we put in are the ones we get back
    #for key in testReqDic:
    #  if reqDic.has_key(key):
    #    self.assertEqual(reqDic[key],testReqDic[key])

    # Set the status = 'Done'
    self.reqContainer.setSubRequestStatus( ind, 'removal', 'Done' )
    testReqDic = self.reqContainer.getSubRequest( ind, 'removal' )
    # Check that it was set to done.
    self.assertEqual( testReqDic['Value']['Attributes']['Status'], 'Done' )
    # Check again that it is empty (which it now should be)
    result = self.reqContainer.isEmpty()
    self.assertTrue( result['Value'] )
    #Now set the file status to done
    self.reqContainer.setSubRequestFileAttributeValue( ind, 'removal', lfn, 'Status', 'Done' )
    result = self.reqContainer.isEmpty()
    self.assertTrue( result )

  def test_addStage( self ):
    # Set up dummy request
    lfn = '/lhcb/production/test/case.lfn'
    reqDic = {'Files':[{'LFN':lfn, 'Status': 'Waiting', 'Attempt': 1,
                        'PFN': 'srm://srm.cern.ch/castor/cern.ch/grid/lhcb/production/test/case.lfn',
                        'Size': 1231231, 'GUID': '7E9CED5A-295B-ED88-CE9A-CF41A62D2175',
                        'Addler': 'addler32', 'Md5': 'md5'}],
              'Datasets':[{'Dataset1':'DC06Stripping'}],
              'Attributes':{'TargetSE':'CERN-tape', 'Operation':'StageAndPin'}}
    # Add this to transfer type list
    self.reqContainer.addSubRequest( reqDic, 'stage' )
    # Only added one transfer so this should be 1
    result = self.reqContainer.getNumSubRequests( 'stage' )
    self.assertEqual( 1, result['Value'] )
    ind = 0
    # Get the only transfer operation in the request
    testReqDic = self.reqContainer.getSubRequest( ind, 'stage' )
    # Make sure it is a dictionary
    self.assertEqual( type( testReqDic ), types.DictType )
    # Make sure that the status is waiting
    self.assertEqual( testReqDic['Value']['Attributes']['Status'], 'Waiting' )
    # Check that the request is not empty
    result = self.reqContainer.isEmpty()
    self.assertFalse( result['Value'] )
    # Check that all the keys/value pairs we put in are the ones we get back
    #for key in testReqDic:
    #  if reqDic.has_key(key):
    #    self.assertEqual(reqDic[key],testReqDic[key])

    # Set the status = 'Done'
    self.reqContainer.setSubRequestStatus( ind, 'stage', 'Done' )
    testReqDic = self.reqContainer.getSubRequest( ind, 'stage' )
    # Check that it was set to done.
    self.assertEqual( testReqDic['Value']['Attributes']['Status'], 'Done' )
    # Check again that it is empty (which it now should be)
    result = self.reqContainer.isEmpty()
    self.assertTrue( result['Value'] )
    #Now set the file status to done
    self.reqContainer.setSubRequestFileAttributeValue( ind, 'stage', lfn, 'Status', 'Done' )
    result = self.reqContainer.isEmpty()
    self.assertTrue( result )

  def test_toFile( self ):
    lfn = '/lhcb/production/test/case.lfn'
    # Add dummy transfer request
    transferDic = {'Attributes': {'Status': 'Waiting', 'SubRequestID': '7F7C1D94-E452-CD50-204C-EE2E2F1816A9',
                                  'Catalogue':'', 'TargetSE':'CERN-tape', 'Operation':'MoveAndRegister', 'SourceSE':'RAL-tape'},
                   'Files':[{'LFN':lfn, 'Status': 'Waiting', 'Attempt': 1, 'PFN': '', 'Size': 1231231,
                             'GUID': '7E9CED5A-295B-ED88-CE9A-CF41A62D2175', 'Addler': '', 'Md5': ''}],
                   'Datasets':[{'Dataset1':'DC06Stripping'}]
                   }
    self.reqContainer.addSubRequest( transferDic, 'transfer' )
    # Add dummy register request
    registerDic = {'Attributes':{'Status': 'Waiting', 'SubRequestID': '7F7C1D94-E452-CD50-204C-EE2E2F1816A9',
                                 'Catalogue':'', 'TargetSE':'CERN-tape', 'Operation':'RegisterFile'},
                   'Files':[{'LFN':lfn, 'Status': 'Waiting', 'Attempt': 1,
                             'PFN': 'srm://srm.cern.ch/castor/cern.ch/grid/lhcb/production/test/case.lfn',
                             'Size': 1231231, 'GUID': '7E9CED5A-295B-ED88-CE9A-CF41A62D2175', 'Addler': 'addler32', 'Md5': 'md5'}],
                   'Datasets':[{'Dataset1':'DC06Stripping'}]
                   }
    self.reqContainer.addSubRequest( registerDic, 'register' )
    # Add dummy removal request
    removalDic = {'Attributes':{'Status': 'Waiting', 'SubRequestID': '7F7C1D94-E452-CD50-204C-EE2E2F1816A9',
                                'Catalogue':'', 'TargetSE':'CERN-tape', 'Operation':'RemoveReplica', 'Catalogue':'LFC'},
                  'Files':[{'LFN':lfn, 'Status': 'Waiting', 'Attempt': 1,
                            'PFN': 'srm://srm.cern.ch/castor/cern.ch/grid/lhcb/production/test/case.lfn',
                            'Size': 1231231, 'GUID': '7E9CED5A-295B-ED88-CE9A-CF41A62D2175', 'Addler': 'addler32', 'Md5': 'md5'}],
                  'Datasets':[{'Dataset1':'DC06Stripping'}]
                  }
    self.reqContainer.addSubRequest( removalDic, 'removal' )
    # Add dummy stage request
    stageDic = {'Attributes':{'Status': 'Waiting', 'SubRequestID': '7F7C1D94-E452-CD50-204C-EE2E2F1816A9',
                              'Catalogue':'', 'TargetSE':'CERN-tape', 'Operation':'StageAndPin'},
                'Files':[{'LFN':lfn, 'Status': 'Waiting', 'Attempt': 1,
                          'PFN': 'srm://srm.cern.ch/castor/cern.ch/grid/lhcb/production/test/case.lfn',
                          'Size': 1231231, 'GUID': '7E9CED5A-295B-ED88-CE9A-CF41A62D2175', 'Addler': 'addler32', 'Md5': 'md5'}],
                'Datasets':[{'Dataset1':'DC06Stripping'}]
                }
    self.reqContainer.addSubRequest( stageDic, 'stage' )
    # Get the XML string of the DM request
    string = self.reqContainer.toXML()
    fname = 'testRequest.xml'
    # Write the reqContainer to a file
    self.reqContainer.toFile( fname )
    # Get the file contents
    reqfile = open( fname, 'r' )
    testString = reqfile.read()
    reqfile.close()
    # Check the file contents are what is expected
    self.assertEqual( string['Value'], testString )

    testReq = RequestContainer( string )
    # Test that what is obtained when parsing the request is the same as what is given.
    transferReqDouble = self.reqContainer.getSubRequest( 0, 'transfer' )
    for key in transferReqDouble.keys():
      if key == 'Files':
        self.assertEqual( transferDic['Files'], transferReqDouble['Files'] )
      elif key == 'Datasets':
        self.assertEqual( transferDic[key], transferReqDouble[key] )
      else:
        for att in transferDic['Attributes'].keys():
          self.assertEqual( transferDic['Attributes'][att], transferReqDouble['Value']['Attributes'][att] )

#    registerReqDouble = self.reqContainer.getSubRequest( 0, 'register' )
#    for key in registerDic.keys():
#      for att in registerDic['Attributes'].keys():
#        self.assertEqual( registerDic['Attributes'][att], registerReqDouble['Value']['Attributes'][att] )
#      if key == 'Files':
#        self.assertEqual( registerDic['Files'], registerReqDouble['Value']['Files'] )
#      elif key == 'Datasets':
#        self.assertEqual( registerDic[key], registerReqDouble['Value'][key] )
#      else:
#        for att in registerDic['Attributes'].keys():
#          self.assertEqual( registerDic['Attributes'][att], registerReqDouble['Value']['Attributes'][att] )

#    removalReqDouble = self.reqContainer.getSubRequest( 0, 'removal' )
#    for key in removalDic.keys():
#      if key == 'Files':
#        self.assertEqual( removalDic['Files'], removalReqDouble['Value']['Files'] )
#      elif key == 'Datasets':
#        self.assertEqual( removalDic[key], removalReqDouble[key] )
#      else:
#        for att in removalDic['Attributes'].keys():
#          self.assertEqual( removalDic['Attributes'][att], removalReqDouble['Value']['Attributes'][att] )
#
#    stageReqDouble = self.reqContainer.getSubRequest( 0, 'stage' )
#    for key in stageDic.keys():
#      if key == 'Files':
#        self.assertEqual( stageDic['Files'], stageReqDouble['Files'] )
#      elif key == 'Datasets':
#        self.assertEqual( stageDic[key], stageReqDouble[key] )
#      else:
#        for att in stageDic['Attributes'].keys():
#          self.assertEqual( stageDic['Attributes'][att], stageReqDouble['Value']['Attributes'][att] )

if __name__ == '__main__':

  suite = unittest.defaultTestLoader.loadTestsFromTestCase( GetSetTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( AddOperationsTestCase ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )


