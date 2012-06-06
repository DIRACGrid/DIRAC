import unittest, types, time
from DIRAC.RequestManagementSystem.Client.RequestContainer import RequestContainer

class reqContainerTestCase( unittest.TestCase ):
  """ Base class for the DataManagementRequest test cases
  """
  def setUp( self ):
    self.reqContainer = RequestContainer()

class GetSetTestCase( reqContainerTestCase ):

  def test_setgetCurrentTime( self ):
    date = time.strftime( '%Y-%m-%d %H:%M:%S' )
    self.reqContainer.setCurrentDate()
    testDate = self.reqContainer.getCurrentDate()
    self.assertEqual( date, testDate )

  def test_setgetJobID( self ):
    jobID = 999
    self.reqContainer.setJobID( jobID )
    testJobID = self.reqContainer.getJobID()
    self.assertEqual( jobID, testJobID )

  def test_setgetOwnerDN( self ):
    dn = '/C=UK/O=eScience/OU=Edinburgh/L=NeSC/CN=andrew cameron smith'
    self.reqContainer.setOwnerDN( dn )
    testDn = self.reqContainer.getOwnerDN()
    self.assertEqual( dn, testDn )

  def test_setgetDIRACSetup( self ):
    instance = 'testInstance'
    self.reqContainer.setDIRACSetup( instance )
    testInstance = self.reqContainer.getDIRACSetup()
    self.assertEqual( instance, testInstance )

  def test_getNumberOfOperations( self ):
    transfers = self.reqContainer.getNumSubRequests( 'transfer' )
    self.assertEqual( 0, transfers )
    registers = self.reqContainer.getNumSubRequests( 'register' )
    self.assertEqual( 0, registers )
    removals = self.reqContainer.getNumSubRequests( 'removal' )
    self.assertEqual( 0, removals )
    stages = self.reqContainer.getNumSubRequests( 'stage' )
    self.assertEqual( 0, stages )

  def test_isEmpty( self ):
    result = self.reqContainer.isEmpty()
    self.assert_( result )

class AddOperationsTestCase( reqContainerTestCase ):

  def test_addTransfer( self ):

    # Set up dummy request
    lfn = '/lhcb/production/test/case.lfn'
    reqDic = {'Files':{"File1":{'LFN':lfn, 'Status': 'Waiting', 'Attempt': 1, 'PFN': '', 'Size': 1231231, 'GUID': '7E9CED5A-295B-ED88-CE9A-CF41A62D2175', 'Addler': '', 'Md5': ''}},
              'Datasets':{'Dataset1':'DC06Stripping'},
              'Attributes':{'TargetSE':'CERN-tape', 'Operation':'MoveAndRegister', 'SourceSE':'RAL-tape'}}
    # Add this to transfer type list
    self.reqContainer.addSubRequest( 'transfer', reqDic )
    # Only added one transfer so this should be 1
    transfers = self.reqContainer.getNumSubRequests( 'transfer' )
    self.assertEqual( 1, transfers )
    ind = 0
    # Get the only transfer operation in the request
    testReqDic = self.reqContainer.getSubRequest( ind, 'transfer' )
    # Make sure it is a dictionary
    self.assertEqual( type( testReqDic ), types.DictType )
    # Make sure that the status is waiting
    self.assertEqual( testReqDic['Attributes']['Status'], 'Waiting' )
    # Check that the request is not empty
    result = self.reqContainer.isEmpty()
    self.assertFalse( result )
    # Check that all the keys/value pairs we put in are the ones we get back
    #for key in testReqDic:
      #if reqDic.has_key(key):
        #self.assertEqual(reqDic[key],testReqDic[key])

    # Set the status = 'Done'
    self.reqContainer.setSubRequestStatus( ind, 'transfer', 'Done' )
    testReqDic = self.reqContainer.getSubRequest( ind, 'transfer' )
    # Check that it was set to done.
    self.assertEqual( testReqDic['Attributes']['Status'], 'Done' )
    # Check again that it is empty (which it now should be)
    result = self.reqContainer.isEmpty()
    self.assertFalse( result )
    #Now set the file status to done
    self.reqContainer.setSubRequestFileAttributeValue( ind, 'transfer', lfn, 'Status', 'Done' )
    result = self.reqContainer.isEmpty()
    self.assertTrue( result )

  def test_addRegister( self ):

    # Set up dummy request
    lfn = '/lhcb/production/test/case.lfn'
    reqDic = {'Files':{'File1':{'LFN':lfn, 'Status': 'Waiting', 'Attempt': 1, 'PFN': 'srm://srm.cern.ch/castor/cern.ch/grid/lhcb/production/test/case.lfn', 'Size': 1231231, 'GUID': '7E9CED5A-295B-ED88-CE9A-CF41A62D2175', 'Addler': 'addler32', 'Md5': 'md5'}},
              'Datasets':{'Dataset1':'DC06Stripping'},
              'Attributes':{'TargetSE':'CERN-tape', 'Operation':'RegisterFile', 'Status':'Waiting'}}
    # Add this to transfer type list
    self.reqContainer.addSubRequest( 'register', reqDic )
    # Only added one transfer so this should be 1
    transfers = self.reqContainer.getNumSubRequests( 'register' )
    self.assertEqual( 1, transfers )
    ind = 0
    # Get the only transfer operation in the request
    testReqDic = self.reqContainer.getSubRequest( ind, 'register' )
    # Make sure it is a dictionary
    self.assertEqual( type( testReqDic ), types.DictType )
    # Make sure that the status is waiting
    self.assertEqual( testReqDic['Attributes']['Status'], 'Waiting' )
    # Check that the request is not empty
    result = self.reqContainer.isEmpty()
    self.assertFalse( result )
    # Check that all the keys/value pairs we put in are the ones we get back
    #for key in testReqDic:
    #  if reqDic.has_key(key):
    #    self.assertEqual(reqDic[key],testReqDic[key])

    # Set the status = 'Done'
    self.reqContainer.setSubRequestStatus( ind, 'register', 'Done' )
    testReqDic = self.reqContainer.getSubRequest( ind, 'register' )
    # Check that it was set to done.
    self.assertEqual( testReqDic['Attributes']['Status'], 'Done' )
    # Check again that it is empty (which it now should be)
    result = self.reqContainer.isEmpty()
    self.assertFalse( result )
    #Now set the file status to done
    self.reqContainer.setSubRequestFileAttributeValue( ind, 'register', lfn, 'Status', 'Done' )
    result = self.reqContainer.isEmpty()
    self.assertTrue( result )

  def test_addRemoval( self ):
    # Set up dummy request
    lfn = '/lhcb/production/test/case.lfn'
    reqDic = {'Files':{'File1':{'LFN':lfn, 'Status': 'Waiting', 'Attempt': 1, 'PFN': 'srm://srm.cern.ch/castor/cern.ch/grid/lhcb/production/test/case.lfn', 'Size': 1231231, 'GUID': '7E9CED5A-295B-ED88-CE9A-CF41A62D2175', 'Addler': 'addler32', 'Md5': 'md5'}},
                       'Datasets':{'Dataset1':'DC06Stripping'},
                       'Attributes':{'TargetSE':'CERN-tape', 'Operation':'RemoveReplica', 'Catalogue':'LFC'}}
    # Add this to transfer type list
    self.reqContainer.addSubRequest( 'removal', reqDic )
    # Only added one transfer so this should be 1
    result = self.reqContainer.getNumSubRequests( 'removal' )
    self.assertEqual( 1, result )
    ind = 0
    # Get the only transfer operation in the request
    testReqDic = self.reqContainer.getSubRequest( ind, 'removal' )
    # Make sure it is a dictionary
    self.assertEqual( type( testReqDic ), types.DictType )
    # Make sure that the status is waiting
    self.assertEqual( testReqDic['Attributes']['Status'], 'Waiting' )
    # Check that the request is not empty
    result = self.reqContainer.isEmpty()
    self.assertFalse( result )
    # Check that all the keys/value pairs we put in are the ones we get back
    #for key in testReqDic:
    #  if reqDic.has_key(key):
    #    self.assertEqual(reqDic[key],testReqDic[key])

    # Set the status = 'Done'
    self.reqContainer.setSubRequestStatus( ind, 'removal', 'Done' )
    testReqDic = self.reqContainer.getSubRequest( ind, 'removal' )
    # Check that it was set to done.
    self.assertEqual( testReqDic['Attributes']['Status'], 'Done' )
    # Check again that it is empty (which it now should be)
    result = self.reqContainer.isEmpty()
    self.assertFalse( result )
    #Now set the file status to done
    self.reqContainer.setSubRequestFileAttributeValue( ind, 'removal', lfn, 'Status', 'Done' )
    result = self.reqContainer.isEmpty()
    self.assertTrue( result )

  def test_addStage( self ):
    # Set up dummy request
    lfn = '/lhcb/production/test/case.lfn'
    reqDic = {'Files':{'File1':{'LFN':lfn, 'Status': 'Waiting', 'Attempt': 1, 'PFN': 'srm://srm.cern.ch/castor/cern.ch/grid/lhcb/production/test/case.lfn', 'Size': 1231231, 'GUID': '7E9CED5A-295B-ED88-CE9A-CF41A62D2175', 'Addler': 'addler32', 'Md5': 'md5'}},
              'Datasets':{'Dataset1':'DC06Stripping'},
              'Attributes':{'TargetSE':'CERN-tape', 'Operation':'StageAndPin'}}
    # Add this to transfer type list
    self.reqContainer.addSubRequest( 'stage', reqDic )
    # Only added one transfer so this should be 1
    result = self.reqContainer.getNumSubRequests( 'stage' )
    self.assertEqual( 1, result )
    ind = 0
    # Get the only transfer operation in the request
    testReqDic = self.reqContainer.getSubRequest( ind, 'stage' )
    # Make sure it is a dictionary
    self.assertEqual( type( testReqDic ), types.DictType )
    # Make sure that the status is waiting
    self.assertEqual( testReqDic['Attributes']['Status'], 'Waiting' )
    # Check that the request is not empty
    result = self.reqContainer.isEmpty()
    self.assertFalse( result )
    # Check that all the keys/value pairs we put in are the ones we get back
    #for key in testReqDic:
    #  if reqDic.has_key(key):
    #    self.assertEqual(reqDic[key],testReqDic[key])

    # Set the status = 'Done'
    self.reqContainer.setSubRequestStatus( ind, 'stage', 'Done' )
    testReqDic = self.reqContainer.getSubRequest( ind, 'stage' )
    # Check that it was set to done.
    self.assertEqual( testReqDic['Attributes']['Status'], 'Done' )
    # Check again that it is empty (which it now should be)
    result = self.reqContainer.isEmpty()
    self.assertFalse( result )
    #Now set the file status to done
    self.reqContainer.setSubRequestFileAttributeValue( ind, 'stage', lfn, 'Status', 'Done' )
    result = self.reqContainer.isEmpty()
    self.assertTrue( result )

  def test_toFile( self ):
    lfn = '/lhcb/production/test/case.lfn'
    # Add dummy transfer request
    transferDic = {'Attributes': {'Status': 'Waiting', 'SubRequestID': '7F7C1D94-E452-CD50-204C-EE2E2F1816A9', 'Catalogue':'', 'TargetSE':'CERN-tape', 'Operation':'MoveAndRegister', 'SourceSE':'RAL-tape'},
                   'Files':{'File1':{'LFN':lfn, 'Status': 'Waiting', 'Attempt': 1, 'PFN': '', 'Size': 1231231, 'GUID': '7E9CED5A-295B-ED88-CE9A-CF41A62D2175', 'Addler': '', 'Md5': ''}},
                   'Datasets':{'Dataset1':'DC06Stripping'}}
    self.reqContainer.addSubRequest( 'transfer', transferDic )
    # Add dummy register request
    registerDic = {'Attributes':{'Status': 'Waiting', 'SubRequestID': '7F7C1D94-E452-CD50-204C-EE2E2F1816A9', 'Catalogue':'', 'TargetSE':'CERN-tape', 'Operation':'RegisterFile'},
                   'Files':{'File1':{'LFN':lfn, 'Status': 'Waiting', 'Attempt': 1, 'PFN': 'srm://srm.cern.ch/castor/cern.ch/grid/lhcb/production/test/case.lfn', 'Size': 1231231, 'GUID': '7E9CED5A-295B-ED88-CE9A-CF41A62D2175', 'Addler': 'addler32', 'Md5': 'md5'}},
                   'Datasets':{'Dataset1':'DC06Stripping'}}
    self.reqContainer.addSubRequest( 'register', registerDic )
    # Add dummy removal request
    removalDic = {'Attributes':{'Status': 'Waiting', 'SubRequestID': '7F7C1D94-E452-CD50-204C-EE2E2F1816A9', 'Catalogue':'', 'TargetSE':'CERN-tape', 'Operation':'RemoveReplica', 'Catalogue':'LFC'},
                  'Files':{'File1':{'LFN':lfn, 'Status': 'Waiting', 'Attempt': 1, 'PFN': 'srm://srm.cern.ch/castor/cern.ch/grid/lhcb/production/test/case.lfn', 'Size': 1231231, 'GUID': '7E9CED5A-295B-ED88-CE9A-CF41A62D2175', 'Addler': 'addler32', 'Md5': 'md5'}},
                  'Datasets':{'Dataset1':'DC06Stripping'}}
    self.reqContainer.addSubRequest( 'removal', removalDic )
    # Add dummy stage request
    stageDic = {'Attributes':{'Status': 'Waiting', 'SubRequestID': '7F7C1D94-E452-CD50-204C-EE2E2F1816A9', 'Catalogue':'', 'TargetSE':'CERN-tape', 'Operation':'StageAndPin'},
                'Files':{'File1':{'LFN':lfn, 'Status': 'Waiting', 'Attempt': 1, 'PFN': 'srm://srm.cern.ch/castor/cern.ch/grid/lhcb/production/test/case.lfn', 'Size': 1231231, 'GUID': '7E9CED5A-295B-ED88-CE9A-CF41A62D2175', 'Addler': 'addler32', 'Md5': 'md5'}},
                'Datasets':{'Dataset1':'DC06Stripping'}}
    self.reqContainer.addSubRequest( 'stage', stageDic )
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
    self.assertEqual( string, testString )

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
          self.assertEqual( transferDic['Attributes'][att], transferReqDouble['Attributes'][att] )

    registerReqDouble = self.reqContainer.getSubRequest( 0, 'register' )
    for key in registerDic.keys():
      if key == 'Files':
        self.assertEqual( registerDic['Files'], registerReqDouble['Files'] )
      elif key == 'Datasets':
        self.assertEqual( registerDic[key], registerReqDouble[key] )
      else:
        for att in registerDic['Attributes'].keys():
          self.assertEqual( registerDic['Attributes'][att], registerReqDouble['Attributes'][att] )

    removalReqDouble = self.reqContainer.getSubRequest( 0, 'removal' )
    for key in removalDic.keys():
      if key == 'Files':
        self.assertEqual( removalDic['Files'], removalReqDouble['Files'] )
      elif key == 'Datasets':
        self.assertEqual( removalDic[key], removalReqDouble[key] )
      else:
        for att in removalDic['Attributes'].keys():
          self.assertEqual( removalDic['Attributes'][att], removalReqDouble['Attributes'][att] )

    stageReqDouble = self.reqContainer.getSubRequest( 0, 'stage' )
    for key in stageDic.keys():
      if key == 'Files':
        self.assertEqual( stageDic['Files'], stageReqDouble['Files'] )
      elif key == 'Datasets':
        self.assertEqual( stageDic[key], stageReqDouble[key] )
      else:
        for att in stageDic['Attributes'].keys():
          self.assertEqual( stageDic['Attributes'][att], stageReqDouble['Attributes'][att] )

if __name__ == '__main__':

  suite = unittest.defaultTestLoader.loadTestsFromTestCase( GetSetTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( AddOperationsTestCase ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )


