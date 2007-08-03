import unittest,types,time
from DIRAC.RequestManagementSystem.Client.DataManagementRequest import DataManagementRequest

class DMRequestTestCase(unittest.TestCase):
  """ Base class for the DataManagementRequest test cases
  """
  def setUp(self):
    self.DMRequest = DataManagementRequest()

class GetSetTestCase(DMRequestTestCase):

  def test_setgetCurrentTime(self):
    date = time.strftime('%Y-%m-%d %H:%M:%S')
    self.DMRequest.setCurrentDate()
    testDate = self.DMRequest.getCurrentDate()
    self.assertEqual(date,testDate)

  def test_setgetJobID(self):
    jobID = 999
    self.DMRequest.setJobID(jobID)
    testJobID = self.DMRequest.getJobID()
    self.assertEqual(jobID,testJobID)

  def test_setgetOwnerDN(self):
    dn = '/C=UK/O=eScience/OU=Edinburgh/L=NeSC/CN=andrew cameron smith'
    self.DMRequest.setOwnerDN(dn)
    testDn = self.DMRequest.getOwnerDN()
    self.assertEqual(dn,testDn)

  def test_setgetMode(self):
    mode = 'test'
    self.DMRequest.setMode(mode)
    testMode = self.DMRequest.getMode()
    self.assertEqual(mode,testMode)

  def test_setgetDiracInstance(self):
    instance = 'testInstance'
    self.DMRequest.setDiracInstance(instance)
    testInstance = self.DMRequest.getDiracInstance()
    self.assertEqual(instance,testInstance)

  def test_getNumberOfOperations(self):
    transfers = self.DMRequest.getNumSubRequests('transfer')
    self.assertEqual(0,transfers['Value'])
    registers = self.DMRequest.getNumSubRequests('register')
    self.assertEqual(0,registers['Value'])
    removals = self.DMRequest.getNumSubRequests('removal')
    self.assertEqual(0,removals['Value'])
    stages = self.DMRequest.getNumSubRequests('stage')
    self.assertEqual(0,stages['Value'])

  def test_isEmpty(self):
    result = self.DMRequest.isEmpty()
    self.assert_(result)

class AddOperationsTestCase(DMRequestTestCase):

  def test_addTransfer(self):
    # Set up dummy request
    lfn = '/lhcb/production/test/case.lfn'
    reqDic = {'Files':{lfn:{'Status': 'Waiting', 'Attempt': 1, 'PFN': '', 'Size': 1231231, 'GUID': '7E9CED5A-295B-ED88-CE9A-CF41A62D2175', 'Addler': '', 'Md5': ''}},'Datasets':['DC06Stripping'],'TargetSE':'CERN-tape','Operation':'MoveAndRegister','SourceSE':'RAL-tape'}
    # Add this to transfer type list
    self.DMRequest.addSubRequest(reqDic,'transfer')
    # Only added one transfer so this should be 1
    transfers = self.DMRequest.getNumSubRequests('transfer')
    self.assertEqual(1,transfers['Value'])
    ind = 0
    # Get the only transfer operation in the request
    result = self.DMRequest.getSubRequest(ind,'transfer')
    testReqDic = result['Value']
    # Make sure it is a dictionary
    self.assertEqual(type(testReqDic),types.DictType)
    # Make sure that the status is waiting
    self.assertEqual(testReqDic['Status'],'Waiting')
    # Check that the request is not empty
    result = self.DMRequest.isEmpty()
    self.assertFalse(result['Value'])
    # Check that all the keys/value pairs we put in are the ones we get back
    for key in testReqDic:
      if reqDic.has_key(key):
        self.assertEqual(reqDic[key],testReqDic[key])

    # Set the status = 'Done'
    self.DMRequest.setSubRequestStatus(ind,'transfer','Done')
    result = self.DMRequest.getSubRequest(ind,'transfer')
    testReqDic = result['Value']
    # Check that it was set to done.
    self.assertEqual(testReqDic['Status'],'Done')
    # Check again that it is empty (which it now should be)
    result = self.DMRequest.isEmpty()
    self.assertFalse(result['Value'])
    #Now set the file status to done
    self.DMRequest.setSubRequestFileStatus(ind,'transfer',lfn,'Done')
    result = self.DMRequest.isEmpty()
    self.assertTrue(result['Value'])

  def test_addRegister(self):
    # Set up dummy request
    lfn = '/lhcb/production/test/case.lfn'
    reqDic = {'Files':{lfn:{'Status': 'Waiting', 'Attempt': 1, 'PFN': 'srm://srm.cern.ch/castor/cern.ch/grid/lhcb/production/test/case.lfn', 'Size': 1231231, 'GUID': '7E9CED5A-295B-ED88-CE9A-CF41A62D2175', 'Addler': 'addler32', 'Md5': 'md5'}},'Datasets':['DC06Stripping'],'TargetSE':'CERN-tape','Operation':'RegisterFile'}
    # Add this to transfer type list
    self.DMRequest.addSubRequest(reqDic,'register')
    # Only added one transfer so this should be 1
    transfers = self.DMRequest.getNumSubRequests('register')
    self.assertEqual(1,transfers['Value'])
    ind = 0
    # Get the only transfer operation in the request
    result = self.DMRequest.getSubRequest(ind,'register')
    testReqDic =  result['Value']
    # Make sure it is a dictionary
    self.assertEqual(type(testReqDic),types.DictType)
    # Make sure that the status is waiting
    self.assertEqual(testReqDic['Status'],'Waiting')
    # Check that the request is not empty
    result = self.DMRequest.isEmpty()
    self.assertFalse(result['Value'])
    # Check that all the keys/value pairs we put in are the ones we get back
    for key in testReqDic:
      if reqDic.has_key(key):
        self.assertEqual(reqDic[key],testReqDic[key])

    # Set the status = 'Done'
    self.DMRequest.setSubRequestStatus(ind,'register','Done')
    result = self.DMRequest.getSubRequest(ind,'register')
    testReqDic = result['Value']
    # Check that it was set to done.
    self.assertEqual(testReqDic['Status'],'Done')
    # Check again that it is empty (which it now should be)
    result = self.DMRequest.isEmpty()
    self.assertFalse(result['Value'])
    #Now set the file status to done
    self.DMRequest.setSubRequestFileStatus(ind,'register',lfn,'Done')
    result = self.DMRequest.isEmpty()
    self.assertTrue(result['Value'])

  def test_addRemoval(self):
    # Set up dummy request
    lfn = '/lhcb/production/test/case.lfn'
    reqDic = {'Files':{lfn:{'Status': 'Waiting', 'Attempt': 1, 'PFN': 'srm://srm.cern.ch/castor/cern.ch/grid/lhcb/production/test/case.lfn', 'Size': 1231231, 'GUID': '7E9CED5A-295B-ED88-CE9A-CF41A62D2175', 'Addler': 'addler32', 'Md5': 'md5'}},'Datasets':['DC06Stripping'],'TargetSE':'CERN-tape','Operation':'RemoveReplica','Catalogue':'LFC'}
    # Add this to transfer type list
    self.DMRequest.addSubRequest(reqDic,'removal')
    # Only added one transfer so this should be 1
    result = self.DMRequest.getNumSubRequests('removal')
    self.assertEqual(1,result['Value'])
    ind = 0
    # Get the only transfer operation in the request
    result = self.DMRequest.getSubRequest(ind,'removal')
    testReqDic = result['Value']
    # Make sure it is a dictionary
    self.assertEqual(type(testReqDic),types.DictType)
    # Make sure that the status is waiting
    self.assertEqual(testReqDic['Status'],'Waiting')
    # Check that the request is not empty
    result = self.DMRequest.isEmpty()
    self.assertFalse(result['Value'])
    # Check that all the keys/value pairs we put in are the ones we get back
    for key in testReqDic:
      if reqDic.has_key(key):
        self.assertEqual(reqDic[key],testReqDic[key])

    # Set the status = 'Done'
    self.DMRequest.setSubRequestStatus(ind,'removal','Done')
    result = self.DMRequest.getSubRequest(ind,'removal')
    testReqDic = result['Value']
    # Check that it was set to done.
    self.assertEqual(testReqDic['Status'],'Done')
    # Check again that it is empty (which it now should be)
    result = self.DMRequest.isEmpty()
    self.assertFalse(result['Value'])
    #Now set the file status to done
    self.DMRequest.setSubRequestFileStatus(ind,'removal',lfn,'Done')
    result = self.DMRequest.isEmpty()
    self.assertTrue(result['Value'])

  def test_addStage(self):
    # Set up dummy request
    lfn = '/lhcb/production/test/case.lfn'
    reqDic = {'Files':{lfn:{'Status': 'Waiting', 'Attempt': 1, 'PFN': 'srm://srm.cern.ch/castor/cern.ch/grid/lhcb/production/test/case.lfn', 'Size': 1231231, 'GUID': '7E9CED5A-295B-ED88-CE9A-CF41A62D2175', 'Addler': 'addler32', 'Md5': 'md5'}},'Datasets':['DC06Stripping'],'TargetSE':'CERN-tape','Operation':'StageAndPin'}
    # Add this to transfer type list
    self.DMRequest.addSubRequest(reqDic,'stage')
    # Only added one transfer so this should be 1
    result = self.DMRequest.getNumSubRequests('stage')
    self.assertEqual(1,result['Value'])
    ind = 0
    # Get the only transfer operation in the request
    result = self.DMRequest.getSubRequest(ind,'stage')
    testReqDic = result['Value']
    # Make sure it is a dictionary
    self.assertEqual(type(testReqDic),types.DictType)
    # Make sure that the status is waiting
    self.assertEqual(testReqDic['Status'],'Waiting')
    # Check that the request is not empty
    result = self.DMRequest.isEmpty()
    self.assertFalse(result['Value'])
    # Check that all the keys/value pairs we put in are the ones we get back
    for key in testReqDic:
      if reqDic.has_key(key):
        self.assertEqual(reqDic[key],testReqDic[key])

    # Set the status = 'Done'
    self.DMRequest.setSubRequestStatus(ind,'stage','Done')
    result = self.DMRequest.getSubRequest(ind,'stage')
    testReqDic = result['Value']
    # Check that it was set to done.
    self.assertEqual(testReqDic['Status'],'Done')
    # Check again that it is empty (which it now should be)
    result = self.DMRequest.isEmpty()
    self.assertFalse(result['Value'])
    #Now set the file status to done
    self.DMRequest.setSubRequestFileStatus(ind,'stage',lfn,'Done')
    result = self.DMRequest.isEmpty()
    self.assertTrue(result['Value'])

  def test_toFile(self):
    lfn = '/lhcb/production/test/case.lfn'
    # Add dummy transfer request
    transferDic = {'Status': 'Waiting','RequestID': '7F7C1D94-E452-CD50-204C-EE2E2F1816A9','Catalogue':'','Files':{lfn:{'Status': 'Waiting', 'Attempt': 1, 'PFN': '', 'Size': 1231231, 'GUID': '7E9CED5A-295B-ED88-CE9A-CF41A62D2175', 'Addler': '', 'Md5': ''}},'Datasets':['DC06Stripping'],'TargetSE':'CERN-tape','Operation':'MoveAndRegister','SourceSE':'RAL-tape'}
    self.DMRequest.addSubRequest(transferDic,'transfer')
    # Add dummy register request
    registerDic = {'Status': 'Waiting','RequestID': '7F7C1D94-E452-CD50-204C-EE2E2F1816A9','Catalogue':'','Files':{lfn:{'Status': 'Waiting', 'Attempt': 1, 'PFN': 'srm://srm.cern.ch/castor/cern.ch/grid/lhcb/production/test/case.lfn', 'Size': 1231231, 'GUID': '7E9CED5A-295B-ED88-CE9A-CF41A62D2175', 'Addler': 'addler32', 'Md5': 'md5'}},'Datasets':['DC06Stripping'],'TargetSE':'CERN-tape','Operation':'RegisterFile'}
    self.DMRequest.addSubRequest(registerDic,'register')
    # Add dummy removal request
    removalDic = {'Status': 'Waiting','RequestID': '7F7C1D94-E452-CD50-204C-EE2E2F1816A9','Catalogue':'','Files':{lfn:{'Status': 'Waiting', 'Attempt': 1, 'PFN': 'srm://srm.cern.ch/castor/cern.ch/grid/lhcb/production/test/case.lfn', 'Size': 1231231, 'GUID': '7E9CED5A-295B-ED88-CE9A-CF41A62D2175', 'Addler': 'addler32', 'Md5': 'md5'}},'Datasets':['DC06Stripping'],'TargetSE':'CERN-tape','Operation':'RemoveReplica','Catalogue':'LFC'}
    self.DMRequest.addSubRequest(removalDic,'removal')
    # Add dummy stage request
    stageDic = {'Status': 'Waiting','RequestID': '7F7C1D94-E452-CD50-204C-EE2E2F1816A9','Catalogue':'','Files':{lfn:{'Status': 'Waiting', 'Attempt': 1, 'PFN': 'srm://srm.cern.ch/castor/cern.ch/grid/lhcb/production/test/case.lfn', 'Size': 1231231, 'GUID': '7E9CED5A-295B-ED88-CE9A-CF41A62D2175', 'Addler': 'addler32', 'Md5': 'md5'}},'Datasets':['DC06Stripping'],'TargetSE':'CERN-tape','Operation':'StageAndPin'}
    self.DMRequest.addSubRequest(stageDic,'stage')
    # Get the XML string of the DM request
    result = self.DMRequest.toXML()
    string = result['Value']
    fname = 'testRequest.xml'
    # Write the DMRequest to a file
    self.DMRequest.toFile(fname)
    # Get the file contents
    reqfile = open(fname,'r')
    testString = reqfile.read()
    reqfile.close()
    # Check the file contents are what is expected
    self.assertEqual(string,testString)

    testReq = DataManagementRequest(string)
    # Test that what is obtained when parsing the request is the same as what is given.
    result = self.DMRequest.getSubRequest(0,'transfer')
    transferReqDouble = result['Value']
    for key in transferReqDouble.keys():
      if key == 'Files':
        for att in transferReqDouble['Files'].keys():
          self.assertEqual(transferDic['Files'][att],transferReqDouble['Files'][att])
      else:
        self.assertEqual(transferDic[key],transferReqDouble[key])

    result = self.DMRequest.getSubRequest(0,'register')
    registerReqDouble = result['Value']
    for key in registerDic.keys():
      if key == 'Files':
        for att in registerDic['Files'].keys():
          self.assertEqual(registerDic['Files'][att],registerReqDouble['Files'][att])
      else:
        self.assertEqual(registerDic[key],registerReqDouble[key])

    result = self.DMRequest.getSubRequest(0,'removal')
    removalReqDouble = result['Value']
    for key in removalDic.keys():
      if key == 'Files':
        for att in removalDic['Files'].keys():
          self.assertEqual(removalDic['Files'][att],removalReqDouble['Files'][att])
      else:
        self.assertEqual(removalDic[key],removalReqDouble[key])

    result = self.DMRequest.getSubRequest(0,'stage')
    stageReqDouble =  result['Value']
    for key in stageDic.keys():
      if key == 'Files':
        for att in stageDic['Files'].keys():
          self.assertEqual(stageDic['Files'][att],stageReqDouble['Files'][att])
      else:
        self.assertEqual(removalDic[key],removalReqDouble[key])

if __name__ == '__main__':

  suite = unittest.defaultTestLoader.loadTestsFromTestCase(GetSetTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(AddOperationsTestCase))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)


