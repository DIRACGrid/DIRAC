import unittest,types,time
from DIRAC.DataManagementSystem.Client.DataManagementRequest import DataManagementRequest

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
    transfers = self.DMRequest.getNumberOfTransfers()
    self.assertEqual(0,transfers)
    registers = self.DMRequest.getNumberOfRegisters()
    self.assertEqual(0,registers)
    removals = self.DMRequest.getNumberOfRemovals()
    self.assertEqual(0,removals)
    stages = self.DMRequest.getNumberOfStages()
    self.assertEqual(0,stages)

  def test_isEmpty(self):
    result = self.DMRequest.isEmpty()
    self.assert_(result)

class AddOperationsTestCase(DMRequestTestCase):

  def test_addTransfer(self):
    # Set up dummy request
    reqDic = {'LFN':'/lhcb/production/test/case.lfn','TargetSE':'CERN-tape','Operation':'MoveAndRegister','Size':1231231,'SourceSE':'RAL-tape'}
    # Add this to transfer type list
    self.DMRequest.addTransfer(reqDic)
    # Only added one transfer so this should be 1
    transfers = self.DMRequest.getNumberOfTransfers()
    self.assertEqual(1,transfers)
    ind = 0
    # Get the only transfer operation in the request
    testReqDic = self.DMRequest.getTransfer(ind)
    # Make sure it is a dictionary
    self.assertEqual(type(testReqDic),types.DictType)
    # Make sure that the status is waiting
    self.assertEqual(testReqDic['Status'],'Waiting')
    # Check that the request is not empty
    result = self.DMRequest.isEmpty()
    self.assertFalse(result)
    # Set the status = 'Done'
    self.DMRequest.setTransferDone(ind)
    testReqDic = self.DMRequest.getTransfer(ind)
    # Check that it was set to done.
    self.assertEqual(testReqDic['Status'],'Done')
    # Check again that it is empty (which it now should be)
    result = self.DMRequest.isEmpty()
    self.assertTrue(result)
    # Check that all the keys/value pairs we put in are the ones we get back
    for key in testReqDic:
      if reqDic.has_key(key):
        self.assertEqual(reqDic[key],testReqDic[key])

  def test_addRegister(self):
    # Set up dummy request
    reqDic = {'LFN':'/lhcb/production/test/case.lfn','TargetSE':'CERN-tape','Operation':'RegisterFile','Size':1231231,'PFN':'srm://srm.cern.ch/castor/cern.ch/grid/lhcb/production/test/case.lfn','Addler':'addler32'}
    # Add this to register type list
    self.DMRequest.addRegister(reqDic)
    # Only added one register so this should be 1
    registers = self.DMRequest.getNumberOfRegisters()
    self.assertEqual(1,registers)
    ind = 0
    # Get the only register operation in the request
    testReqDic = self.DMRequest.getRegister(ind)
    # Make sure it is a dictionary
    self.assertEqual(type(testReqDic),types.DictType)
    # Make sure that the status is waiting
    self.assertEqual(testReqDic['Status'],'Waiting')
    # Check that the request is not empty
    result = self.DMRequest.isEmpty()
    self.assertFalse(result)
    # Set the status = 'Done'
    self.DMRequest.setRegisterDone(ind)
    testReqDic = self.DMRequest.getRegister(ind)
    # Check that it was set to done.
    self.assertEqual(testReqDic['Status'],'Done')
    # Check again that it is empty (which it now should be)
    result = self.DMRequest.isEmpty()
    self.assertTrue(result)
    # Check that all the keys/value pairs we put in are the ones we get back
    for key in testReqDic:
      if reqDic.has_key(key):
        self.assertEqual(reqDic[key],testReqDic[key])

  def test_addRemoval(self):
    # Set up dummy request
    reqDic = {'LFN':'/lhcb/production/test/case.lfn','TargetSE':'CERN-tape','Operation':'RemoveReplica','PFN':'srm://srm.cern.ch/castor/cern.ch/grid/lhcb/production/test/case.lfn','Catalog':'LFC'}
    # Add this to removal type list
    self.DMRequest.addRemoval(reqDic)
    # Only added one removal so this should be 1
    removals = self.DMRequest.getNumberOfRemovals()
    self.assertEqual(1,removals)
    ind = 0
    # Get the only removal operation in the request
    testReqDic = self.DMRequest.getRemoval(ind)
    # Make sure it is a dictionary
    self.assertEqual(type(testReqDic),types.DictType)
    # Make sure that the status is waiting
    self.assertEqual(testReqDic['Status'],'Waiting')
    # Check that the request is not empty
    result = self.DMRequest.isEmpty()
    self.assertFalse(result)
    # Set the status = 'Done'
    self.DMRequest.setRemovalDone(ind)
    testReqDic = self.DMRequest.getRemoval(ind)
    # Check that it was set to done.
    self.assertEqual(testReqDic['Status'],'Done')
    # Check again that it is empty (which it now should be)
    result = self.DMRequest.isEmpty()
    self.assertTrue(result)
    # Check that all the keys/value pairs we put in are the ones we get back
    for key in testReqDic:
      if reqDic.has_key(key):
        self.assertEqual(reqDic[key],testReqDic[key])

  def test_addStage(self):
    # Set up dummy request
    reqDic = {'LFN':'/lhcb/production/test/case.lfn','TargetSE':'CERN-tape','Operation':'Stage'}
    # Add this to stage type list
    self.DMRequest.addStage(reqDic)
    # Only added one stage so this should be 1
    stages = self.DMRequest.getNumberOfStages()
    self.assertEqual(1,stages)
    ind = 0
    # Get the only stage operation in the request
    testReqDic = self.DMRequest.getStage(ind)
    # Make sure it is a dictionary
    self.assertEqual(type(testReqDic),types.DictType)
    # Make sure that the status is waiting
    self.assertEqual(testReqDic['Status'],'Waiting')
    # Check that the request is not empty
    result = self.DMRequest.isEmpty()
    self.assertFalse(result)
    # Set the status = 'Done'
    self.DMRequest.setStageDone(ind)
    testReqDic = self.DMRequest.getStage(ind)
    # Check that it was set to done.
    self.assertEqual(testReqDic['Status'],'Done')
    # Check again that it is empty (which it now should be)
    result = self.DMRequest.isEmpty()
    self.assertTrue(result)
    # Check that all the keys/value pairs we put in are the ones we get back
    for key in testReqDic:
      if reqDic.has_key(key):
        self.assertEqual(reqDic[key],testReqDic[key])

  def test_toFile(self):
    # Add dummy transfer request
    transferDic = {'LFN':'/lhcb/production/test/case.lfn','TargetSE':'CERN-tape','Operation':'MoveAndRegister','Size':1231231,'SourceSE':'RAL-tape'}
    self.DMRequest.addTransfer(transferDic)
    # Add dummy register request
    registerDic = {'LFN':'/lhcb/production/test/case.lfn','TargetSE':'CERN-tape','Operation':'RegisterFile','Size':1231231,'PFN':'srm://srm.cern.ch/castor/cern.ch/grid/lhcb/production/test/case.lfn','Addler':'addler32'}
    self.DMRequest.addRegister(registerDic)
    # Add dummy removal request
    removalDic = {'LFN':'/lhcb/production/test/case.lfn','TargetSE':'CERN-tape','Operation':'RemoveReplica','PFN':'srm://srm.cern.ch/castor/cern.ch/grid/lhcb/production/test/case.lfn','Catalog':'LFC'}
    self.DMRequest.addRemoval(removalDic)
    # Add dummy stage request
    stageDic = {'LFN':'/lhcb/production/test/case.lfn','TargetSE':'CERN-tape','Operation':'Stage'}
    self.DMRequest.addStage(stageDic)
    # Get the XML string of the DM request
    string = self.DMRequest.toXML()
    fname = 'testRequest.xml'
    # Write the DMRequest to a file
    self.DMRequest.toFile(fname)
    # Get the file contents
    reqfile = open(fname,'r')
    testString = reqfile.read()
    reqfile.close()
    # Check the file contents are what is expected
    self.assertEqual(string,testString)

if __name__ == '__main__':

  suite = unittest.defaultTestLoader.loadTestsFromTestCase(GetSetTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(AddOperationsTestCase))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)


