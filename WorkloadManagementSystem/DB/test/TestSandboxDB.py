import unittest,zlib
from DIRAC.WorkloadManagementSystem.DB.SandboxDB import SandboxDB

class JobDBTestCase(unittest.TestCase):
  """ Base class for the SandboxDB test cases
  """
  
  def setUp(self):
    print
    self.sDB = SandboxDB('Test',20)
    

class SandboxCase(JobDBTestCase):
  """  TestJobDB represents a test suite for the JobDB database front-end
  """  
    
  def test_uploadFile(self):
  
    sandbox = 'out'
    
    #testfile = open('test.jdl','r')
    testfile = open('/home/atsareg/distributive/skype-1.3.0.53-1mdk.i586.rpm','r')
    body  = testfile.read()
    #body = zlib.compress(body)
    testfile.close()

    result = self.sDB.storeSandboxFile(1,sandbox+'putFile1',body,sandbox)
    print result   
    self.assert_( result['OK']) 
    
    result = self.sDB.getSandboxFile(1,sandbox+'putFile1',sandbox)
    self.assert_( result['OK'])    
    
    newbody = result['Value']
    
    self.assertEqual(body,newbody)    
    
    result = self.sDB.getFileNames(1,sandbox)
    self.assert_( result['OK'])      
    print result                
      
if __name__ == '__main__':

  suite = unittest.defaultTestLoader.loadTestsFromTestCase(SandboxCase)
#  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(JobRemovalCase))
  
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
