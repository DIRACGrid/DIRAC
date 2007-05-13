import unittest,types,time,datetime
from DIRAC.WorkloadManagementSystem.DB.JobLoggingDB import JobLoggingDB

class JobDBTestCase(unittest.TestCase):
  """ Base class for the JobDB test cases
  """
  
  def setUp(self):
    print
    self.jlogDB = JobLoggingDB('Test',20)
    

class JobLoggingCase(JobDBTestCase):
  """  TestJobDB represents a test suite for the JobDB database front-end
  """  
    
  def test_JobStatus(self):
  
    result = self.jlogDB.addLoggingRecord(1,status="testing",
                                          minor='date=datetime.datetime.utcnow()',
                                          date=datetime.datetime.utcnow(),
                                          source='Unittest')
    self.assert_( result['OK']) 
    date = '2006-04-25 14:20:17'
    result = self.jlogDB.addLoggingRecord(1,status="testing",
                                          minor='2006-04-25 14:20:17',
                                          date=date,
                                          source='Unittest')
    self.assert_( result['OK']) 
    result = self.jlogDB.addLoggingRecord(1,status="testing",
                                          minor='No date 1',
                                          source='Unittest')
    self.assert_( result['OK'])  
    result = self.jlogDB.addLoggingRecord(1,status="testing",
                                          minor='No date 2',
                                          source='Unittest')
    self.assert_( result['OK'])  
    result = self.jlogDB.getJobLoggingInfo(1)
    self.assert_( result['OK'])  
    #for row in result['Value']:
    #  print row  
      
    result = self.jlogDB.getWMSTimeStamps(1)
    self.assert_( result['OK'])        
    #print result['Value']                              
                              
      
if __name__ == '__main__':

  suite = unittest.defaultTestLoader.loadTestsFromTestCase(JobLoggingCase)
#  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(JobRemovalCase))
  
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
