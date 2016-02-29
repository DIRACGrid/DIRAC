""" This test only need the JobLoggingDB to be present
"""

import unittest, datetime

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC.WorkloadManagementSystem.DB.JobLoggingDB import JobLoggingDB

class JobLoggingDBTestCase( unittest.TestCase ):
  """ Base class for the JobLoggingDB test cases
  """

  def setUp(self):
    self.jlogDB = JobLoggingDB()

  def tearDown( self ):
    pass

class JobLoggingCase( JobLoggingDBTestCase ):
  """  TestJobDB represents a test suite for the JobDB database front-end
  """  
    
  def test_JobStatus(self):
  
    result = self.jlogDB.addLoggingRecord( 1, status = "testing",
                                           minor = 'date=datetime.datetime.utcnow()',
                                           date = datetime.datetime.utcnow(),
                                           source = 'Unittest' )
    self.assert_( result['OK'] )
    date = '2006-04-25 14:20:17'
    result = self.jlogDB.addLoggingRecord( 1, status = "testing",
                                           minor = '2006-04-25 14:20:17',
                                           date = date,
                                           source = 'Unittest' )
    self.assert_( result['OK'] )
    result = self.jlogDB.addLoggingRecord( 1, status = "testing",
                                           minor = 'No date 1',
                                           source = 'Unittest' )
    self.assert_( result['OK'] )
    result = self.jlogDB.addLoggingRecord( 1, status = "testing",
                                           minor = 'No date 2',
                                           source = 'Unittest' )
    self.assert_( result['OK'] )
    result = self.jlogDB.getJobLoggingInfo( 1 )
    self.assert_( result['OK'] )

    result = self.jlogDB.getWMSTimeStamps( 1 )
    self.assert_( result['OK'] )

    self.jlogDB.deleteJob( 1 )


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( JobLoggingCase )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
