import unittest,types
from DIRAC.RequestManagementSystem.DB.RequestDB import RequestDB
from DIRAC.RequestManagementSystem.Client.DataManagementRequest import DataManagementRequest

class RequestDBTestCase(unittest.TestCase):
  """ Base class for the JobDB test cases
  """

  def setUp(self):
    self.requestDB = RequestDB('mysql')

  def getTestRequestString(self):
    xmlfile = open("testRequest.xml","r")
    xmlString = xmlfile.read()
    xmlfile.close()
    return xmlString

class RequestSubmissionCase(RequestDBTestCase):
  """  TestRequestDB represents a test suite for the RequestDB database front-end
  """

  def test_setRequest(self):

    requestString = self.getTestRequestString()
    requestType = 'transfer'
    requestName = 'test'
    requestStatus = 'waiting'
    #result = self.requestDB.setRequest(requestType,requestName,requestStatus,requestString)
    #print result

    result = self.requestDB.getRequest('transfer')
    print result

  """
  def test_setRequestStatus(self):

    requestType = 'transfer'
    requestName = 'test'
    requestStatus = 'done'
    result = self.requestDB.setRequestStatus(requestType,requestName,requestStatus)
  """


  """
    self.assert_( result['OK'])
    self.assertEqual(type(result['Value']),types.IntType)
    id1 = result['Value']
    result = self.jobDB.getJobID()
    self.assert_( result['OK'])
    self.assertEqual(type(result['Value']),types.IntType)
    id2 = result['Value']
    self.assertEqual(id2,id1+1)

  def test_insertJobIntoDB(self):

    jobID = self.createJob()
    jdlfile = open("test.jdl","r")
    jdl = jdlfile.read()
    jdlfile.close()
    result = self.jobDB.insertJobIntoDB(jobID,jdl)
    self.assert_( result['OK'],'Status after insertJobIntoDB')
    result = self.jobDB.getJobAttribute(jobID,'Status')
    self.assert_( result['OK'],'Status after getJobAttribute')
    self.assertEqual(result['Value'],'received','Proper received status')

  def test_addJobToDB(self):

    jobID = self.createJob()
    jdlfile = open("test.jdl","r")
    jdl = jdlfile.read()
    jdlfile.close()
    result = self.jobDB.addJobToDB(jobID,jdl)
    self.assert_( result['OK'],'Status after addJobToDB')
    result = self.jobDB.getJobAttribute(jobID,'Status')
    self.assert_( result['OK'],'Status after getJobAttribute')
    self.assertEqual(result['Value'],'received','Proper received status')
    result = self.jobDB.getJobAttribute(jobID,'MinorStatus')
    self.assert_( result['OK'],'Status after getJobAttribute')
    self.assertEqual(result['Value'],'Job accepted','Proper received status')

class JobRemovalCase(JobDBTestCase):

  def test_removeJobFromDB(self):

    for i in range(10):
      jobID = self.createJob()

    result = self.jobDB.selectJobs({})
    self.assert_( result['OK'],'Status after selectJobs')
    jobs = result['Value']
    for job in jobs:
      result = self.jobDB.removeJobFromDB(job)
      self.assert_( result['OK'],'Status after removeJobFromDB')

class JobRescheduleCase(JobDBTestCase):

  def test_rescheduleJob(self):

    jobID = self.createJob()
    result = self.jobDB.rescheduleJob(jobID)
    self.assert_( result['OK'],'Status after rescheduleJob')

    for i in range(10):
      jobID = self.createJob()
      result = self.jobDB.addJobToDB(jobID)

    result = self.jobDB.selectJobs({})
    self.assert_( result['OK'],'Status after selectJobs')
    jobs = result['Value']
    result = self.jobDB.rescheduleJobs(jobs)
    self.assert_( result['OK'],'Status after rescheduleJobs')

class JobParametersCase(JobDBTestCase):

  def test_countJobs(self):

    result = self.jobDB.countJobs({})
    self.assert_( result['OK'],'Status after countJobs')
    njobs = result['Value']
    result = self.jobDB.selectJobs({})
    self.assert_( result['OK'],'Status after selectJobs')
    jobs = result['Value']
    self.assertEqual(njobs,len(jobs),'Equality of number of jobs' )

class SiteMaskCase(JobDBTestCase):

  def test_setMask(self):

    result = self.jobDB.setMask(["DIRAC.in2p3.fr","DIRAC.cern.ch"])
    self.assert_( result['OK'],'Status after setMask')
    result = self.jobDB.getMask()
    self.assert_( result['OK'],'Status after getMask')
    self.assertEqual(result['Value'],
                     '[  Requirements = OtherSite == "DIRAC.in2p3.fr" || Other.Site == "DIRAC.cern.ch"   ]',
                     'Equality of the site mask' )

  def test_allowSiteInMask(self):

    result = self.jobDB.setMask(["DIRAC.in2p3.fr","DIRAC.cern.ch"])
    self.assert_( result['OK'],'Status after setMask')
    result = self.jobDB.banSiteInMask("DIRAC.in2p3.fr")
    self.assert_( result['OK'],'Status after banSiteInMask')
    result = self.jobDB.getMask()
    self.assert_( result['OK'],'Status after getMask')
    self.assertEqual(result['Value'],
                     '[  Requirements = OtherSite == "DIRAC.cern.ch"   ]',
                     'Equality of the site mask' )
    result = self.jobDB.allowSiteInMask("DIRAC.in2p3.fr")
    self.assert_( result['OK'],'Status after allowSiteInMask')
    result = self.jobDB.getMask()
    self.assert_( result['OK'],'Status after getMask')
    self.assertEqual(result['Value'],
                     '[  Requirements = OtherSite == "DIRAC.in2p3.fr" || Other.Site == "DIRAC.cern.ch"   ]',
                     'Equality of the site mask' )

class TaskQueueCase(JobDBTestCase):

  def test_manipulateQueue(self):

    result = self.jobDB.selectQueue('Other.Site == "DIRAC.IN2P3.fr"')
    self.assert_( result['OK'],'Status after selectQueue')
    queueID_1 = result['Value']
    result = self.jobDB.selectQueue('Other.Site == "DIRAC.IN2P3.fr"')
    self.assert_( result['OK'],'Status after selectQueue')
    queueID_2 = result['Value']
    self.assertEqual(queueID_1,queueID_2)

  def test_jobsInQueue(self):

    jobID = self.createJob()
    result = self.jobDB.addJobToDB(jobID)
    self.assert_( result['OK'],'Status after addJobToDB')
    result = self.jobDB.selectQueue('Other.Site == "DIRAC.IN2P3.fr"')
    self.assert_( result['OK'],'Status after selectQueue')
    queueID = result['Value']
    result = self.jobDB.addJobToQueue(jobID,queueID,120)
    self.assert_( result['OK'],'Status after addJobToQueue')
    result = self.jobDB.deleteJobFromQueue(jobID)
    self.assert_( result['OK'],'Status after deleteJobFromQueue')

class CountJobsCase(JobDBTestCase):

  def test_getCounters(self):

    result = self.jobDB.getCounters(['Status','MinorStatus'],{},'2007-04-22 00:00:00')
    self.assert_( result['OK'],'Status after getCounters')

  """
if __name__ == '__main__':

  suite = unittest.defaultTestLoader.loadTestsFromTestCase(RequestSubmissionCase)
  #suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(RequestStateSetCase))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
