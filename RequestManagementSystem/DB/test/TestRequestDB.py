import unittest,types,time
from DIRAC.RequestManagementSystem.DB.RequestDB import RequestDB
from DIRAC.RequestManagementSystem.Client.DataManagementRequest import DataManagementRequest

class RequestDBTestCase(unittest.TestCase):
  """ Base class for the JobDB test cases
  """

  def setUp(self):
    self.requestDB = RequestDB('mysql')
    self.requestName = 'test-%s' % time.strftime('%Y-%m-%d %H:%M:%S')

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
    requestStatus = 'waiting'
    result = self.requestDB.setRequest(requestType,self.requestName,requestStatus,requestString)
    self.assert_(result['OK'])
    result = self.requestDB.getRequest('transfer')
    self.assert_(result['OK'])
    request = DataManagementRequest(request=result['Value']['RequestString'])
    res = request.getNumSubRequests(requestType)
    for ind in range (res['Value']):
      res = request.getSubRequestFiles(ind,requestType)
      files = res['Value']
      for file in files:
        file['Status'] = 'Done'
      res = request.setSubRequestFiles(ind, type, files)
    res = request.toXML()
    requestString = res['Value']
    result = self.requestDB.updateRequest(self.requestName, requestString)
    self.assert_(result['OK'])
    requestStatus = 'done'
    result = self.requestDB.setRequestStatus(requestType,self.requestName,requestStatus)
    #there is some transient error here where it fails to retrieve the requestID
    self.assert_( result['OK'])
    result = self.requestDB.deleteRequest(self.requestName)
    self.assert_(result['OK'])

if __name__ == '__main__':

  suite = unittest.defaultTestLoader.loadTestsFromTestCase(RequestSubmissionCase)
  #suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(RequestRemovalCase))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)

