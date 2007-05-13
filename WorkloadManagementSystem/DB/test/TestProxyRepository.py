import unittest,types,time,datetime
from DIRAC.WorkloadManagementSystem.DB.ProxyRepositoryDB import ProxyRepositoryDB




class ProxyCase(unittest.TestCase):
  """  TestJobDB represents a test suite for the JobDB database front-end
  """  
  
  def setUp(self):
    self.repository = ProxyRepositoryDB('Test',10)

    
  def test_manipulateProxies(self):
  
    result = self.repository.storeProxy('This is an imitation of a proxy',
                                        'This is a DN',
                                        '/lhcb' )
    self.assert_( result['OK']) 
    result = self.repository.getProxy('This is a DN','/lhcb')
    self.assert_( result['OK']) 
    self.assertEqual('This is an imitation of a proxy',result['Value'])
    result = self.repository.getUsers()
    self.assert_( result['OK']) 
    result = self.repository.removeProxy(userDN='This is a DN')
    self.assert_( result['OK'])                  
                              
      
if __name__ == '__main__':

  suite = unittest.defaultTestLoader.loadTestsFromTestCase(ProxyCase)
#  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(JobRemovalCase))
  
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
