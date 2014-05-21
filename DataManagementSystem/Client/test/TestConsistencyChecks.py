import unittest
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager

class ConsistencyChecksTestCase(unittest.TestCase):
  """ Base class for the Consistency Checks test cases
  """
  def setUp( self ):
    
    
  def tearDown( self ):
    pass
  

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(ConsistencyChecksTestCase)
  #suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(DirectoryTestCase))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
