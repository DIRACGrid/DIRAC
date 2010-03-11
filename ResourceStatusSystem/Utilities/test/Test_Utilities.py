import unittest
import sys
from DIRAC.ResourceStatusSystem.Utilities.mock import Mock
import DIRAC.ResourceStatusSystem.test.fake_rsDB
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Utilities.Utils import *
from DIRAC.ResourceStatusSystem.Policy import Configurations
from DIRAC.ResourceStatusSystem.Utilities.InfoGetter import InfoGetter

class UtilitiesTestCase(unittest.TestCase):
  """ Base class for the Utilities test cases
  """
  def setUp(self):
    from DIRAC.Core.Base import Script
    Script.parseCommandLine() 
    #sys.modules["DIRAC.ResourceStatusSystem.DB.ResourceStatusDB"] = DIRAC.ResourceStatusSystem.test.fake_rsDB
    from DIRAC.ResourceStatusSystem.Utilities.Publisher import Publisher
    
    from DIRAC.ResourceStatusSystem.test.fake_rsDB import ResourceStatusDB
    self.rsDB = ResourceStatusDB()
    
    mockCC = Mock()
    
    self.p = Publisher(self.rsDB, mockCC)
    
    self.ig = InfoGetter()
    
    #self.mock_command = Mock()

#############################################################################
            
class PublisherSuccess(UtilitiesTestCase):
  
  def test_getInfo(self):
    comb = ( ('Site', 'LCG.CERN.ch', 'Site_View'), 
             ('Resource', 'grid0.fe.infn.it', 'Resource_View'), 
             ('StorageElement', 'CERN-RAW', 'SE_View') )
    for (g, n, v) in comb: 
      res = self.p.getInfo(g, n, v)
      info = self.ig.getInfoToApply(('view_info', ), None, None, None, None, None, None, v)

      l = []
      i = 0
      
      for k in info[0]['Panels'].keys():
        l.append((k, res[i]))
        i = i + 1
      
      for (panel, res) in l:
        self.assert_(res.has_key(panel))
      


#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(UtilitiesTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(PublisherSuccess))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)

