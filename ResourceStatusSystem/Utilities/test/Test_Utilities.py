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
    
#############################################################################

#rifalla con un un mock al posto di infoGetter + un test vero!             
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
        k in res.keys()
        i = i + 1
      
      for (panel, res) in l:
        self.assert_(res.has_key(panel))
      
#############################################################################

class InfoGetterSuccess(UtilitiesTestCase):
  
  def testGetInfoToApply(self):
#    for arg in ('policy', 'policyType', 'panel_info', 'view_info'):
    for g in ValidRes:
      for s in ValidStatus: 
        for site_t in ValidSiteType: 
          for service_t in ValidServiceType: 
      
            if g in ('Site', 'Sites'):
              panel = 'Site_Panel'
            if g in ('Service', 'Services'):
              if service_t == 'Storage':
                panel = 'Service_Storage_Panel'
              if service_t == 'Computing':
                panel = 'Service_Computing_Panel'
              if service_t == 'Others':
                panel = 'Service_Others_Panel'
            if g in ('Resource', 'Resources'):
              panel = 'Resource_Panel'
            if g in ('StorageElement', 'StorageElements'):
              panel = 'SE_Panel'

      
            for resource_t in ValidResourceType: 

              res = self.ig.getInfoToApply(('policyType', ), g, s, None, site_t, service_t, resource_t)
              for p_res in res[0]['PolicyType']:
                self.assert_(p_res in Configurations.Policy_Types.keys())

              for useNewRes in (True, False):

                res = self.ig.getInfoToApply(('policy', ), g, s, None, site_t, service_t, resource_t, useNewRes)
                for p_res in res[0]['Policies']:
                  self.assert_(p_res['Name'] in Configurations.Policies.keys())
                  if useNewRes is False:
                    self.assertEqual(p_res['commandIn'], Configurations.Policies[p_res['Name']]['commandIn'])
                  else:
                    try:
                      self.assertEqual(p_res['commandIn'], Configurations.Policies[p_res['Name']]['commandInNewRes'])
                    except KeyError:
                      self.assertEqual(p_res['commandIn'], Configurations.Policies[p_res['Name']]['commandIn'])

                res = self.ig.getInfoToApply(('panel_info', ), g, s, None, site_t, service_t, resource_t, useNewRes)
                for p_res in res[0]['Info']:
                  for p_name in p_res.keys():
                    self.assert_(p_name in Configurations.Policies.keys())
                    if isinstance(p_res[p_name], list):
                      for i in range(len(p_res[p_name])):
                        for k in p_res[p_name][i].keys():
                          self.assertEqual(p_res[p_name][i][k]['args'], 
                                           Configurations.Policies[p_name][panel][i][k]['args'])
                          if useNewRes:
                            try:
                              self.assertEqual(p_res[p_name][i][k]['Command'], 
                                               Configurations.Policies[p_name][panel][i][k]['CommandNew'])
                            except KeyError:
                              self.assertEqual(p_res[p_name][i][k]['Command'], 
                                               Configurations.Policies[p_name][panel][i][k]['Command'])
                          else:
                            self.assertEqual(p_res[p_name][i][k]['Command'], 
                                             Configurations.Policies[p_name][panel][i][k]['Command'])
                            
                    else:
                      self.assertEqual(p_res[p_name], Configurations.Policies[p_name][panel])


#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(UtilitiesTestCase)
#  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(PublisherSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(InfoGetterSuccess))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)

