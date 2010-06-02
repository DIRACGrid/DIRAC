import unittest
import sys
from DIRAC.ResourceStatusSystem.Utilities.mock import Mock
import DIRAC.ResourceStatusSystem.test.fake_rsDB
#from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Utilities.Utils import *
from DIRAC.ResourceStatusSystem.Policy import Configurations
from DIRAC.ResourceStatusSystem.Utilities.InfoGetter import InfoGetter
from DIRAC.ResourceStatusSystem.Utilities.Publisher import Publisher

class UtilitiesTestCase(unittest.TestCase):
  """ Base class for the Utilities test cases
  """
  def setUp(self):
#    from DIRAC.Core.Base import Script
#    Script.parseCommandLine() 
    sys.modules["DIRAC.ResourceStatusSystem.DB.ResourceStatusDB"] = DIRAC.ResourceStatusSystem.test.fake_rsDB
    
#    from DIRAC.ResourceStatusSystem.test.fake_rsDB import ResourceStatusDB
#    self.rsDB = ResourceStatusDB()
    
#############################################################################

class PublisherSuccess(UtilitiesTestCase):
  
  def test_getInfo(self):

    mockCC = Mock()
    mockIG = Mock()
    mockWMSA = Mock()
    
    mockCC.commandInvocation.return_value = 'INFO_GOT_MOCK'
    mockWMSA.getSiteMaskLogging.return_value = {'OK': True, 
                                                'Value': {'LCG.CERN.ch': [('Active', '2009-11-25 17:36:14', 'atsareg', 'test')]}}
    
    p = Publisher(rsDBIn = None, commandCallerIn = mockCC, infoGetterIn = mockIG, 
                  WMSAdminIn = mockWMSA)
    
    igR = [{'Panels': {'Service_Storage_Panel': 
                        [ {'OnStorageServicePropagation_SE': {'RSS': 'StorageElementsOfSite'}}, 
                          {'OnStorageServicePropagation_Res': {'RSS': 'ResOfStorService'}}], 
                       'OtherServices_Panel': [], 
                       'Site_Panel': [{'GGUSTickets': 
                                        [{'WebLink': {'args': None, 'Command': 'GGUS_Link'}}, 
                                         {'TextInfo': {'args': None, 'Command': 'GGUS_Info'}}]}, 
                                         {'DT_Scheduled': [{'WebLink': {'args': None, 'Command': 'DT_Link'}}]}, 
                                         {'OnSitePropagation': {'RSS': 'ServiceOfSite'}}], 
                                         'Service_Computing_Panel': [{'OnComputingServicePropagation': 
                                                                      {'RSS': 'ResOfCompService'}}, 
                                                                      {'JobsEfficiencySimple': [{'FillChart': {'args': ('Job', 'CumulativeNumberOfJobs', {'hours': 24, 'Format': 'LastHours'}, 'FinalMajorStatus', None), 'Command': 'DiracAccountingGraph'}}, 
                                                                                                {'PieChart': {'args': ('Job', 'TotalNumberOfJobs', {'hours': 24, 'Format': 'LastHours'}, 'JobType', {'FinalMajorStatus': 'Failed'}), 'Command': 'DiracAccountingGraph'}}]}, 
                       {'PilotsEfficiencySimple_Service': [{'FillChart': {'args': ('Pilot', 'CumulativeNumberOfPilots', {'hours': 24, 'Format': 'LastHours'}, 'GridStatus', None), 'Command': 'DiracAccountingGraph'}}, 
                                                           {'PieChart': {'args': ('Pilot', 'TotalNumberOfPilots', {'hours': 24, 'Format': 'LastHours'}, 'GridCE', None), 'Command': 'DiracAccountingGraph'}}]}]}}]

    mockIG.getInfoToApply.return_value = igR 
    
    res = p.getInfo('Site', 'LCG.CERN.ch')

    for panel in igR[0]['Panels'].keys():
      self.assert_(panel in res.keys())
      for i in range(len(igR[0]['Panels'][panel])):
        for policy in igR[0]['Panels'][panel][i].keys():
          self.assert_(policy in res[panel]['InfoForPanel'].keys())

    for panel_name in res.keys():
      self.assert_(panel_name in igR[0]['Panels'].keys())
      for policy in res[panel_name]['InfoForPanel'].keys():
        pNames = []
        for i in range(len(igR[0]['Panels'][panel_name])):
          pNames = pNames + igR[0]['Panels'][panel_name][i].keys()
        self.assert_(policy in pNames)
        self.assert_('Status' in res[panel_name]['InfoForPanel'][policy].keys())
        self.assert_('Reason' in res[panel_name]['InfoForPanel'][policy].keys())
        self.assert_('infos' in res[panel_name]['InfoForPanel'][policy].keys())
        self.assert_('desc' in res[panel_name]['InfoForPanel'][policy].keys())


    igR = [{'Panels':  {'Resource_Panel': [
                               {'DT_Scheduled': [{'WebLink': {'args': None, 'Command': 'DT_Link'}}]}, 
                               {'SAM_CE': [{'SAM': {'args': (None, ['LHCb CE-lhcb-availability', 'LHCb CE-lhcb-install', 'LHCb CE-lhcb-job-Boole', 'LHCb CE-lhcb-job-Brunel', 'LHCb CE-lhcb-job-DaVinci', 'LHCb CE-lhcb-job-Gauss', 'LHCb CE-lhcb-os', 'LHCb CE-lhcb-queues', 'LHCb CE-lhcb-queues', 'bi', 'csh', 'js', 'gfal', 'swdir', 'voms']), 
                                                    'Command': 'SAM_Tests'}}]}, 
                               {'PilotsEfficiencySimple_Resource': [{'FillChart': {'args': ('Pilot', 'CumulativeNumberOfPilots', {'hours': 24, 'Format': 'LastHours'}, 'GridStatus', None), 
                                                                                  'Command': 'DiracAccountingGraph'}}]}, 
                       ]}}]
    
    mockIG.getInfoToApply.return_value = igR
    
    res = p.getInfo('Resource', 'grid0.fe.infn.it')

    for panel in igR[0]['Panels'].keys():
      self.assert_(panel in res.keys())
      for i in range(len(igR[0]['Panels'][panel])):
        for policy in igR[0]['Panels'][panel][i].keys():
          self.assert_(policy in res[panel]['InfoForPanel'].keys())

    for panel_name in res.keys():
      self.assert_(panel_name in igR[0]['Panels'].keys())
      for policy in res[panel_name]['InfoForPanel'].keys():
        pNames = []
        for i in range(len(igR[0]['Panels'][panel_name])):
          pNames = pNames + igR[0]['Panels'][panel_name][i].keys()
        self.assert_(policy in pNames)
        self.assert_('Status' in res[panel_name]['InfoForPanel'][policy].keys())
        self.assert_('Reason' in res[panel_name]['InfoForPanel'][policy].keys())
        self.assert_('infos' in res[panel_name]['InfoForPanel'][policy].keys())
        self.assert_('desc' in res[panel_name]['InfoForPanel'][policy].keys())


    igR = [{'Panels':  {'Resource_Panel': [
                               {'SAM_LFC_L': [{'SAM': {'args': (None, ['lfcstreams', 'lfclr', 'lfcls', 'lfcping']), 
                                                       'Command': 'SAM_Tests'}}]}, 
                               {'DT_Scheduled': [{'WebLink': {'args': None, 'Command': 'DT_Link'}}]}, 
                       ]}}]
    
    mockIG.getInfoToApply.return_value = igR
    
    res = p.getInfo('Resource', 'prod-lfc-lhcb-ro.cern.ch')

    for panel in igR[0]['Panels'].keys():
      self.assert_(panel in res.keys())
      for i in range(len(igR[0]['Panels'][panel])):
        for policy in igR[0]['Panels'][panel][i].keys():
          self.assert_(policy in res[panel]['InfoForPanel'].keys())

    for panel_name in res.keys():
      self.assert_(panel_name in igR[0]['Panels'].keys())
      for policy in res[panel_name]['InfoForPanel'].keys():
        pNames = []
        for i in range(len(igR[0]['Panels'][panel_name])):
          pNames = pNames + igR[0]['Panels'][panel_name][i].keys()
        self.assert_(policy in pNames)
        self.assert_('Status' in res[panel_name]['InfoForPanel'][policy].keys())
        self.assert_('Reason' in res[panel_name]['InfoForPanel'][policy].keys())
        self.assert_('infos' in res[panel_name]['InfoForPanel'][policy].keys())
        self.assert_('desc' in res[panel_name]['InfoForPanel'][policy].keys())


    igR = [{'Panels': {'SE_Panel': [
                          {'OnStorageElementPropagation': {'RSS': 'ResOfStorEl'}}, 
                          {'TransferQuality': [{'FillChart': {'args': ('DataOperation', 'Quality', {'hours': 24, 'Format': 'LastHours'}, 'Channel', {'OperationType': 'putAndRegister'}), 
                                                              'Command': 'DiracAccountingGraph'}}]}, 
                          {'SEOccupancy': [{'WebLink': {'args': None, 'Command': 'SLS_Link'}}]}, 
                          {'SEQueuedTransfers': [{'WebLink': {'args': None, 'Command': 'SLS_Link'}}]}]}}]
    
    mockIG.getInfoToApply.return_value = igR
    
    res = p.getInfo('StorageElement', 'CERN-RAW')

    for panel in igR[0]['Panels'].keys():
      self.assert_(panel in res.keys())
      for i in range(len(igR[0]['Panels'][panel])):
        for policy in igR[0]['Panels'][panel][i].keys():
          self.assert_(policy in res[panel]['InfoForPanel'].keys())

    for panel_name in res.keys():
      self.assert_(panel_name in igR[0]['Panels'].keys())
      for policy in res[panel_name]['InfoForPanel'].keys():
        pNames = []
        for i in range(len(igR[0]['Panels'][panel_name])):
          pNames = pNames + igR[0]['Panels'][panel_name][i].keys()
        self.assert_(policy in pNames)
        self.assert_('Status' in res[panel_name]['InfoForPanel'][policy].keys())
        self.assert_('Reason' in res[panel_name]['InfoForPanel'][policy].keys())
        self.assert_('infos' in res[panel_name]['InfoForPanel'][policy].keys())
        self.assert_('desc' in res[panel_name]['InfoForPanel'][policy].keys())


#############################################################################

class InfoGetterSuccess(UtilitiesTestCase):
  
  def testGetInfoToApply(self):
    ig = InfoGetter()
    
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

              res = ig.getInfoToApply(('policyType', ), g, s, None, site_t, service_t, resource_t)
              for p_res in res[0]['PolicyType']:
                self.assert_(p_res in Configurations.Policy_Types.keys())

              for useNewRes in (True, False):

                res = ig.getInfoToApply(('policy', ), g, s, None, site_t, service_t, resource_t, useNewRes)
                pModuleList = [None]
                for k in Configurations.Policies.keys():
                  try:
                    if Configurations.Policies[k]['module'] not in pModuleList:
                      pModuleList.append(Configurations.Policies[k]['module'])
                  except KeyError:
                    pass
                for p_res in res[0]['Policies']:
                  self.assert_(p_res['Name'] in Configurations.Policies.keys())
                  self.assert_(p_res['Module'] in pModuleList)
                  if useNewRes is False:
                    self.assertEqual(p_res['commandIn'], Configurations.Policies[p_res['Name']]['commandIn'])
                  else:
                    try:
                      self.assertEqual(p_res['commandIn'], Configurations.Policies[p_res['Name']]['commandInNewRes'])
                    except KeyError:
                      self.assertEqual(p_res['commandIn'], Configurations.Policies[p_res['Name']]['commandIn'])

                res = ig.getInfoToApply(('panel_info', ), g, s, None, site_t, service_t, resource_t, useNewRes)
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
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(PublisherSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(InfoGetterSuccess))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)

