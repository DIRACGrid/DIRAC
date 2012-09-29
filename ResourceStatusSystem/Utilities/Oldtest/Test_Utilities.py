#import unittest
#
#from DIRAC.Core.Base import Script
#Script.parseCommandLine()
#
#from DIRAC.ResourceStatusSystem.Utilities.Utils import *
#from DIRAC.ResourceStatusSystem import *
#from DIRAC.ResourceStatusSystem.Utilities.InfoGetter import InfoGetter
#
#from DIRAC.ResourceStatusSystem.Client.mock.ResourceStatusClient import ResourceStatusClient
#from DIRAC.ResourceStatusSystem.Client.mock.ResourceManagementClient import ResourceManagementClient
#
#class UtilitiesTestCase(unittest.TestCase):
#  """ Base class for the Utilities test cases
#  """
#  def setUp(self):
#
#    from DIRAC import gConfig
#
##    from DIRAC.ResourceStatusSystem.Utilities.Publisher import Publisher
#    from DIRAC.ResourceStatusSystem.Utilities.Synchronizer import Synchronizer
#
##    from DIRAC.ResourceStatusSystem.test.fake_rsDB import ResourceStatusDB
##    self.rsDB = ResourceStatusDB()
#
#    self.VO = gConfig.getValue("DIRAC/Extensions")
#
#     self.mockCC.commandInvocation.return_value = 'INFO_GOT_MOCK'
#    # self.mockWMSA.getSiteMaskLogging.return_value = {'OK': True,
#    #                                             'Value': {'LCG.CERN.ch': [('Active', '2009-11-25 17:36:14', 'atsareg', 'test')]}}
#
#
#    # self.p = Publisher(self.VO, rsDBIn = None, commandCallerIn = self.mockCC, infoGetterIn = self.mockIG,
#    #                    WMSAdminIn = self.mockWMSA)
#
#    self.configModule = voimport("DIRAC.ResourceStatusSystem.Policy.Configurations", self.VO)
#
#    self.syncC = Synchronizer()
#    self.rsClient = ResourceStatusClient()
#    self.rmClient = ResourceManagementClient()
#
##############################################################################
#
## class PublisherSuccess(UtilitiesTestCase):
#
##   def test_getInfo(self):
#
##     igR = [{'Panels': {'Service_Storage_Panel':
##                         [ {'OnStorageServicePropagation_SE': {'RSS': 'StorageElementsOfSite'}},
##                           {'OnStorageServicePropagation_Res': {'RSS': 'ResOfStorService'}}],
##                        'Service_VOMS_Panel': [],
##                        'Service_VO-BOX_Panel': [],
##                        'Site_Panel': [{'GGUSTickets':
##                                         [{'WebLink': {'args': None, 'CommandIn': 'GGUS_Link'}},
##                                          {'TextInfo': {'args': None, 'CommandIn': 'GGUS_Info'}}]},
##                                          {'DT_Scheduled': [{'WebLink': {'args': None, 'CommandIn': 'DT_Link'}}]},
##                                          {'OnSitePropagation': {'RSS': 'ServiceOfSite'}}],
##                         'Service_Computing_Panel': [{'OnComputingServicePropagation':
##                                                                       {'RSS': 'ResOfCompService'}},
##                                                                       {'JobsEfficiencySimple': [{'FillChart': {'args': ('Job', 'CumulativeNumberOfJobs', {'hours': 24, 'Format': 'LastHours'}, 'FinalMajorStatus', None), 'CommandIn': 'DiracAccountingGraph'}},
##                                                                                                 {'PieChart': {'args': ('Job', 'TotalNumberOfJobs', {'hours': 24, 'Format': 'LastHours'}, 'JobType', {'FinalMajorStatus': 'Failed'}), 'CommandIn': 'DiracAccountingGraph'}}]},
##                                                                        {'PilotsEfficiencySimple_Service': [{'FillChart': {'args': ('Pilot', 'CumulativeNumberOfPilots', {'hours': 24, 'Format': 'LastHours'}, 'GridStatus', None), 'CommandIn': 'DiracAccountingGraph'}},
##                                                                                                            {'PieChart': {'args': ('Pilot', 'TotalNumberOfPilots', {'hours': 24, 'Format': 'LastHours'}, 'GridCE', None), 'CommandIn': 'DiracAccountingGraph'}}]}]}}]
#
##     self.mockIG.getInfoToApply.return_value = igR
#
##     res = self.p.getInfo('Site', 'LCG.CERN.ch')
#
##     for record in res['Records']:
##       self.assert_(record[0] in ('ResultsForResource', 'SpecificInformation'))
##       self.assert_(record[1] in ('Service_Storage', 'Service_VOMS', 'Service_VO-BOX',
##                                  'Site', 'Service_Computing'))
##       self.assert_(record[2] is not None)
##       if record[0] == 'SpecificInformation':
##         self.assert_(record[3] is not None)
##       else:
##         self.assert_(record[3] is None)
##       self.assert_(record[5] in ValidStatus)
#
## #    for panel in igR[0]['Panels'].keys():
## #      self.assert_(panel in res.keys())
## #      for i in range(len(igR[0]['Panels'][panel])):
## #        for policy in igR[0]['Panels'][panel][i].keys():
## #          self.assert_(policy in res[panel]['InfoForPanel'].keys())
## #
## #    for panel_name in res.keys():
## #      self.assert_(panel_name in igR[0]['Panels'].keys())
## #      for policy in res[panel_name]['InfoForPanel'].keys():
## #        pNames = []
## #        for i in range(len(igR[0]['Panels'][panel_name])):
## #          pNames = pNames + igR[0]['Panels'][panel_name][i].keys()
## #        self.assert_(policy in pNames)
## #        self.assert_('Status' in res[panel_name]['InfoForPanel'][policy].keys())
## #        self.assert_('Reason' in res[panel_name]['InfoForPanel'][policy].keys())
## #        self.assert_('infos' in res[panel_name]['InfoForPanel'][policy].keys())
## #        self.assert_('desc' in res[panel_name]['InfoForPanel'][policy].keys())
#
#
##     igR = [{'Panels':  {'Resource_Panel': [
##                                {'DT_Scheduled': [{'WebLink': {'args': None, 'CommandIn': 'DT_Link'}}]},
##                                {'SAM_CE': [{'SAM': {'args': (None, ['LHCb CE-lhcb-availability', 'LHCb CE-lhcb-install', 'LHCb CE-lhcb-job-Boole', 'LHCb CE-lhcb-job-Brunel', 'LHCb CE-lhcb-job-DaVinci', 'LHCb CE-lhcb-job-Gauss', 'LHCb CE-lhcb-os', 'LHCb CE-lhcb-queues', 'LHCb CE-lhcb-queues', 'bi', 'csh', 'js', 'gfal', 'swdir', 'voms']),
##                                                     'CommandIn': 'SAM_Tests'}}]},
##                                {'PilotsEfficiencySimple_Resource': [{'FillChart': {'args': ('Pilot', 'CumulativeNumberOfPilots', {'hours': 24, 'Format': 'LastHours'}, 'GridStatus', None),
##                                                                                   'CommandIn': 'DiracAccountingGraph'}}]},
##                        ]}}]
#
##     self.mockIG.getInfoToApply.return_value = igR
#
##     res = self.p.getInfo('Resource', 'grid0.fe.infn.it')
#
##     for record in res['Records']:
##       self.assert_(record[0] in ('ResultsForResource', 'SpecificInformation'))
##       self.assert_(record[1] in ('Resource_Panel'))
##       self.assert_(record[2] is not None)
##       if record[0] == 'SpecificInformation':
##         self.assert_(record[3] is not None)
##       else:
##         self.assert_(record[3] is None)
##       self.assert_(record[5] in ValidStatus)
#
## #    for panel in igR[0]['Panels'].keys():
## #      self.assert_(panel in res.keys())
## #      for i in range(len(igR[0]['Panels'][panel])):
## #        for policy in igR[0]['Panels'][panel][i].keys():
## #          self.assert_(policy in res[panel]['InfoForPanel'].keys())
## #
## #    for panel_name in res.keys():
## #      self.assert_(panel_name in igR[0]['Panels'].keys())
## #      for policy in res[panel_name]['InfoForPanel'].keys():
## #        pNames = []
## #        for i in range(len(igR[0]['Panels'][panel_name])):
## #          pNames = pNames + igR[0]['Panels'][panel_name][i].keys()
## #        self.assert_(policy in pNames)
## #        self.assert_('Status' in res[panel_name]['InfoForPanel'][policy].keys())
## #        self.assert_('Reason' in res[panel_name]['InfoForPanel'][policy].keys())
## #        self.assert_('infos' in res[panel_name]['InfoForPanel'][policy].keys())
## #        self.assert_('desc' in res[panel_name]['InfoForPanel'][policy].keys())
#
#
##     igR = [{'Panels':  {'Resource_Panel': [
##                                {'SAM_LFC_L': [{'SAM': {'args': (None, ['lfcstreams', 'lfclr', 'lfcls', 'lfcping']),
##                                                        'CommandIn': 'SAM_Tests'}}]},
##                                {'DT_Scheduled': [{'WebLink': {'args': None, 'CommandIn': 'DT_Link'}}]},
##                        ]}}]
#
##     self.mockIG.getInfoToApply.return_value = igR
#
##     res = self.p.getInfo('Resource', 'prod-lfc-lhcb-ro.cern.ch')
#
##     for record in res['Records']:
##       self.assert_(record[0] in ('ResultsForResource', 'SpecificInformation'))
##       self.assert_(record[1] in ('Resource_Panel'))
##       self.assert_(record[2] is not None)
##       if record[0] == 'SpecificInformation':
##         self.assert_(record[3] is not None)
##       else:
##         self.assert_(record[3] is None)
##       self.assert_(record[5] in ValidStatus)
#
#
## #    for panel in igR[0]['Panels'].keys():
## #      self.assert_(panel in res.keys())
## #      for i in range(len(igR[0]['Panels'][panel])):
## #        for policy in igR[0]['Panels'][panel][i].keys():
## #          self.assert_(policy in res[panel]['InfoForPanel'].keys())
## #
## #    for panel_name in res.keys():
## #      self.assert_(panel_name in igR[0]['Panels'].keys())
## #      for policy in res[panel_name]['InfoForPanel'].keys():
## #        pNames = []
## #        for i in range(len(igR[0]['Panels'][panel_name])):
## #          pNames = pNames + igR[0]['Panels'][panel_name][i].keys()
## #        self.assert_(policy in pNames)
## #        self.assert_('Status' in res[panel_name]['InfoForPanel'][policy].keys())
## #        self.assert_('Reason' in res[panel_name]['InfoForPanel'][policy].keys())
## #        self.assert_('infos' in res[panel_name]['InfoForPanel'][policy].keys())
## #        self.assert_('desc' in res[panel_name]['InfoForPanel'][policy].keys())
#
#
##     igR = [{'Panels': {'SE_Panel': [
##                           {'OnStorageElementPropagation': {'RSS': 'ResOfStorEl'}},
##                           {'TransferQuality': [{'FillChart': {'args': ('DataOperation', 'Quality', {'hours': 24, 'Format': 'LastHours'}, 'Channel', {'OperationType': 'putAndRegister'}),
##                                                               'CommandIn': 'DiracAccountingGraph'}}]},
##                           {'SEOccupancy': [{'WebLink': {'args': None, 'CommandIn': 'SLS_Link'}}]},
##                           {'SEQueuedTransfers': [{'WebLink': {'args': None, 'CommandIn': 'SLS_Link'}}]}]}}]
#
##     self.mockIG.getInfoToApply.return_value = igR
#
##     res = self.p.getInfo('StorageElementRead', 'CERN-RAW')
#
##     for record in res['Records']:
##       self.assert_(record[0] in ('ResultsForResource', 'SpecificInformation'))
##       self.assert_(record[1] in ('SE_Panel'))
##       self.assert_(record[2] is not None)
##       if record[0] == 'SpecificInformation':
##         self.assert_(record[3] is not None)
##       else:
##         self.assert_(record[3] is None)
##       self.assert_(record[5] in ValidStatus)
#
#
## #    for panel in igR[0]['Panels'].keys():
## #      self.assert_(panel in res.keys())
## #      for i in range(len(igR[0]['Panels'][panel])):
## #        for policy in igR[0]['Panels'][panel][i].keys():
## #          self.assert_(policy in res[panel]['InfoForPanel'].keys())
## #
## #    for panel_name in res.keys():
## #      self.assert_(panel_name in igR[0]['Panels'].keys())
## #      for policy in res[panel_name]['InfoForPanel'].keys():
## #        pNames = []
## #        for i in range(len(igR[0]['Panels'][panel_name])):
## #          pNames = pNames + igR[0]['Panels'][panel_name][i].keys()
## #        self.assert_(policy in pNames)
## #        self.assert_('Status' in res[panel_name]['InfoForPanel'][policy].keys())
## #        self.assert_('Reason' in res[panel_name]['InfoForPanel'][policy].keys())
## #        self.assert_('infos' in res[panel_name]['InfoForPanel'][policy].keys())
## #        self.assert_('desc' in res[panel_name]['InfoForPanel'][policy].keys())
#
#
##############################################################################
#
#class InfoGetterSuccess(UtilitiesTestCase):
#
#  def testGetInfoToApply(self):
#    ig = InfoGetter('LHCb')
#
#    for g in ValidRes:
#      for s in ValidStatus:
#        for site_t in ValidSiteType:
#          for service_t in ValidServiceType:
#
#            if g in ('Site', 'Sites'):
#              panel = 'Site_Panel'
#            if g in ('Service', 'Services'):
#              if service_t == 'Storage':
#                panel = 'Service_Storage_Panel'
#              if service_t == 'Computing':
#                panel = 'Service_Computing_Panel'
#              if service_t == 'VO-BOX':
#                panel = 'Service_VO-BOX_Panel'
#              if service_t == 'VOMS':
#                panel = 'Service_VOMS_Panel'
#            if g in ('Resource', 'Resources'):
#              panel = 'Resource_Panel'
#            if g in ('StorageElementRead', 'StorageElementsRead'):
#              panel = 'SE_Panel'
#            if g in ('StorageElementWrite', 'StorageElementsWrite'):
#              panel = 'SE_Panel'
#
#            for resource_t in ValidResourceType:
#
#              ## Testing the policyType (__getPolTypes) part
#              res = ig.getInfoToApply(('policyType', ), g, None, s, None, site_t, service_t, resource_t)
#              for p_res in res['PolicyType']:
#                self.assert_(p_res in CS.getTypedDictRootedAt("PolicyTypes").keys())
#
#              for useNewRes in (False, True):
#
#                ## Testing the policy (__getPolToEval) part
#                res = ig.getInfoToApply(('policy', ), g, None, s, None, site_t, service_t, resource_t, useNewRes)
#                pModuleList = []
#
#                for k in self.configModule.Policies.keys():
#                  try:
#                    if self.configModule.Policies[k]['module'] not in pModuleList:
#                      pModuleList.append(self.configModule.Policies[k]['module'])
#                  except KeyError:
#                    pass
#
#                for p_res in res['Policies']: # All __getPolToEval results...
#                  self.assertTrue(p_res['Name'] in CS.getTypedDictRootedAt("Policies"))
##                  self.assertTrue(p_res['Module'] in pModuleList)
#                  if useNewRes is False:
#                    self.assertEqual(p_res['commandIn'], self.configModule.Policies[p_res['Name']]['commandIn'])
#                    self.assertEqual(p_res['args'], self.configModule.Policies[p_res['Name']]['args'])
#                  else:
#                    try:
#                      self.assertEqual(p_res['commandIn'], self.configModule.Policies[p_res['Name']]['commandInNewRes'])
#                    except KeyError:
#                      self.assertEqual(p_res['commandIn'], self.configModule.Policies[p_res['Name']]['commandIn'])
#                    try:
#                      self.assertEqual(p_res['args'], self.configModule.Policies[p_res['Name']]['argsNewRes'])
#                    except KeyError:
#                      self.assertEqual(p_res['args'], self.configModule.Policies[p_res['Name']]['args'])
#
#                res = ig.getInfoToApply(('panel_info', ), g, None, s, None, site_t, service_t, resource_t, useNewRes)
#                for p_res in res['Info']:
#
##                  if 'JobsEfficiencySimple' in p_res.keys():
##                    print useNewRes, p_res
#
#
#                  for p_name in p_res.keys():
#                    self.assert_(p_name in self.configModule.Policies.keys())
#                    if isinstance(p_res[p_name], list):
#                      for i in range(len(p_res[p_name])):
#                        for k in p_res[p_name][i].keys():
#                          if useNewRes:
#                            try:
#                              self.assertEqual(p_res[p_name][i][k]['CommandIn'],
#                                               self.configModule.Policies[p_name][panel][i][k]['CommandInNewRes'])
#                            except KeyError:
#                              self.assertEqual(p_res[p_name][i][k]['CommandIn'],
#                                               self.configModule.Policies[p_name][panel][i][k]['CommandIn'])
#                            except TypeError:
#                              self.assertEqual(p_res[p_name][i][k],
#                                               self.configModule.Policies[p_name][panel][i][k])
#
#                            try:
#                              self.assertEqual(p_res[p_name][i][k]['args'],
#                                               self.configModule.Policies[p_name][panel][i][k]['argsNewRes'])
#                            except KeyError:
#                              self.assertEqual(p_res[p_name][i][k]['args'],
#                                               self.configModule.Policies[p_name][panel][i][k]['args'])
#                            except TypeError:
#                              self.assertEqual(p_res[p_name][i][k],
#                                               self.configModule.Policies[p_name][panel][i][k])
#
#                          else:
#
#                            try:
#                              self.assertEqual(p_res[p_name][i][k]['CommandIn'],
#                                               self.configModule.Policies[p_name][panel][i][k]['CommandIn'])
#                            except:
#                              self.assertEqual(p_res[p_name][i][k],
#                                               self.configModule.Policies[p_name][panel][i][k])
#
#                            try:
#                              self.assertEqual(p_res[p_name][i][k]['args'],
#                                               self.configModule.Policies[p_name][panel][i][k]['args'])
#                            except:
#                              self.assertEqual(p_res[p_name][i][k],
#                                               self.configModule.Policies[p_name][panel][i][k])
#
#                    else:
#                      self.assertEqual(p_res[p_name], self.configModule.Policies[p_name][panel])
#
#
##############################################################################
#
#class SynchronizerSuccess(UtilitiesTestCase):
#  def test__syncSites(self):
#    self.syncC._syncSites()
#
#  def test__syncResources(self):
#    self.syncC._syncResources()
#
#  def test__syncStorageElements(self):
#    self.syncC._syncStorageElements()
#
##############################################################################
#
#
#if __name__ == '__main__':
#  suite = unittest.defaultTestLoader.loadTestsFromTestCase(UtilitiesTestCase)
#  # suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(PublisherSuccess))
#  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(InfoGetterSuccess))
#  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(SynchronizerSuccess))
#  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
