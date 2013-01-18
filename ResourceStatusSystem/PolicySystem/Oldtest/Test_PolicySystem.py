#import unittest
#import sys
#
#from DIRAC.Core.Base import Script
#Script.parseCommandLine()
#
#from DIRAC.ResourceStatusSystem.Utilities.mock       import Mock
#
#from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
#from DIRAC.ResourceStatusSystem.PolicySystem.Status import *
#
#import DIRAC.ResourceStatusSystem.test.fake_Logger
#import DIRAC.ResourceStatusSystem.test.fake_Admin
#import DIRAC.ResourceStatusSystem.test.fake_NotificationClient
#
#from DIRAC.ResourceStatusSystem.PolicySystem.PEP          import PEP
#from DIRAC.ResourceStatusSystem.PolicySystem.PDP          import PDP
#from DIRAC.ResourceStatusSystem.PolicySystem.PolicyCaller import PolicyCaller
#
##############################################################################
#
#class PolicySystemTestCase(unittest.TestCase):
#  """ Base class for the PDP - PEP test cases
#  """
##############################################################################
#
#  def setUp(self):
#    sys.modules["DIRAC"] = DIRAC.ResourceStatusSystem.test.fake_Logger
#    sys.modules["DIRAC.ResourceStatusSystem.Utilities.CS"] = DIRAC.ResourceStatusSystem.test.fake_Logger
#    sys.modules["DIRAC.Core.Utilities.SiteCEMapping"] = DIRAC.ResourceStatusSystem.test.fake_Logger
#    sys.modules["DIRAC.Core.Utilities.SiteSEMapping"] = DIRAC.ResourceStatusSystem.test.fake_Logger
#    sys.modules["DIRAC.Core.Utilities.SitesDIRACGOCDBmapping"] = DIRAC.ResourceStatusSystem.test.fake_Logger
#    sys.modules["DIRAC.Interfaces.API.DiracAdmin"] = DIRAC.ResourceStatusSystem.test.fake_Admin
#    sys.modules["DIRAC.FrameworkSystem.Client.NotificationClient"] = DIRAC.ResourceStatusSystem.test.fake_NotificationClient
#
#    from DIRAC.ResourceStatusSystem.Utilities.InfoGetter import InfoGetter
#    from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase
##    from DIRAC.ResourceStatusSystem.PolicySystem.PolicyInvoker import PolicyInvoker
#
#    from DIRAC import gConfig
#    self.VO = gConfig.getValue("DIRAC/Extensions")
#    if 'LHCb' in self.VO:
#      self.VO = 'LHCb'
#
#    self.mock_command = Mock()
#    self.mock_policy = Mock()
#    self.mock_p = Mock()
#    self.mock_args = Mock()
#    self.pb = PolicyBase()
##    self.pi = PolicyInvoker()
#    self.mock_pdp = Mock()
#    self.mock_rsDB = Mock()
#    self.mock_rmDB = Mock()
#    self.mock_nc = Mock()
#    self.mock_da = Mock()
#    self.mock_da.getBannedSites.return_value = {'OK': True,
#                                                'Value': ['LCG.APC.fr', 'LCG.Bari.it', 'LCG.Catania.it']}
#    self.mock_da.addSiteInMask.return_value = {'OK': True, 'Value': ''}
#    self.mock_da.banSiteFromMask.return_value = {'OK': True, 'Value': ''}
#    self.mock_da.sendMail.return_value = {'OK': True, 'Value': ''}
#    self.mock_csAPI = Mock()
#    self.mock_csAPI.setOption.return_value = {'OK': True, 'Value': ''}
#    self.mock_csAPI.commit.return_value = {'OK': True, 'Value': ''}
#    self.ig = InfoGetter(self.VO)
#
##############################################################################
#
#class PEPSuccess(PolicySystemTestCase):
#
##############################################################################
#
#  def test_enforce(self):
#
#    for policyType in PolicyTypes:
#      for granularity in ValidRes:
#        for status in ValidStatus:
#          oldStatus = status # oldStatus never used by anything, let's reduce by 2 the duration of this lengthy test!!
#          for newPolicyType in PolicyTypes:
#            if policyType == newPolicyType:
#              continue
##              for newGranularity in ValidRes:
#            for siteType in ValidSiteType:
#              for serviceType in ValidServiceType:
#                for resourceType in ValidResourceType:
#                  for user in ("RS_SVC", "Federico"):
#                    for setup in ("LHCb-Production", "LHCb-Development", "LHCb-Certification"):
#
#                      self.mock_pdp.takeDecision.return_value = {'PolicyCombinedResult': {'PolicyType':[policyType, newPolicyType],
#                                                                                          'Action':True, 'Status':status,
#                                                                                          'Reason':'testReason'},
#                                                                 'SinglePolicyResults': [{'Status': 'Active',
#                                                                                          'PolicyName': 'SAM_CE_Policy',
#                                                                                          'Reason': 'SAM:ok'},
#                                                                                         {'Status': 'Banned',
#                                                                                          'PolicyName': 'DT_Policy_Scheduled',
#                                                                                          'Reason': 'DT:OUTAGE in 1 hours',
#                                                                                          'EndDate': '2010-02-16 15:00:00'}]}
#          #                pep = PEP(granularity, 'XX', status, oldStatus, 'XX', 'T1', 'Computing', 'CE', {'PolicyType':newPolicyType, 'Granularity':newGranularity})
#
#
#                      pep = PEP(self.VO, granularity, 'XX', status, oldStatus, 'XX', siteType,
#                                serviceType, resourceType, user)
#
#
#        #                self.mock_pdp.takeDecision.return_value = {'PolicyCombinedResult': [{'PolicyType':[policyType, newPolicyType],
#        #                                                                                     'Action':True, 'Status':status,
#        #                                                                                     'Reason':'testReason'},
#        #                                                                                     {'PolicyType':[policyType, newPolicyType],
#        #                                                                                      'Action':True, 'Status':status,
#        #                                                                                      'Reason':'testReason'}],
#        #                                                           'SinglePolicyResults': [{'Status': 'Active',
#        #                                                                                    'PolicyName': 'SAM_CE_Policy',
#        #                                                                                    'Reason': 'SAM:ok',
#        #                                                                                    'SAT': True},
#        #                                                                                    {'Status': 'Banned',
#        #                                                                                     'PolicyName': 'DT_Policy_Scheduled',
#        #                                                                                     'Reason': 'DT:OUTAGE in 1 hours',
#        #                                                                                     'EndDate': '2010-02-16 15:00:00',
#        #                                                                                     'SAT': True}] }
#          #                pep = PEP(granularity, 'XX', status, oldStatus, 'XX', 'T1', 'Computing', 'CE', {'PolicyType':newPolicyType, 'Granularity':newGranularity})
#        #                pep = PEP(granularity, 'XX', status, oldStatus, 'XX', 'T1', 'Computing', 'CE', user)
#                      self.mock_rsDB.getMonitoredsHistory.return_value = ('Active', 'Reason', '2010-04-09 09:54:52')
#                      res = pep.enforce(pdpIn = self.mock_pdp, rsDBIn = self.mock_rsDB, rmDBIn = self.mock_rmDB, ncIn = self.mock_nc,
#                                        setupIn = setup, daIn = self.mock_da, csAPIIn = self.mock_csAPI)
#                      self.assertEqual(res, None)
#                      self.mock_pdp.takeDecision.return_value = {'PolicyCombinedResult': {'PolicyType':[policyType, newPolicyType],
#                                                                                             'Action':False, 'Reason':'testReason'},
#                                                                   'SinglePolicyResults': [{'Status': 'Active',
#                                                                                            'PolicyName': 'SAM_CE_Policy',
#                                                                                            'Reason': 'SAM:ok'},
#                                                                                            {'Status': 'Banned',
#                                                                                             'PolicyName': 'DT_Policy_Scheduled',
#                                                                                             'Reason': 'DT:OUTAGE in 1 hours',
#                                                                                             'EndDate': '2010-02-16 15:00:00'}]}
#                      res = pep.enforce(pdpIn = self.mock_pdp, rsDBIn = self.mock_rsDB, rmDBIn = self.mock_rmDB, ncIn = self.mock_nc,
#                                        setupIn = setup, daIn = self.mock_da, csAPIIn = self.mock_csAPI)
#                      self.assertEqual(res, None)
#
#
#        #            self.mock_pdp.takeDecision.return_value = {'PolicyCombinedResult': [{'PolicyType':[policyType, newPolicyType],
#        #                                                                                 'Action':True, 'Status':status, 'Reason':'testReason'}],
#        #                                                       'SinglePolicyResults': [{'Status': 'Active',
#        #                                                                                    'PolicyName': 'SAM_CE_Policy',
#        #                                                                                    'Reason': 'SAM:ok',
#        #                                                                                    'SAT': True},
#        #                                                                                    {'Status': 'Banned',
#        #                                                                                     'PolicyName': 'DT_Policy_Scheduled',
#        #                                                                                     'Reason': 'DT:OUTAGE in 1 hours',
#        #                                                                                     'EndDate': '2010-02-16 15:00:00',
#        #                                                                                     'SAT': True}] }
#        #            pep = PEP(granularity, 'XX', status, oldStatus, 'XX', 'T1', 'Computing', 'CE')
#        #            res = pep.enforce(pdpIn = self.mock_pdp, rsDBIn = self.mock_rsDB, ncIn = self.mock_nc)
#        #            self.assertEqual(res, None)
#        #            self.mock_pdp.takeDecision.return_value =  {'PolicyCombinedResult': [{'PolicyType':[policyType, newPolicyType],
#        #                                                                                  'Action':False, 'Reason':'testReason'}],
#        #                                                       'SinglePolicyResults': [{'Status': 'Active',
#        #                                                                                'PolicyName': 'SAM_CE_Policy',
#        #                                                                                'Reason': 'SAM:ok',
#        #                                                                                'SAT': True},
#        #                                                                                {'Status': 'Banned',
#        #                                                                                 'PolicyName': 'DT_Policy_Scheduled',
#        #                                                                                 'Reason': 'DT:OUTAGE in 1 hours',
#        #                                                                                 'EndDate': '2010-02-16 15:00:00',
#        #                                                                                 'SAT': True}] }
#        #            res = pep.enforce(pdpIn = self.mock_pdp, rsDBIn = self.mock_rsDB, ncIn = self.mock_nc)
#        #            self.assertEqual(res, None)
#
#
#        #        self.mock_pdp.takeDecision.return_value = {'PolicyCombinedResult': [{'PolicyType':[policyType, newPolicyType],
#        #                                                                             'Action':True, 'Status':status, 'Reason':'testReason'}],
#        #                                                  'SinglePolicyResults': [{'Status': 'Active',
#        #                                                                           'PolicyName': 'SAM_CE_Policy',
#        #                                                                           'Reason': 'SAM:ok',
#        #                                                                           'SAT': True},
#        #                                                                           {'Status': 'Banned',
#        #                                                                            'PolicyName': 'DT_Policy_Scheduled',
#        #                                                                            'Reason': 'DT:OUTAGE in 1 hours',
#        #                                                                            'EndDate': '2010-02-16 15:00:00',
#        #                                                                            'SAT': True}] }
#        #        pep = PEP(granularity, 'XX')
#        #        res = pep.enforce(pdpIn = self.mock_pdp, rsDBIn = self.mock_rsDB, ncIn = self.mock_nc)
#        #        self.assertEqual(res, None)
#        #        pep = PEP(granularity, 'XX')
#        #        res = pep.enforce(pdpIn = self.mock_pdp, rsDBIn = self.mock_rsDB, ncIn = self.mock_nc)
#        #        self.assertEqual(res, None)
#        #        self.mock_pdp.takeDecision.return_value = {'PolicyCombinedResult': [{'PolicyType':[policyType, newPolicyType],
#        #                                                                             'Action':False, 'Reason':'testReason'}],
#        #                                                  'SinglePolicyResults': [{'Status': 'Active',
#        #                                                                           'PolicyName': 'SAM_CE_Policy',
#        #                                                                           'Reason': 'SAM:ok',
#        #                                                                           'SAT': True},
#        #                                                                           {'Status': 'Banned',
#        #                                                                            'PolicyName': 'DT_Policy_Scheduled',
#        #                                                                            'Reason': 'DT:OUTAGE in 1 hours',
#        #                                                                            'EndDate': '2010-02-16 15:00:00',
#        #                                                                            'SAT': True}] }
#        #        res = pep.enforce(pdpIn = self.mock_pdp, rsDBIn = self.mock_rsDB, ncIn = self.mock_nc)
#        #        self.assertEqual(res, None)
#
##############################################################################
#
#class PEPFailure(PolicySystemTestCase):
#
##############################################################################
#
#  def test_PEPFail(self):
#
#    for policyType in PolicyTypes:
#      for granularity in ValidRes:
#        for status in ValidStatus:
#          for oldStatus in ValidStatus:
#            if status == oldStatus:
#              continue
#            for newPolicyType in PolicyTypes:
#              if policyType == newPolicyType:
#                continue
##              for newGranularity in ValidRes:
#              for siteType in ValidSiteType:
#                for serviceType in ValidServiceType:
#                  for resourceType in ValidResourceType:
##                    pep = PEP(granularity, 'XX', status, oldStatus, 'XX', siteType, serviceType, resourceType,  {'PolicyType':newPolicyType, 'Granularity':newGranularity})
#                    pep = PEP(self.VO, granularity, 'XX', status, oldStatus, 'XX', siteType, serviceType, resourceType)
#                    self.failUnlessRaises(Exception, pep.enforce, self.mock_pdp, self.mock_rsDB, ncIn = self.mock_nc,
#                                          setupIn = 'LHCb-Development', daIn = self.mock_da, csAPIIn = self.mock_csAPI )
#                    self.failUnlessRaises(Exception, pep.enforce, self.mock_pdp, self.mock_rsDB, knownInfo={'DT':'AT_RISK'},
#                                          ncIn = self.mock_nc, setupIn = 'LHCb-Development', daIn = self.mock_da,
#                                          csAPIIn = self.mock_csAPI )
#
#
##############################################################################
#
#  def test_PEPBadInputs(self):
#    for policyType in PolicyTypes:
#      for status in ValidStatus:
#        for oldStatus in ValidStatus:
#          if status == oldStatus:
#            continue
#    for policyType in PolicyTypes:
#      for granularity in ValidRes:
#        for status in ValidStatus:
#          for oldStatus in ValidStatus:
#            if status == oldStatus:
#              continue
#
#
##############################################################################
#
#class PDPSuccess(PolicySystemTestCase):
#
##############################################################################
#
#
#  def test_takeDecision(self):
#
#    for granularity in ValidRes:
#      for status in ValidStatus:
#        for oldStatus in ValidStatus:
#          if status == oldStatus: continue
#          self.mock_p.evaluate.return_value = [{'Status': status, 'Reason': 'testReason', 'PolicyName': 'test_P'}]
#          pdp = PDP(self.VO, granularity, 'XX', oldStatus, None, 'XX')
#          res = pdp.takeDecision(policyIn = self.mock_p)
#          res = res['PolicyCombinedResult']
#          self.assert_(res['Action'])
#
#          res = pdp.takeDecision(policyIn = self.mock_p, argsIn = ())
#          res = res['PolicyCombinedResult']
#          self.assert_(res['Action'])
#
#          res = pdp.takeDecision(policyIn = self.mock_p, knownInfo={})
#          res = res['PolicyCombinedResult']
#          self.assert_(res['Action'])
#
#  def test__policyCombination(self):
#
#    for granularity in ValidRes:
#      for status in ValidStatus:
#        for oldStatus in ValidStatus:
#          if status == oldStatus:
#            continue
#
#          for newStatus1 in ValidStatus:
#            for newStatus2 in ValidStatus:
#              pdp = PDP(self.VO, granularity, 'XX', status, oldStatus, 'XX')
#              polRes  = {'Status':newStatus1, 'Reason':'-Reason1-'}
#              polRes2 = {'Status':newStatus2, 'Reason':'-Reason2-'}
#
#
#              # 0 policies
#              res = pdp._policyCombination([])
#              self.assertEqual(res, {})
#
#              # 1 policy
#              res = pdp._policyCombination([polRes])
#
#              if status == 'Banned':
#                self.assertTrue(value_of_status(res['Status']) <= 1)
#
#              if status == 'Banned' and newStatus1 in ['Active','Bad','Probing']:
#                self.assertEqual(res['Status'], 'Probing')
#              else:
#                self.assertEqual(res['Status'], newStatus1)
#
#
#              # 2 policies
#              res = pdp._policyCombination([polRes, polRes2])
#
#              if status == 'Banned':
#                self.assertTrue(value_of_status(res['Status']) <= 1)
#
#              if status == 'Banned' and newStatus1 in ['Active','Bad','Probing'] and newStatus2 in ['Active','Bad','Probing']:
#                self.assertEqual(res['Status'], 'Probing')
#
#              if status != 'Banned' and value_of_status(newStatus1) < value_of_status(newStatus1):
#                self.assertEqual(res['Status'], newStatus1)
#              if status != 'Banned' and value_of_status(newStatus2) < value_of_status(newStatus1):
#                self.assertEqual(res['Status'], newStatus2)
#
#              # all different policies
#              def make_polres(status):
#                return { 'Status': status, 'Reason': 'Because of ' + status }
#              all_polres = [make_polres(s) for s in ValidStatus]
#
#              res = pdp._policyCombination(all_polres)
#              self.assertEqual(res['Status'], 'Banned')
#
##############################################################################
#
#class PDPFailure(PolicySystemTestCase):
#
##############################################################################
#
#  def test_PolicyFail(self):
#    for granularity in ValidRes:
#      for status in ValidStatus:
#        for oldStatus in ValidStatus:
#          if status == oldStatus:
#            continue
#          pdp = PDP(self.VO, granularity, 'XX', status, oldStatus, 'XX')
#          self.failUnlessRaises(Exception, pdp.takeDecision, self.mock_pdp, self.mock_rsDB)
#          self.failUnlessRaises(Exception, pdp.takeDecision, self.mock_pdp, self.mock_rsDB, knownInfo={'DT':'AT_RISK'})
#
##############################################################################
#
#  def test_PDPBadInputs(self):
#    for status in ValidStatus:
#      for oldStatus in ValidStatus:
#        if status == oldStatus:
#          continue
#    for granularity in ValidRes:
#      for oldStatus in ValidStatus:
#        for status in ValidStatus:
#          if status == oldStatus:
#            continue
#
##############################################################################
#
#class PolicyCallerSuccess(PolicySystemTestCase):
#
#  def test_policyInvocation(self):
#    cc = Mock()
#
#    policies_modules = {'Site':['DT_Policy', 'GGUSTickets_Policy'],
#                        'Service': ['PilotsEfficiency_Simple_Policy', 'JobsEfficiency_Simple_Policy'],
#                        'Resource':['SAMResults_Policy', 'DT_Policy'],
#                        'StorageElementRead':['SEOccupancy_Policy', 'TransferQuality_Policy'],
#                        'StorageElementWrite':['SEOccupancy_Policy', 'TransferQuality_Policy']
#                        }
#
#    for g in ValidRes:
#      for status in ValidStatus:
#        self.mock_p.evaluate.return_value = {'Status':status,
#                                             'Reason':'testReason',
#                                             'PolicyName': 'test_P'}
#        pc = PolicyCaller(commandCallerIn = cc)
#
#        for pol_mod in policies_modules[g]:
#          res = pc.policyInvocation(self.VO, g, 'XX', status, self.mock_p,
#                                    (g, 'XX'), None, pol_mod)
#          self.assertEqual(res['Status'], status)
#
#          res = pc.policyInvocation(self.VO, g, 'XX', status, self.mock_p,
#                                    None, None, pol_mod)
#          self.assertEqual(res['Status'], status)
#
#          for extraArgs in ((g, 'XX'), [(g, 'XX'), (g, 'XX')]):
#            res = pc.policyInvocation(self.VO, g, 'XX', status, self.mock_p,
#                                      None, None, pol_mod, extraArgs)
#            self.assertEqual(res['Status'], status)
#
##############################################################################
#
#class PolicyBaseSuccess(PolicySystemTestCase):
#
#  def test_setArgs(self):
#    for g in ValidRes:
#      for a in [(g, 'XX')]:
#        self.pb.setArgs(a)
#        self.assertEqual(self.pb.args, a)
#
#  def test_evaluate(self):
#    for g in ValidRes:
#      for a in [(g, 'XX')]:
#        self.pb.setArgs(a)
#        self.mock_command.doCommand.return_value = {'Result':'aRes'}
#        self.pb.setCommand(self.mock_command)
#        res = self.pb.evaluate()
#        self.assertEqual(res, 'aRes')
#
##############################################################################
#
#class PolicyBaseFailure(PolicySystemTestCase):
#
#  def test_setBadArgs(self):
#
#
#    # 6 arguments should be handled with no problem: why the limitation to 5 ?! (removing this test)
#    # self.pb.setArgs(('Site', 'XX', 'Active', 'BOH', 'BOH', 'BOH'))
#    # self.mock_command.doCommand.return_value = {'Result':'aRes'}
#    # self.pb.setCommand(self.mock_command)
#    # Lists are unsupported by Command for now, useless to test.
#    # self.pb.setArgs([('Site', 'XX', 'Active', 'BOH', 'BOH', 'BOH'), ('Site', 'XX', 'Active', 'BOH', 'BOH', 'BOH')])
#
##############################################################################
#
## class PolicyInvokerSuccess(PolicySystemTestCase):
#
##   def test_setPolicy(self):
##     self.pi.setPolicy(self.mock_policy)
##     self.assertEqual(self.pi.policy, self.mock_policy)
#
##   def test_evaluatePolicy(self):
#
##     self.mock_policy.evaluate.return_value = {'Result':'Satisfied', 'Status':'Banned', 'Reason':"reason"}
##     self.pi.setPolicy(self.mock_policy)
##     for granularity in ValidRes:
##       res = self.pi.evaluatePolicy()
##       self.assertEqual(res['Result'], 'Satisfied')
##     self.mock_policy.evaluate.return_value = {'Result':'Un-Satisfied'}
##     self.pi.setPolicy(self.mock_policy)
##     for granularity in ValidRes:
##       res = self.pi.evaluatePolicy()
##       self.assertEqual(res['Result'], 'Un-Satisfied')
#
## #############################################################################
#
## class PolicyInvokerFailure(PolicySystemTestCase):
#
##   def test_policyFail(self):
##     for granularity in ValidRes:
##       self.failUnlessRaises(Exception, self.pi.evaluatePolicy)
#
##############################################################################
#
#if __name__ == '__main__':
#  suite = unittest.defaultTestLoader.loadTestsFromTestCase(PolicySystemTestCase)
#  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(PolicyBaseSuccess))
#  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(PolicyBaseFailure))
##  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(PolicyInvokerSuccess))
##  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(PolicyInvokerFailure))
#  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(PEPSuccess))
#  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(PEPFailure))
#  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(PDPSuccess))
#  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(PDPFailure))
#  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(PolicyCallerSuccess))
#  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
#
##############################################################################
