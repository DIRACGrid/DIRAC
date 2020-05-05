from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import unittest

from mock import MagicMock

from DIRAC import gLogger
from DIRAC.ResourceStatusSystem.PolicySystem.PEP          import PEP
# from DIRAC.ResourceStatusSystem.PolicySystem.PDP          import PDP
# from DIRAC.ResourceStatusSystem.PolicySystem.PolicyCaller import PolicyCaller

#############################################################################

class PolicySystemTestCase(unittest.TestCase):
  """ Base class for the PDP - PEP test cases
  """
#############################################################################

  def setUp(self):
    gLogger.setLevel( 'DEBUG' )

    self.RSMock = MagicMock()
    self.RMMock = MagicMock()
    self.RMMock.selectStatusElement.return_value = {'OK':True, 'Value': 'bla'}
    self.mockPDP = MagicMock()

#############################################################################

class PEPSuccess(PolicySystemTestCase):

#############################################################################

  def test_enforce(self):

    pep = PEP( {'ResourceStatusClient':self.RSMock, 'ResourceManagementClient': self.RMMock, 'SiteStatus': self.RMMock} )
    pep.pdp = self.mockPDP
    res = pep.enforce( None )
    self.assertTrue(res['OK'])

    decisionParams = {}
    res = pep.enforce( decisionParams )
    self.assertTrue(res['OK'])

    decisionParams = {'element':'Site', 'name': 'Site1'}
    decParamsPDP = dict(decisionParams)
    decParamsPDP['active'] = 'active'
    self.mockPDP.takeDecision.return_value = {'OK':True,
                                              'Value':{'policyCombinedResult': {'PolicyType':['', ''],
                                                                                'PolicyAction':[( 'aa', 'bb' )],
                                                                                'Status':'S',
                                                                                'Reason':'testReason'},
                                                       'singlePolicyResults': [{'Status': 'Active',
                                                                                'PolicyName': 'SAM_CE_Policy',
                                                                                'Reason': 'SAM:ok'},
                                                                               {'Status': 'Banned',
                                                                                'PolicyName': 'DT_Policy_Scheduled',
                                                                                'Reason': 'DT:OUTAGE in 1 hours',
                                                                                'EndDate': '2010-02-16 15:00:00'}],
                                                       'decisionParams':decParamsPDP}}
    res = pep.enforce( decisionParams )
    self.assertTrue(res['OK'])

    decisionParams = {'element':'Resource', 'name': 'StorageElement', 'statusType': 'ReadAccess'}
    res = pep.enforce( decisionParams )
    self.assertTrue(res['OK'])

# class PDPSuccess(PolicySystemTestCase):
#
# #############################################################################
#
#
#   def test_takeDecision(self):
#
#     for granularity in ValidRes:
#       for status in ValidStatus:
#         for oldStatus in ValidStatus:
#           if status == oldStatus: continue
#           self.mock_p.evaluate.return_value = [{'Status': status, 'Reason': 'testReason', 'PolicyName': 'test_P'}]
#           pdp = PDP(self.VO, granularity, 'XX', oldStatus, None, 'XX')
#           res = pdp.takeDecision(policyIn = self.mock_p)
#           res = res['PolicyCombinedResult']
#           self.assertTrue(res['Action'])
#
#           res = pdp.takeDecision(policyIn = self.mock_p, argsIn = ())
#           res = res['PolicyCombinedResult']
#           self.assertTrue(res['Action'])
#
#           res = pdp.takeDecision(policyIn = self.mock_p, knownInfo={})
#           res = res['PolicyCombinedResult']
#           self.assertTrue(res['Action'])
#
#   def test__policyCombination(self):
#
#     for granularity in ValidRes:
#       for status in ValidStatus:
#         for oldStatus in ValidStatus:
#           if status == oldStatus:
#             continue
#
#           for newStatus1 in ValidStatus:
#             for newStatus2 in ValidStatus:
#               pdp = PDP(self.VO, granularity, 'XX', status, oldStatus, 'XX')
#               polRes  = {'Status':newStatus1, 'Reason':'-Reason1-'}
#               polRes2 = {'Status':newStatus2, 'Reason':'-Reason2-'}
#
#
#               # 0 policies
#               res = pdp._policyCombination([])
#               self.assertEqual(res, {})
#
#               # 1 policy
#               res = pdp._policyCombination([polRes])
#
#               if status == 'Banned':
#                 self.assertTrue(value_of_status(res['Status']) <= 1)
#
#               if status == 'Banned' and newStatus1 in ['Active','Bad','Probing']:
#                 self.assertEqual(res['Status'], 'Probing')
#               else:
#                 self.assertEqual(res['Status'], newStatus1)
#
#
#               # 2 policies
#               res = pdp._policyCombination([polRes, polRes2])
#
#               if status == 'Banned':
#                 self.assertTrue(value_of_status(res['Status']) <= 1)
#
#               if status == 'Banned' and newStatus1 in ['Active','Bad','Probing'] and newStatus2 in ['Active','Bad','Probing']:
#                 self.assertEqual(res['Status'], 'Probing')
#
#               if status != 'Banned' and value_of_status(newStatus1) < value_of_status(newStatus1):
#                 self.assertEqual(res['Status'], newStatus1)
#               if status != 'Banned' and value_of_status(newStatus2) < value_of_status(newStatus1):
#                 self.assertEqual(res['Status'], newStatus2)
#
#               # all different policies
#               def make_polres(status):
#                 return { 'Status': status, 'Reason': 'Because of ' + status }
#               all_polres = [make_polres(s) for s in ValidStatus]
#
#               res = pdp._policyCombination(all_polres)
#               self.assertEqual(res['Status'], 'Banned')
#
# #############################################################################
#
# class PDPFailure(PolicySystemTestCase):
#
# #############################################################################
#
#   def test_PolicyFail(self):
#     for granularity in ValidRes:
#       for status in ValidStatus:
#         for oldStatus in ValidStatus:
#           if status == oldStatus:
#             continue
#           pdp = PDP(self.VO, granularity, 'XX', status, oldStatus, 'XX')
#           self.assertRaises(Exception, pdp.takeDecision, self.mock_pdp, self.mock_rsDB)
#           self.assertRaises(Exception, pdp.takeDecision, self.mock_pdp, self.mock_rsDB, knownInfo={'DT':'AT_RISK'})
#
# #############################################################################
#
#   def test_PDPBadInputs(self):
#     for status in ValidStatus:
#       for oldStatus in ValidStatus:
#         if status == oldStatus:
#           continue
#     for granularity in ValidRes:
#       for oldStatus in ValidStatus:
#         for status in ValidStatus:
#           if status == oldStatus:
#             continue
#
# #############################################################################
#
# class PolicyCallerSuccess(PolicySystemTestCase):
#
#   def test_policyInvocation(self):
#     cc = Mock()
#
#     policies_modules = {'Site':['DT_Policy', 'GGUSTickets_Policy'],
#                         'Service': ['PilotsEfficiency_Simple_Policy', 'JobsEfficiency_Simple_Policy'],
#                         'Resource':['SAMResults_Policy', 'DT_Policy'],
#                         'StorageElementRead':['SEOccupancy_Policy', 'TransferQuality_Policy'],
#                         'StorageElementWrite':['SEOccupancy_Policy', 'TransferQuality_Policy']
#                         }
#
#     for g in ValidRes:
#       for status in ValidStatus:
#         self.mock_p.evaluate.return_value = {'Status':status,
#                                              'Reason':'testReason',
#                                              'PolicyName': 'test_P'}
#         pc = PolicyCaller(commandCallerIn = cc)
#
#         for pol_mod in policies_modules[g]:
#           res = pc.policyInvocation(self.VO, g, 'XX', status, self.mock_p,
#                                     (g, 'XX'), None, pol_mod)
#           self.assertEqual(res['Status'], status)
#
#           res = pc.policyInvocation(self.VO, g, 'XX', status, self.mock_p,
#                                     None, None, pol_mod)
#           self.assertEqual(res['Status'], status)
#
#           for extraArgs in ((g, 'XX'), [(g, 'XX'), (g, 'XX')]):
#             res = pc.policyInvocation(self.VO, g, 'XX', status, self.mock_p,
#                                       None, None, pol_mod, extraArgs)
#             self.assertEqual(res['Status'], status)
#
# #############################################################################
#
# class PolicyBaseSuccess(PolicySystemTestCase):
#
#   def test_setArgs(self):
#     for g in ValidRes:
#       for a in [(g, 'XX')]:
#         self.pb.setArgs(a)
#         self.assertEqual(self.pb.args, a)
#
#   def test_evaluate(self):
#     for g in ValidRes:
#       for a in [(g, 'XX')]:
#         self.pb.setArgs(a)
#         self.mock_command.doCommand.return_value = {'Result':'aRes'}
#         self.pb.setCommand(self.mock_command)
#         res = self.pb.evaluate()
#         self.assertEqual(res, 'aRes')
#
# #############################################################################
#
# class PolicyBaseFailure(PolicySystemTestCase):
#
#   def test_setBadArgs(self):
#
#
#     # 6 arguments should be handled with no problem: why the limitation to 5 ?! (removing this test)
#     # self.pb.setArgs(('Site', 'XX', 'Active', 'BOH', 'BOH', 'BOH'))
#     # self.mock_command.doCommand.return_value = {'Result':'aRes'}
#     # self.pb.setCommand(self.mock_command)
#     # Lists are unsupported by Command for now, useless to test.
#     # self.pb.setArgs([('Site', 'XX', 'Active', 'BOH', 'BOH', 'BOH'), ('Site', 'XX', 'Active', 'BOH', 'BOH', 'BOH')])
#
# #############################################################################
#
# # class PolicyInvokerSuccess(PolicySystemTestCase):
#
# #   def test_setPolicy(self):
# #     self.pi.setPolicy(self.mock_policy)
# #     self.assertEqual(self.pi.policy, self.mock_policy)
#
# #   def test_evaluatePolicy(self):
#
# #     self.mock_policy.evaluate.return_value = {'Result':'Satisfied', 'Status':'Banned', 'Reason':"reason"}
# #     self.pi.setPolicy(self.mock_policy)
# #     for granularity in ValidRes:
# #       res = self.pi.evaluatePolicy()
# #       self.assertEqual(res['Result'], 'Satisfied')
# #     self.mock_policy.evaluate.return_value = {'Result':'Un-Satisfied'}
# #     self.pi.setPolicy(self.mock_policy)
# #     for granularity in ValidRes:
# #       res = self.pi.evaluatePolicy()
# #       self.assertEqual(res['Result'], 'Un-Satisfied')
#
# # #############################################################################
#
# # class PolicyInvokerFailure(PolicySystemTestCase):
#
# #   def test_policyFail(self):
# #     for granularity in ValidRes:
# #       self.assertRaises(Exception, self.pi.evaluatePolicy)

#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(PolicySystemTestCase)
#   suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(PolicyBaseSuccess))
#   suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(PolicyBaseFailure))
#  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(PolicyInvokerSuccess))
#  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(PolicyInvokerFailure))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(PEPSuccess))
#   suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(PEPFailure))
#   suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(PDPSuccess))
#   suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(PDPFailure))
#   suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(PolicyCallerSuccess))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)

#############################################################################
