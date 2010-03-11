import unittest
import sys
from DIRAC.ResourceStatusSystem.Utilities.mock import Mock
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Utilities.Utils import *
from DIRAC.ResourceStatusSystem.Utilities.InfoGetter import InfoGetter
from DIRAC.ResourceStatusSystem.PolicySystem.PEP import PEP
from DIRAC.ResourceStatusSystem.PolicySystem.PDP import PDP
from DIRAC.ResourceStatusSystem.PolicySystem.PolicyCaller import PolicyCaller

import DIRAC.ResourceStatusSystem.test.fake_NotificationClient

#############################################################################

class PolicySystemTestCase(unittest.TestCase):
  """ Base class for the PDP - PEP test cases
  """
#############################################################################

  def setUp(self):
    from DIRAC.Core.Base import Script
    Script.parseCommandLine() 
    
    sys.modules["DIRAC.FrameworkSystem.Client.NotificationClient"] = DIRAC.ResourceStatusSystem.test.fake_NotificationClient
    self.mock_p = Mock()
    self.mock_args = Mock()
    self.mock_pdp = Mock()
    self.mock_rsDB = Mock()
    self.mock_nc = Mock()
    self.ig = InfoGetter()
    
#############################################################################

class PEPSuccess(PolicySystemTestCase):
  
#############################################################################

  def test_enforce(self):

    for policyType in PolicyTypes:
      for granularity in ValidRes:
        for status in ValidStatus:
          for oldStatus in ValidStatus:
            if status == oldStatus:
              continue
            for newPolicyType in PolicyTypes:
              for newGranularity in ValidRes:
#                for siteType in ValidSiteType:
#                for serviceType in ValidServiceType:
#                for resourceType in ValidResourceType:
                self.mock_pdp.takeDecision.return_value = {'PolicyCombinedResult': [{'PolicyType':[policyType, newPolicyType],
                                                                                     'Action':True, 'Status':status,
                                                                                     'Reason':'testReason'}], 
                                                           'SinglePolicyResults': [{'Status': 'Active', 
                                                                                    'PolicyName': 'SAM_CE_Policy', 
                                                                                    'Reason': 'SAM:ok', 
                                                                                    'SAT': True}, 
                                                                                    {'Status': 'Banned', 
                                                                                     'PolicyName': 'DT_Policy_Scheduled', 
                                                                                     'Reason': 'DT:OUTAGE in 1 hours', 
                                                                                     'EndDate': '2010-02-16 15:00:00', 
                                                                                     'SAT': True}] }
                pep = PEP(granularity, 'XX', status, oldStatus, 'XX', 'T1', 'Computing', 'CE', {'PolicyType':newPolicyType, 'Granularity':newGranularity})
                self.mock_pdp.takeDecision.return_value = {'PolicyCombinedResult': [{'PolicyType':[policyType, newPolicyType], 
                                                                                     'Action':True, 'Status':status, 
                                                                                     'Reason':'testReason'}, 
                                                                                     {'PolicyType':[policyType, newPolicyType], 
                                                                                      'Action':True, 'Status':status, 
                                                                                      'Reason':'testReason'}], 
                                                           'SinglePolicyResults': [{'Status': 'Active', 
                                                                                    'PolicyName': 'SAM_CE_Policy', 
                                                                                    'Reason': 'SAM:ok', 
                                                                                    'SAT': True}, 
                                                                                    {'Status': 'Banned', 
                                                                                     'PolicyName': 'DT_Policy_Scheduled', 
                                                                                     'Reason': 'DT:OUTAGE in 1 hours', 
                                                                                     'EndDate': '2010-02-16 15:00:00', 
                                                                                     'SAT': True}] }
                pep = PEP(granularity, 'XX', status, oldStatus, 'XX', 'T1', 'Computing', 'CE', {'PolicyType':newPolicyType, 'Granularity':newGranularity})
                res = pep.enforce(pdpIn = self.mock_pdp, rsDBIn = self.mock_rsDB, ncIn = self.mock_nc)
                self.assertEqual(res, None)
                self.mock_pdp.takeDecision.return_value = {'PolicyCombinedResult': [{'PolicyType':[policyType, newPolicyType],
                                                                                     'Action':False, 'Reason':'testReason'}], 
                                                           'SinglePolicyResults': [{'Status': 'Active', 
                                                                                    'PolicyName': 'SAM_CE_Policy', 
                                                                                    'Reason': 'SAM:ok', 
                                                                                    'SAT': True}, 
                                                                                    {'Status': 'Banned', 
                                                                                     'PolicyName': 'DT_Policy_Scheduled', 
                                                                                     'Reason': 'DT:OUTAGE in 1 hours', 
                                                                                     'EndDate': '2010-02-16 15:00:00', 
                                                                                     'SAT': True}] }
                res = pep.enforce(pdpIn = self.mock_pdp, rsDBIn = self.mock_rsDB, ncIn = self.mock_nc)
                self.assertEqual(res, None)
            self.mock_pdp.takeDecision.return_value = {'PolicyCombinedResult': [{'PolicyType':[policyType, newPolicyType], 
                                                                                 'Action':True, 'Status':status, 'Reason':'testReason'}], 
                                                       'SinglePolicyResults': [{'Status': 'Active', 
                                                                                    'PolicyName': 'SAM_CE_Policy', 
                                                                                    'Reason': 'SAM:ok', 
                                                                                    'SAT': True}, 
                                                                                    {'Status': 'Banned', 
                                                                                     'PolicyName': 'DT_Policy_Scheduled', 
                                                                                     'Reason': 'DT:OUTAGE in 1 hours', 
                                                                                     'EndDate': '2010-02-16 15:00:00', 
                                                                                     'SAT': True}] }
            pep = PEP(granularity, 'XX', status, oldStatus, 'XX', 'T1', 'Computing', 'CE')
            res = pep.enforce(pdpIn = self.mock_pdp, rsDBIn = self.mock_rsDB, ncIn = self.mock_nc)
            self.assertEqual(res, None)
            self.mock_pdp.takeDecision.return_value =  {'PolicyCombinedResult': [{'PolicyType':[policyType, newPolicyType], 
                                                                                  'Action':False, 'Reason':'testReason'}], 
                                                       'SinglePolicyResults': [{'Status': 'Active', 
                                                                                'PolicyName': 'SAM_CE_Policy', 
                                                                                'Reason': 'SAM:ok', 
                                                                                'SAT': True}, 
                                                                                {'Status': 'Banned', 
                                                                                 'PolicyName': 'DT_Policy_Scheduled', 
                                                                                 'Reason': 'DT:OUTAGE in 1 hours', 
                                                                                 'EndDate': '2010-02-16 15:00:00', 
                                                                                 'SAT': True}] }
            res = pep.enforce(pdpIn = self.mock_pdp, rsDBIn = self.mock_rsDB, ncIn = self.mock_nc)
            self.assertEqual(res, None)
        self.mock_pdp.takeDecision.return_value = {'PolicyCombinedResult': [{'PolicyType':[policyType, newPolicyType], 
                                                                             'Action':True, 'Status':status, 'Reason':'testReason'}], 
                                                  'SinglePolicyResults': [{'Status': 'Active', 
                                                                           'PolicyName': 'SAM_CE_Policy', 
                                                                           'Reason': 'SAM:ok', 
                                                                           'SAT': True}, 
                                                                           {'Status': 'Banned', 
                                                                            'PolicyName': 'DT_Policy_Scheduled', 
                                                                            'Reason': 'DT:OUTAGE in 1 hours', 
                                                                            'EndDate': '2010-02-16 15:00:00', 
                                                                            'SAT': True}] }
        pep = PEP(granularity, 'XX')
        res = pep.enforce(pdpIn = self.mock_pdp, rsDBIn = self.mock_rsDB, ncIn = self.mock_nc)
        self.assertEqual(res, None)
        pep = PEP(granularity, 'XX')
        res = pep.enforce(pdpIn = self.mock_pdp, rsDBIn = self.mock_rsDB, ncIn = self.mock_nc)
        self.assertEqual(res, None)
        self.mock_pdp.takeDecision.return_value = {'PolicyCombinedResult': [{'PolicyType':[policyType, newPolicyType], 
                                                                             'Action':False, 'Reason':'testReason'}], 
                                                  'SinglePolicyResults': [{'Status': 'Active', 
                                                                           'PolicyName': 'SAM_CE_Policy', 
                                                                           'Reason': 'SAM:ok', 
                                                                           'SAT': True}, 
                                                                           {'Status': 'Banned', 
                                                                            'PolicyName': 'DT_Policy_Scheduled', 
                                                                            'Reason': 'DT:OUTAGE in 1 hours', 
                                                                            'EndDate': '2010-02-16 15:00:00', 
                                                                            'SAT': True}] }
        res = pep.enforce(pdpIn = self.mock_pdp, rsDBIn = self.mock_rsDB, ncIn = self.mock_nc)
        self.assertEqual(res, None)

#############################################################################

class PEPFailure(PolicySystemTestCase):
  
#############################################################################

  def test_PEPFail(self):

    self.mock_pdp.takeDecision.sideEffect = RSSException
    for policyType in PolicyTypes:
      for granularity in ValidRes:
        for status in ValidStatus:
          for oldStatus in ValidStatus:
            if status == oldStatus:
              continue
            for newPolicyType in PolicyTypes:
              for newGranularity in ValidRes:
                for siteType in ValidSiteType:
                  for serviceType in ValidServiceType:
                    for resourceType in ValidResourceType:
                      pep = PEP(granularity, 'XX', status, oldStatus, 'XX', siteType, serviceType, resourceType,  {'PolicyType':newPolicyType, 'Granularity':newGranularity})
                      self.failUnlessRaises(Exception, pep.enforce, self.mock_pdp, self.mock_rsDB, ncIn = self.mock_nc)
                      self.failUnlessRaises(Exception, pep.enforce, self.mock_pdp, self.mock_rsDB, knownInfo={'DT':'AT_RISK'}, ncIn = self.mock_nc)
    
#############################################################################

  def test_PEPBadInputs(self):
    for policyType in PolicyTypes:
      for status in ValidStatus:
        for oldStatus in ValidStatus:
          if status == oldStatus:
            continue
          self.failUnlessRaises(InvalidRes, PEP, 'sites', 'LCG.Ferrara.it', status, oldStatus, 'XX')
    for policyType in PolicyTypes:
      for granularity in ValidRes:
        for status in ValidStatus:
          for oldStatus in ValidStatus:
            if status == oldStatus:
              continue
            self.failUnlessRaises(InvalidStatus, PEP, granularity, 'XX', 'actives', oldStatus, 'XX')
            self.failUnlessRaises(InvalidStatus, PEP, granularity, 'XX', status, 'banneds', 'XX')


#############################################################################

class PDPSuccess(PolicySystemTestCase):
  
#############################################################################

  def test_takeDecision(self):

    for granularity in ValidRes:
      for status in ValidStatus:
        for oldStatus in ValidStatus:
          if status == oldStatus:
            continue
          self.mock_p.evaluate.return_value = [{'SAT':True, 'Status':status, 
                                               'Reason':'testReason', 'PolicyName': 'test_P'}]
          pdp = PDP(granularity, 'XX', status, oldStatus, 'XX')
          res = pdp.takeDecision(policyIn = self.mock_p)
          res = res['PolicyCombinedResult']
          for r in res:
            self.assert_(r['Action'])
          res = pdp.takeDecision(policyIn = self.mock_p, argsIn = ())
          res = res['PolicyCombinedResult']
          for r in res:
            self.assert_(r['Action'])
          res = pdp.takeDecision(policyIn = self.mock_p, knownInfo={})
          res = res['PolicyCombinedResult']
          for r in res:
            self.assert_(r['Action'])
          self.mock_p.evaluate.return_value = [{'SAT':False, 'Status':status, 
                                               'Reason':'testReason', 'PolicyName': 'test_P'}]
          res = pdp.takeDecision(policyIn = self.mock_p)
          res = res['PolicyCombinedResult']
          for r in res:
            self.assertFalse(r['Action'])
          res = pdp.takeDecision(policyIn = self.mock_p, argsIn = ())
          res = res['PolicyCombinedResult']
          for r in res:
            self.assertFalse(r['Action'])
          res = pdp.takeDecision(policyIn = self.mock_p, knownInfo={})
          res = res['PolicyCombinedResult']
          for r in res:
            self.assertFalse(r['Action'])
 
  def test__policyCombination(self):
    
    for granularity in ValidRes:
      for status in ValidStatus:
        for oldStatus in ValidStatus:
          if status == oldStatus:
            continue
          for newStatus1 in ValidStatus:
            newStatusF1 = newStatus1
            for newStatus2 in ValidStatus:
              newStatusF2 = newStatus2
              if newStatus1 == newStatusF2 or newStatusF1 == newStatus2:
                continue
              pdp = PDP(granularity, 'XX', status, oldStatus, 'XX')
              polRes = {'SAT':True, 'Status':newStatus1, 'Reason':'-Reason1-'}
              polResF = {'SAT':False, 'Status':newStatusF2, 'Reason':'-Reason2-'}
              # 1 policy
              res = pdp._policyCombination(polRes)
              self.assert_(res['SAT'])
              self.assertEqual(res['Status'], newStatus1)
              self.assertEqual(res['Reason'], '-Reason1-')
              res = pdp._policyCombination(polResF)
              self.assertFalse(res['SAT'])
              self.assertEqual(res['Status'], newStatus2)
              self.assertEqual(res['Reason'], '-Reason2-')

              # 2 policies
              # FALSE, FALSE
              res = pdp._policyCombination(polResF, polResF)
              self.assertFalse(res['SAT'])
              self.assertEqual(res['Status'], newStatus2)
              self.assertEqual(res['Reason'], '-Reason2- |###| -Reason2-')
              # FALSE, TRUE
#              polResF = {'SAT':False, 'Status':newStatus2, 'Reason':'-Reason2-'}
              if newStatus1 == newStatusF2 or newStatusF1 == newStatus2:
                continue
              else:
                res = pdp._policyCombination(polResF, polRes)
                if ValidStatus.index(newStatus1) > ValidStatus.index(newStatus2):
                  self.assert_(res['SAT'])
                  self.assertEqual(res['Status'], newStatus1)
                  self.assertEqual(res['Reason'], '-Reason1-')
                elif ValidStatus.index(newStatus1) < ValidStatus.index(newStatus2):
                  self.assertFalse(res['SAT'])
                  self.assertEqual(res['Status'], newStatus2)
                  self.assertEqual(res['Reason'], '-Reason2-')
                # TRUE, FALSE
   #             polResF = {'SAT':False, 'Status':newStatus2, 'Reason':'-Reason2-'}
                res = pdp._policyCombination(polRes, polResF)
                if ValidStatus.index(newStatus1) > ValidStatus.index(newStatus2):
                  self.assert_(res['SAT'])
                  self.assertEqual(res['Status'], newStatus1)
                  self.assertEqual(res['Reason'], '-Reason1-')
                elif ValidStatus.index(newStatus1) < ValidStatus.index(newStatus2):
                  self.assertFalse(res['SAT'])
                  self.assertEqual(res['Status'], newStatus2)
                  self.assertEqual(res['Reason'], '-Reason2-')
              # TRUE, TRUE
              polRes2 = {'SAT':True, 'Status':newStatus2, 'Reason':'-Reason2-'}
              res = pdp._policyCombination(polRes, polRes2)
              if ValidStatus.index(newStatus1) > ValidStatus.index(newStatus2):
                self.assert_(res['SAT'])
                self.assertEqual(res['Status'], newStatus1)
                self.assertEqual(res['Reason'], '-Reason1-')
              elif ValidStatus.index(newStatus1) < ValidStatus.index(newStatus2):
                self.assert_(res['SAT'])
                self.assertEqual(res['Status'], newStatus2)
                self.assertEqual(res['Reason'], '-Reason2-')
              elif ValidStatus.index(newStatus1) == ValidStatus.index(newStatus2):
                self.assert_(res['SAT'])
                self.assertEqual(res['Status'], newStatus1)
                self.assertEqual(res['Status'], newStatus2)
                self.assertEqual(res['Reason'], '-Reason1-|-Reason2-')

            for newStatus3 in ValidStatus:
              newStatusF3 = newStatus3
              if newStatus1 == status:
                continue
              if newStatus2 == status:
                continue
              if newStatus3 == status:
                continue
              polRes = {'SAT':True, 'Status':newStatus1, 'Reason':'-ReasonTrue-'}
              polRes2 = {'SAT':True, 'Status':newStatus2, 'Reason':'-ReasonTrue2-'}
              polRes3 = {'SAT':True, 'Status':newStatus3, 'Reason':'-ReasonTrue3-'}
              polResF = {'SAT':False, 'Status':newStatusF1, 'Reason':'-ReasonFalse-'}
              polResF2 = {'SAT':False, 'Status':newStatusF2, 'Reason':'-ReasonFalse2-'}
              polResF3 = {'SAT':False, 'Status':newStatusF3, 'Reason':'-ReasonFalse3-'}
              # TRUE, TRUE, TRUE
              
#              print  "status", status, "newStatus1", newStatus1, "newStatus2", newStatus2,  "newStatus3", newStatus3  
              
              res = pdp._policyCombination(polRes, polRes2, polRes3)
              if ValidStatus.index(newStatus1) > ValidStatus.index(newStatus2):
                if ValidStatus.index(newStatus2) > ValidStatus.index(newStatus3):
                  self.assert_(res['SAT'])
                  self.assertEqual(res['Status'], newStatus1)
                  self.assertEqual(res['Reason'], '-ReasonTrue-')
              if ValidStatus.index(newStatus1) > ValidStatus.index(newStatus3):
                if ValidStatus.index(newStatus3) > ValidStatus.index(newStatus2):
                  self.assert_(res['SAT'])
                  self.assertEqual(res['Status'], newStatus1)
                  self.assertEqual(res['Reason'], '-ReasonTrue-')
              if ValidStatus.index(newStatus2) > ValidStatus.index(newStatus1):
                if ValidStatus.index(newStatus1) > ValidStatus.index(newStatus3):
                  self.assert_(res['SAT'])
                  self.assertEqual(res['Status'], newStatus2)
                  self.assertEqual(res['Reason'], '-ReasonTrue2-')
              elif ValidStatus.index(newStatus1) == ValidStatus.index(newStatus2) == ValidStatus.index(newStatus3):
                self.assert_(res['SAT'])
                self.assertEqual(res['Status'], newStatus1)
                self.assertEqual(res['Status'], newStatus2)
                self.assertEqual(res['Status'], newStatus3)
                self.assertEqual(res['Reason'], '-ReasonTrue- |###| -ReasonTrue2- |###| -ReasonTrue3-')
              # etc...
              # TRUE, TRUE, FALSE
              if newStatusF3 == newStatus1 or newStatusF3 == newStatus2:
                continue
              else:  
                res = pdp._policyCombination(polRes, polRes2, polResF3)
                if ValidStatus.index(newStatus1) > ValidStatus.index(newStatus2):
                  if ValidStatus.index(newStatus2) > ValidStatus.index(newStatus3):
                    self.assert_(res['SAT'])
                    self.assertEqual(res['Status'], newStatus1)
                    self.assertEqual(res['Reason'], '-ReasonTrue-')
                if ValidStatus.index(newStatus1) > ValidStatus.index(newStatus3):
                  if ValidStatus.index(newStatus3) > ValidStatus.index(newStatus2):
                    print newStatus3
                    print res
                    self.assert_(res['SAT'])
                    self.assertEqual(res['Status'], newStatus1)
                    self.assertEqual(res['Reason'], '-ReasonTrue-')
                if ValidStatus.index(newStatus3) > ValidStatus.index(newStatus2):
                  if ValidStatus.index(newStatus2) > ValidStatus.index(newStatus1):
                    print newStatus3
                    print res
                    self.assert_(res['SAT'])
                    self.assertEqual(res['Status'], newStatus3)
                    self.assertEqual(res['Reason'], '-ReasonTrue3-')


#############################################################################

class PDPFailure(PolicySystemTestCase):
  
#############################################################################

  def test_PolicyFail(self):
    self.mock_p.evaluate.sideEffect = RSSException
    for granularity in ValidRes:
      for status in ValidStatus:
        for oldStatus in ValidStatus:
          if status == oldStatus:
            continue
          pdp = PDP(granularity, 'XX', status, oldStatus, 'XX')
          self.failUnlessRaises(Exception, pdp.takeDecision, self.mock_pdp, self.mock_rsDB)
          self.failUnlessRaises(Exception, pdp.takeDecision, self.mock_pdp, self.mock_rsDB, knownInfo={'DT':'AT_RISK'})
        
#############################################################################

  def test_PDPBadInputs(self):
    for status in ValidStatus:
      for oldStatus in ValidStatus:
        if status == oldStatus:
          continue
        self.failUnlessRaises(InvalidRes, PDP, 'sites', 'XX', status, oldStatus, 'XX')
    for granularity in ValidRes:
      for oldStatus in ValidStatus:
        for status in ValidStatus:
          if status == oldStatus:
            continue
          self.failUnlessRaises(InvalidStatus, PDP, granularity, 'XX', 'actives', oldStatus, 'XX')
          self.failUnlessRaises(InvalidStatus, PDP, granularity, 'XX', status, 'banneds', 'XX')

#############################################################################

class PolicyCallerSuccess(PolicySystemTestCase):

  def test_policyInvocation(self):
    for g in ValidRes:
      for status in ValidStatus:
        self.mock_p.evaluate.return_value = {'SAT':True, 'Status':status, 
                                             'Reason':'testReason', 
                                             'PolicyName': 'test_P'}
        pc = PolicyCaller()
        policies_names = self.ig.getInfoToApply(('policy', ), g)[0]['Policies']
        for pol_name in policies_names:
          res = pc.policyInvocation(g, 'XX', status, self.mock_p, 
                                    (g, 'XX', status), pol_name)
          self.assert_(res['SAT'])
  
#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(PolicySystemTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(PEPSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(PEPFailure))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(PDPSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(PDPFailure))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(PolicyCallerSuccess))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
  
#############################################################################
