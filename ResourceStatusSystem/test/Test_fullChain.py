import sys
import unittest
from DIRAC.ResourceStatusSystem.Utilities.Utils import *
from DIRAC.ResourceStatusSystem.PolicySystem.PEP import PEP
from DIRAC.ResourceStatusSystem.PolicySystem.PDP import PDP

import DIRAC.ResourceStatusSystem.test.fake_rsDB

class FullChainTestCase(unittest.TestCase):
  """ Base class for the full chain test cases
  """
  def setUp(self):
    sys.modules["DIRAC.ResourceStatusSystem.DB.ResourceStatusDB"] = DIRAC.ResourceStatusSystem.test.fake_rsDB

class PolicySystemFullChain(FullChainTestCase):
  
  def test_PolicySystemFullChain(self):

    print "\n %%%%%% SITO %%%%%%%\n "
    
    for status in ValidStatus:
      for oldStatus in ValidStatus:
        if status == oldStatus:
          continue
        print "############################"
        print " "
        print 'nel test:', status, oldStatus
        pdp = PDP('Site', 'LCG.CESGA.es', status, oldStatus, 'XX')
        res = pdp.takeDecision()
        print res

#    print "\n %%%%%% SERVICE %%%%%%%\n "
#    
#    for status in ValidStatus:
#      for oldStatus in ValidStatus:
#        if status == oldStatus:
#          continue
#        print "############################"
#        print " "
#        print 'nel test:', status, oldStatus
#        pdp = PDP('Service', 'LCG.CERN.ch', status, oldStatus, 'XX')
#        res = pdp.takeDecision()
#        print res


    
#    print "\n %%%%%% RISORSA %%%%%%%\n "
#
#    for status in ValidStatus:
#      for oldStatus in ValidStatus:
#        if status == oldStatus:
#          continue
#        print "############################"
#        print " "
#        print status, oldStatus
#        pdp = PDP('Resource', 'cs-grid1.bgu.ac.il', status, oldStatus, 'XXXXX')
#        res = pdp.takeDecision()
#        print res

#    for status in ValidStatus:
#      for oldStatus in ValidStatus:
#        if status == oldStatus:
#          continue
#        print "############################"
#        print " "
#        print status, oldStatus
#        pdp = PDP('Resource', 'ce103.cern.ch', status, oldStatus, 'XXXXX')
#        res = pdp.takeDecision()
#        print res

        
if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(FullChainTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(PolicySystemFullChain))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)