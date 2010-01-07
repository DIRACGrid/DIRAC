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

    sito = 'LCG.CESGA.fr'
    servizio = 'Computing`@LCG.Ferrrara.it'
    risorsa = 'srmlhcb.pic.es'
    se = 'CERN-RAW'
    
    
    print "\n ~~~~~~~ SITO ~~~~~~~ %s \n" %(sito)
    
    for status in ValidStatus:
      for oldStatus in ValidStatus:
        if status == oldStatus:
          continue
        print "############################"
        print " "
        print 'nel test:', status, oldStatus
        pdp = PDP('Site', sito, status, oldStatus, 'XX')
        res = pdp.takeDecision()
        print res

    print "\n ~~~~~~~ SERVICE ~~~~~~~ : %s \n " %servizio
    
    for status in ValidStatus:
      for oldStatus in ValidStatus:
        if status == oldStatus:
          continue
        print "############################"
        print " "
        print 'nel test:', status, oldStatus
        pdp = PDP('Service', servizio, status, oldStatus, 'XX')
        res = pdp.takeDecision()
        print res


    
    print "\n ~~~~~~~ RISORSA ~~~~~~~ : %s \n " %risorsa

    for status in ValidStatus:
      for oldStatus in ValidStatus:
        if status == oldStatus:
          continue
        print "############################"
        print " "
        print status, oldStatus
        pdp = PDP('Resource', risorsa, status, oldStatus, 'XXXXX')
        res = pdp.takeDecision()
        print res


 
    print "\n ~~~~~~~ StorageElement ~~~~~~~ : %s \n " %se

    for status in ValidStatus:
      for oldStatus in ValidStatus:
        if status == oldStatus:
          continue
        print "############################"
        print " "
        print status, oldStatus
        pdp = PDP('StorageElement', se, status, oldStatus, 'XXXXX')
        res = pdp.takeDecision()
        print res

        
if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(FullChainTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(PolicySystemFullChain))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)