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

    sito = {'name':'LCG.CESGA.fr', 'siteType':'T2'}
    servizio = {'name':'Computing`@LCG.Ferrrara.it', 'siteType':'T2', 'serviceType':'Computing'}
#    risorsa = {'name':'srmlhcb.pic.es', 'siteType':'T1', 'resourceType':'SE'}
    risorsa = {'name':'cream-1-fzk.gridka.de', 'siteType':'T1', 'resourceType':'CE'}
    se = {'name':'CERN-RAW', 'siteType':'T0', 'resourceType':'SE'}
    
    
#    print "\n ~~~~~~~ SITO ~~~~~~~ %s \n" %(sito)
#    
#    for status in ValidStatus:
#      for oldStatus in ValidStatus:
#        if status == oldStatus:
#          continue
#        print "############################"
#        print " "
#        print 'nel test:', status, oldStatus
#        pdp = PDP(granularity = 'Site', name = sito['name'], status = status, 
#                  formerStatus = oldStatus, reason = 'XXXXX', siteType = sito['siteType']) 
#        res = pdp.takeDecision()
#        print res
#
#    print "\n ~~~~~~~ SERVICE ~~~~~~~ : %s \n " %servizio
#    
#    for status in ValidStatus:
#      for oldStatus in ValidStatus:
#        if status == oldStatus:
#          continue
#        print "############################"
#        print " "
#        print 'nel test:', status, oldStatus
#        pdp = PDP(granularity = 'Service', name = servizio['name'], status = status, 
#                  formerStatus = oldStatus, reason = 'XXXXX', siteType = servizio['siteType'],
#                  serviceType = servizio['serviceType']) 
#        res = pdp.takeDecision()
#        print res
#
#
#    
    print "\n ~~~~~~~ RISORSA ~~~~~~~ : %s \n " %risorsa

    for status in ValidStatus:
      for oldStatus in ValidStatus:
        if status == oldStatus:
          continue
        print "############################"
        print " "
        print status, oldStatus
        pdp = PDP(granularity = 'Resource', name = risorsa['name'], status = status, 
                  formerStatus = oldStatus, reason = 'XXXXX', siteType = risorsa['siteType'], 
                  resourceType = risorsa['resourceType'])
        res = pdp.takeDecision()
        print res

 
#    print "\n ~~~~~~~ StorageElement ~~~~~~~ : %s \n " %se
#
#    for status in ValidStatus:
#      for oldStatus in ValidStatus:
#        if status == oldStatus:
#          continue
#        print "############################"
#        print " "
#        print status, oldStatus
#        pdp = PDP(granularity = 'Resource', name = risorsa['name'], status = status, 
#                  formerStatus = oldStatus, reason = 'XXXXX', siteType = risorsa['siteType'], 
#                  resourceType = risorsa['resourceType']) 
#        res = pdp.takeDecision()
#        print res

        
if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(FullChainTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(PolicySystemFullChain))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)