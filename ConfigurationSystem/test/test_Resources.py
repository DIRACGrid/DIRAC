import unittest
from DIRAC.Core.Base import Script
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import Resources

class ResourcesTestCase( unittest.TestCase ):
  
  def setUp( self ):
  
    Script.disableCS( )
    Script.parseCommandLine()
    self.resources = Resources()
  
  def test_getSites( self ):
  
    print
    result = self.resources.getSites( {'Name':['CERN','CPPM','PNPI']} )
    self.assertTrue( result['OK'], 'getSites' )
    sites = result['Value']
    print sites
    result = self.resources.getEligibleSites( {'Name':['CERN','CPPM','PNPI']} )    
    self.assertTrue( result['OK'], 'getEligibleSites' )
    eligibleSites = result['Value']
    self.assertEqual(sites, eligibleSites, 'sites and eligible sites are the same')

  def test_getResources( self ):
    
    print
    result = self.resources.getResources( 'CERN', 'Storage' )
    self.assertTrue( result['OK'], 'getResources' )
    ses = result['Value']
    print ses
    
  def test_getNodes( self ):
    
    print
    result = self.resources.getNodes( 'CERN::ce130', 'Queue'  )
    self.assertTrue( result['OK'], 'getNodes' )
    nodes = result['Value']
    print nodes  
    
  def test_getEligibleResources( self ):
    
    print 
    result = self.resources.getEligibleResources( 'Computing', { 'Site':['CERN','CPPM','Zurich'],'SubmissionMode':'Direct' }  )
    self.assertTrue( result['OK'], 'getEligibleResources' )
    ces = result['Value']
    print ces    
    
  def test_getEligibleNodes( self ):
    
    print
    result = self.resources.getEligibleNodes( 'AccessProtocol', 
                                              { 'Site':['CERN','CPPM','Zurich'] },
                                              { 'Protocol':'srm' }  )
    self.assertTrue( result['OK'], 'getEligibleNodes' )
    aps = result['Value']
    print aps   
    
  def test_getEligibleComputingElements( self ):
    
    siteMask = ['LCG.CERN.ch','LCG.CPPM.fr']
    
    result = self.resources.getEligibleResources( 'Computing', {'Site':siteMask,
                                                                'SubmissionMode':'gLite',
                                                                'CEType':['LCG','CREAM']} )  
    self.assertTrue( result['OK'], 'getEligibleResources' )
    print
    for ce in result['Value']:
      ceHost = self.resources.getComputingElementValue( ce, 'Host', 'unknown' )
      print ce, ceHost 
     
suite = unittest.TestLoader().loadTestsFromTestCase(ResourcesTestCase)
unittest.TextTestRunner(verbosity=2).run(suite)