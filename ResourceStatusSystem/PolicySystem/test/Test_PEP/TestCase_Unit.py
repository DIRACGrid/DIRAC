import unittest
import inspect

class TestCase_Unit( unittest.TestCase ):
  
  def test_enforce( self ):
    
#  def enforce( self, granularity = None, name = None, statusType = None,
#                status = None, formerStatus = None, reason = None, siteType = None,
#                serviceType = None, resourceType = None, tokenOwner = None,
#                useNewRes = False, knownInfo = None  ):    
    
    #res = self.pep.enforce()
    #self.assertEquals( res, { 'PolicyCombinedResult' : {} } )
    res = self.pep.enforce( granularity = '' )
    self.assertEquals( res, { 'PolicyCombinedResult' : {} } )
    res = self.pep.enforce( name = '' )
    self.assertEquals( res, { 'PolicyCombinedResult' : {} } ) 
    res = self.pep.enforce( statusType = '' )
    self.assertEquals( res, { 'PolicyCombinedResult' : {} } )
    res = self.pep.enforce( status = '' )
    self.assertEquals( res, { 'PolicyCombinedResult' : {} } )
    res = self.pep.enforce( formerStatus = '' )
    self.assertEquals( res, { 'PolicyCombinedResult' : {} } )
    res = self.pep.enforce( reason = '' )
    self.assertEquals( res, { 'PolicyCombinedResult' : {} } )
    res = self.pep.enforce( siteType = '' )
    self.assertEquals( res, { 'PolicyCombinedResult' : {} } )
    res = self.pep.enforce( serviceType = '' )
    self.assertEquals( res, { 'PolicyCombinedResult' : {} } )
    res = self.pep.enforce( resourceType = '' )
    self.assertEquals( res, { 'PolicyCombinedResult' : {} } )
    res = self.pep.enforce( tokenOwner = '' )
    self.assertEquals( res, { 'PolicyCombinedResult' : {} } )
    res = self.pep.enforce( useNewRes = False )
    self.assertEquals( res, { 'PolicyCombinedResult' : {} } )
    res = self.pep.enforce( knownInfo = '' )
    self.assertEquals( res, { 'PolicyCombinedResult' : {} } )

    res = self.pep.enforce( granularity = '', name = '', statusType = '',
                            status = '', formerStatus = '', reason = '', 
                            siteType = '', serviceType = '', resourceType = '', 
                            tokenOwner = '', useNewRes = False, knownInfo = '' )
    self.assertEquals( res, { 'PolicyCombinedResult' : {} } )


################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF                   