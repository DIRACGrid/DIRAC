# $HeadURL: $
''' Test_RSS_Client_ResourceStatus

'''

import unittest

__RCSID__ = '$Id: $'

# MockStuff 

class DummyPassive( object ):
  
  def __init__( self, *args, **kwargs ):
    pass
  def __getattr__( self, name ):
    return self.dummyMethod
  def dummyMethod( self, *args, **kwargs ):
    pass

dummyResults = {}
class DummyReturn( object ):
  
  def __init__( self, *args, **kwargs ):
    pass
  def __getattr__( self, name ):
    return self.dummyMethod
  def dummyMethod( self, *args, **kwargs ):
    return dummyResults[ self.__class__.__name__ ]

def dummyCallable( whatever ):
  return whatever
  
class dgConfig( DummyReturn )             : pass
class dgLogger( DummyReturn )             : pass
class dCSAPI( DummyReturn )               : pass
class dResourceStatusClient( DummyReturn ): pass
  
################################################################################
# TestCases

class ResourceStatusFunctions_TestCase( unittest.TestCase ):
  
  def setUp( self ):
    '''
    Setup
    '''

    # We need the proper software, and then we overwrite it.
    import DIRAC.ResourceStatusSystem.Client.ResourceStatus as moduleTested   
    moduleTested.gConfig              = dgConfig()
    moduleTested.S_OK                 = dummyCallable
    moduleTested.S_ERROR              = dummyCallable
    moduleTested.gLogger              = dgLogger()
    moduleTested.DIRACSingleton       = type
    moduleTested.CSAPI                = dCSAPI
    moduleTested.ResourceStatusClient = dResourceStatusClient
    moduleTested.RSSCache             = DummyPassive   
      
    self.getDictFromList      = moduleTested.getDictFromList
    self.getCacheDictFromList = moduleTested.getCacheDictFromList

  def tearDown( self ):
    '''
    TearDown
    '''
    del self.getDictFromList     
    del self.getCacheDictFromList

class ResourceStatus_TestCase( unittest.TestCase ):
  
  def setUp( self ):
    '''
    Setup
    '''

    # We need the proper software, and then we overwrite it.
    import DIRAC.ResourceStatusSystem.Client.ResourceStatus as moduleTested   
    moduleTested.gConfig              = dgConfig()
    moduleTested.S_OK                 = dummyCallable
    moduleTested.S_ERROR              = dummyCallable
    moduleTested.gLogger              = dgLogger()
    moduleTested.DIRACSingleton       = type
    moduleTested.CSAPI                = dCSAPI
    moduleTested.ResourceStatusClient = dResourceStatusClient
    moduleTested.RSSCache             = DummyPassive   
      
    self.resourceStatus = moduleTested.ResourceStatus

  def tearDown( self ):
    '''
    TearDown
    '''
    del self.resourceStatus  

################################################################################
# Tests

class ResourceStatus_Success( ResourceStatus_TestCase ):
  
  def test_instantiate( self ):
    ''' tests that we can instantiate one object of the tested class
    '''  
    resourceStatus = self.resourceStatus()
    self.assertEqual( 'ResourceStatus', resourceStatus.__class__.__name__ )    
    
  def test_getMode( self ):  
    ''' tests the method getMode
    '''
    resourceStatus = self.resourceStatus()
    self.assertEquals( resourceStatus.rssClient, None )
    
    global dummyResults
    dummyResults[ 'dgConfig' ] = 'Active'
    res = resourceStatus._ResourceStatus__getMode()
    self.assertEqual( res, True )
    self.assertNotEqual( resourceStatus.rssClient, None )
    
    dummyResults[ 'dgConfig' ] = 'InActive'
    res = resourceStatus._ResourceStatus__getMode()
    self.assertEqual( res, False )
    self.assertEqual( resourceStatus.rssClient, None )
    
  def test_updateSECache( self ):
    ''' tests the method __updateSECache
    '''  
    resourceStatus = self.resourceStatus()
    resourceStatus.rssClient = dResourceStatusClient()

    global dummyResults
    dummyResults[ 'dgConfig' ]     = 'InActive'
    res = resourceStatus._ResourceStatus__updateSECache()
    self.assertEqual( res, { 'OK' : False, 'Message' : 'RSS flag is inactive' } )
    dummyResults[ 'dgConfig' ]              = 'Active'
    dummyResults[ 'dResourceStatusClient' ] = { 'OK' : False, 'Message' : 'Black Friday' }
    res = resourceStatus._ResourceStatus__updateSECache()
    self.assertEqual( res, { 'OK' : False, 'Message' : 'Black Friday' } )
    dummyResults[ 'dResourceStatusClient' ] = { 'OK' : True, 'Value' : [ ( 1,2,3 ) ] }
    res = resourceStatus._ResourceStatus__updateSECache()
    # mocking makes this return something funny ( due to S_OK mocked function )
    self.assertEqual( res, { '1#2': 3 } )
    dummyResults[ 'dResourceStatusClient' ] = { 'OK' : True, 'Value' : [ ( 1,2,3 ), (1,3,4) ] }
    res = resourceStatus._ResourceStatus__updateSECache()
    self.assertEqual( res, { '1#2': 3, '1#3' : 4 } )
    
class ResourceStatusFunctions_Success( ResourceStatusFunctions_TestCase ):
  
  def test_getDictFromList( self ):
    ''' tests the logic behind the function getDictFromList
    '''
    res = self.getDictFromList( [ ( 1,2,3 ), ( 4,5,6 ) ] )
    self.assertEqual( res, { 1: { 2 : 3 }, 4 : { 5 : 6 } } )
    res = self.getDictFromList( [ ( 1,2,3 ), ( 1,5,6 ) ] )
    self.assertEqual( res, { 1: { 2 : 3, 5 : 6 } } )
    res = self.getDictFromList( [ ( 1,2,3 ), ( 1,5,6 ), ( 1,5,7 ) ] )
    self.assertEqual( res, { 1: { 2 : 3, 5 : 7 } } )
  
  def test_getCacheDictFromList( self ):
    ''' tests the logic behind the function getCacheDictFromList
    '''
    res = self.getCacheDictFromList( [ (1,2,3), (4,5,6) ] )
    self.assertEqual( res, { '1#2' : 3, '4#5' : 6 } )
    res = self.getCacheDictFromList( [ (1,2,3), (4,5,6), ( 'A', '#', '#' ) ] )
    self.assertEqual( res, { '1#2' : 3, '4#5' : 6, 'A##' : '#' } ) 
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF