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

returnResults = {}
class DummyReturn( object ):
  
  def __init__( self, *args, **kwargs ):
    pass
  def __getattr__( self, name ):
    return self.dummyMethod
  def dummyMethod( self, *args, **kwargs ):
    return returnResults[ self.__name__ ]
  
class dgConfig( DummyReturn )             : pass
class dgLogger( DummyReturn )             : pass
class dS_OK( DummyReturn )                : pass
class dS_ERROR( DummyReturn )             : pass
class dCSAPI( DummyReturn )               : pass
class dResourceStatusClient( DummyReturn ): pass
  
################################################################################
# TestCases

class ResourceStatus_TestCase( unittest.TestCase ):
  
  def setUp( self ):
    '''
    Setup
    '''

    # We need the proper software, and then we overwrite it.
    import DIRAC.ResourceStatusSystem.Client.ResourceStatus as moduleTested   
    moduleTested.gConfig              = dgConfig()
    moduleTested.S_OK                 = dS_OK()
    moduleTested.S_ERROR              = dS_ERROR()
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
    self.assertEqual( 'ResourceStatus', cache.__class__.__name__ )    
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF