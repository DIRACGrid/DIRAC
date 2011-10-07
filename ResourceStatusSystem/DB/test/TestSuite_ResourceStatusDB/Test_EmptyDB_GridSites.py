from DIRAC import S_OK
from DIRAC.ResourceStatusSystem.DB.test.TestSuite_ResourceStatusDB.TestCase_EmptyDB import TestCase_EmptyDB

import inspect

class Test_EmptyDB_GridSites( TestCase_EmptyDB ):  
  
  def test_01_addOrModifyGridSite( self ):
    '''
    addOrModifyGridSite( self, gridSiteName, gridTier )
    '''
 
    ins = inspect.getargspec( self.rsDB.addOrModifyGridSite.f )   
    self.assertEqual( ins.args, [ 'self', 'gridSiteName', 'gridTier' ] )
    self.assertEqual( ins.varargs,  None)
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )
 
    initArgs = [ None, None ]
    
    #All fail because first parameter does not validate
    modArgs = initArgs[:]
    res = self.rsDB.addOrModifyGridSite( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )

    modArgs[ 1 ] = 'T2'
    res = self.rsDB.addOrModifyGridSite( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ 'T2' ]
    res = self.rsDB.addOrModifyGridSite( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.addOrModifyGridSite( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ 'eGGs' ]
    res = self.rsDB.addOrModifyGridSite( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ 'T2', 'eGGs' ]
    res = self.rsDB.addOrModifyGridSite( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ None ]
    res = self.rsDB.addOrModifyGridSite( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ 'T2', 'eGGs', None ]
    res = self.rsDB.addOrModifyGridSite( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    #All fail because second parameter does not validate
    modArgs[ 0 ] = 'AUVERGRID'
    res = self.rsDB.addOrModifyGridSite( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'AUVERGRID' ]
    res = self.rsDB.addOrModifyGridSite( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = 'eGGs'
    res = self.rsDB.addOrModifyGridSite( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'eGGs' ]
    res = self.rsDB.addOrModifyGridSite( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'AUVERGRID', 'eGGs' ]
    res = self.rsDB.addOrModifyGridSite( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ None ]
    res = self.rsDB.addOrModifyGridSite( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'AUVERGRID', 'eGGs', None ]
    res = self.rsDB.addOrModifyGridSite( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    #First and second parameter
    res = self.rsDB.addOrModifyGridSite( 'AUVERGRID', 'T2' )
    self.assertEqual( res[ 'OK' ], True )
    res = self.rsDB.addOrModifyGridSite( 'AUVERGRID', 'T2' )
    self.assertEqual( res[ 'OK' ], True )

    res = self.rsDB.addOrModifyGridSite( [ 'AUVERGRID' ], 'T2' )
    self.assertEqual( res[ 'OK' ], False )            
    res = self.rsDB.addOrModifyGridSite( 'AUVERGRID', 'eGGs' )
    self.assertEqual( res[ 'OK' ], False )
    res = self.rsDB.addOrModifyGridSite( 'AUVERGRID', [ 'T2' ] )
    self.assertEqual( res[ 'OK' ], False )       
    res = self.rsDB.addOrModifyGridSite( [ 'AUVERGRID' ], [ 'T2' ] )
    self.assertEqual( res[ 'OK' ], False )       

  def test_02_getGridSites( self ):
    '''
    getGridSites( self, gridSiteName, gridTier, **kwargs )
    '''
    
    ins = inspect.getargspec( self.rsDB.getGridSites.f )   
    self.assertEqual( ins.args, ['self', 'gridSiteName', 'gridTier'] )
    self.assertEqual( ins.varargs,  None)
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, None )
    
    initArgs = [ None, None ]
    
    res = self.rsDB.getGridSites( *tuple( initArgs ) )
    self.assertEqual( res[ 'OK' ], True )
    
    #Test first parameter ( gridSiteName )
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'AUVERGRID'
    res = self.rsDB.getGridSites( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'AUVERGRID' ]
    res = self.rsDB.getGridSites( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], True )
    modArgs[ 0 ] = 'eGGs'
    res = self.rsDB.getGridSites( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'eGGs' ]
    res = self.rsDB.getGridSites( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'AUVERGRID', 'eGGs' ]
    res = self.rsDB.getGridSites( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'AUVERGRID', 'INFN-T1' ]
    res = self.rsDB.getGridSites( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], True )
    modArgs[ 0 ] = [ None ]
    res = self.rsDB.getGridSites( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'AUVERGRID', 'eGGs', None ]
    res = self.rsDB.getGridSites( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'AUVERGRID', 'eGGs', None, 'INFN-T1' ]
    res = self.rsDB.getGridSites( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], True )

    #Test second parameter ( gridTier )
    modArgs = initArgs[:]
    modArgs[ 1 ] = 'T2'
    res = self.rsDB.getGridSites( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], True )
    modArgs[ 1 ] = [ 'T2' ]
    res = self.rsDB.getGridSites( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], True )
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.getGridSites( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], True )
    modArgs[ 1 ] = [ 'eGGs' ]
    res = self.rsDB.getGridSites( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], True )
    modArgs[ 1 ] = [ 'T2', 'eGGs' ]
    res = self.rsDB.getGridSites( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], True )
    modArgs[ 1 ] = [ 'T2', 'T1' ]
    res = self.rsDB.getGridSites( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], True )
    modArgs[ 1 ] = [ None ]
    res = self.rsDB.getGridSites( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], True )
    modArgs[ 1 ] = [ 'T2', 'eGGs', None ]
    res = self.rsDB.getGridSites( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], True )
    modArgs[ 1 ] = [ 'T2', 'eGGs', None, 'T1' ]
    res = self.rsDB.getGridSites( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], True )
   
  def test_03_deleteGridSites( self ):
    '''
    deleteGridSites( self, gridSiteName )
    '''
    
    ins = inspect.getargspec( self.rsDB.deleteGridSites.f )   
    self.assertEqual( ins.args, ['self', 'gridSiteName'] )
    self.assertEqual( ins.varargs,  None)
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )
    
    res = self.rsDB.deleteGridSites( None )
    self.assertEqual( res[ 'OK' ], False )
    res = self.rsDB.deleteGridSites( [ None ] ) 
    self.assertEqual( res[ 'OK' ], False )
    res = self.rsDB.deleteGridSites( [ 'AUVERGRID', None ] ) 
    self.assertEqual( res[ 'OK' ], False )
    
    res = self.rsDB.deleteGridSites( 'eGGs' )
    self.assertEqual( res[ 'OK' ], True )
    res = self.rsDB.deleteGridSites( 'AUVERGRID' ) 
    self.assertEqual( res[ 'OK' ], True )
    res = self.rsDB.deleteGridSites( [ 'AUVERGRID' ] ) 
    self.assertEqual( res[ 'OK' ], True )
    res = self.rsDB.deleteGridSites( [ 'INFN-T1', 'eGGs' ] ) 
    self.assertEqual( res[ 'OK' ], True )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF           