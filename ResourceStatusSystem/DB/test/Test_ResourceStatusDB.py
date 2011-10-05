import unittest
import itertools

from datetime import datetime
#import datetime

from DIRAC.Core.Base import Script
Script.parseCommandLine()

from DIRAC.ResourceStatusSystem.DB.ResourceStatusDB import ResourceStatusDB
from DIRAC.Core.Utilities.MySQL import MySQL

from DIRAC import S_OK

################################################################################

class Test_ResourceStatusDB( unittest.TestCase ):
  
  def setUp( self ):
    
    db        = MySQL( 'localhost', 'test', 'test', 'ResourceStatusTestDB' )
    self.rsDB = ResourceStatusDB( DBin = db )
    
################################################################################         
    
class Test_GridSites( Test_ResourceStatusDB ):  
  
  def test_GridSites01_addOrModifyGridSite( self ):
    '''
    addOrModifyGridSite( self, gridSiteName, gridTier )
    '''
    res = self.rsDB.addOrModifyGridSite()
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.addOrModifyGridSite( 'AUVERGRID' )
    self.assertNotEqual( res.has_key( 'Value' ), True )          
    res = self.rsDB.addOrModifyGridSite( 'AUVERGRID', 'T2', 'eGGs' )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    
    initArgs = [ None, None ]
    sol      = [
                [ 'AUVERGRID', 'T2' ],
                [ 'INFN-T1', 'T1' ]
               ]
    
    #Test first parameter ( gridSiteName )
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'AUVERGRID'
    res = self.rsDB.addOrModifySite( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 0 ] = [ 'AUVERGRID' ]
    res = self.rsDB.addOrModifySite( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 0 ] = 'eGGs'
    res = self.rsDB.addOrModifySite( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 0 ] = [ 'eGGs' ]
    res = self.rsDB.addOrModifySite( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 0 ] = [ 'AUVERGRID', 'eGGs' ]
    res = self.rsDB.addOrModifySite( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 0 ] = [ None ]
    res = self.rsDB.addOrModifySite( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 0 ] = [ 'AUVERGRID', 'eGGs', None ]
    res = self.rsDB.addOrModifySite( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )

    #Test second parameter ( gridTier )
    modArgs = initArgs[:]
    modArgs[ 1 ] = 'T2'
    res = self.rsDB.addOrModifySite( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 1 ] = [ 'T2' ]
    res = self.rsDB.addOrModifySite( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.addOrModifySite( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 1 ] = [ 'eGGs' ]
    res = self.rsDB.addOrModifySite( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 1 ] = [ 'T2', 'eGGs' ]
    res = self.rsDB.addOrModifySite( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 1 ] = [ None ]
    res = self.rsDB.addOrModifySite( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 1 ] = [ 'T2', 'eGGs', None ]
    res = self.rsDB.addOrModifySite( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    
    '''
      Mixed tests
    '''
    
    '''
      Kwargs tests...
    '''
    
    res = self.rsDB.addOrModifyGridSite( 'AUVERGRID', '' )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    
    res = self.rsDB.addOrModifyGridSite( 'AUVERGRID', 'T2' )
    self.assertEqual( res, S_OK() )
    res = self.rsDB.addOrModifyGridSite( 'AUVERGRID', 'T2' )
    self.assertEqual( res, S_OK() )
    res = self.rsDB.addOrModifyGridSite( 'INFN-T1', 'T2' )
    self.assertEqual( res, S_OK() )
    res = self.rsDB.addOrModifyGridSite( 'INFN-T1', 'T1' )
    self.assertEqual( res, S_OK() )                   
                
  def test_GridSites02_getGridSites( self ):
    '''
    getGridSites( self, gridSiteName, gridTier, **kwargs )
    '''
    res = self.rsDB.getGridSites()
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.getGridSites( 'AUVERGRID' )
    self.assertNotEqual( res.has_key( 'Value' ), True )          
    res = self.rsDB.getGridSites( 'AUVERGRID', 'T2', 'eGGs' )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    
    initArgs = [ None, None ]
    sol      = [
                [ 'AUVERGRID', 'T2' ],
                [ 'INFN-T1', 'T1' ]
               ]
    
    res = self.rsDB.getGridSites( *tuple( initArgs ) )
    self.assertEqual( res[ 'Value' ], sol )
    
    #Test first parameter ( gridSiteName )
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'AUVERGRID'
    res = self.rsDB.getGridSites( *tuple( modArgs ) )
    self.assertEqual( res[ 'Value' ], [ sol[ 0 ] ] )
    modArgs[ 0 ] = [ 'AUVERGRID' ]
    res = self.rsDB.getGridSites( *tuple( modArgs ) )
    self.assertEqual( res[ 'Value' ], [ sol[ 0 ] ] )
    modArgs[ 0 ] = 'eGGs'
    res = self.rsDB.getGridSites( *tuple( modArgs ) )
    self.assertEqual( res[ 'Value' ], [] )
    modArgs[ 0 ] = [ 'eGGs' ]
    res = self.rsDB.getGridSites( *tuple( modArgs ) )
    self.assertEqual( res[ 'Value' ], [] )
    modArgs[ 0 ] = [ 'AUVERGRID', 'eGGs' ]
    res = self.rsDB.getGridSites( *tuple( modArgs ) )
    self.assertEqual( res[ 'Value' ], [ sol[ 0 ] ] )
    modArgs[ 0 ] = [ 'AUVERGRID', 'INFN-T1' ]
    res = self.rsDB.getGridSites( *tuple( modArgs ) )
    self.assertEqual( res[ 'Value' ], sol )
    modArgs[ 0 ] = [ None ]
    res = self.rsDB.getGridSites( *tuple( modArgs ) )
    self.assertEqual( res[ 'Value' ], [ sol[ 0 ] ] )
    modArgs[ 0 ] = [ 'AUVERGRID', 'eGGs', None ]
    res = self.rsDB.getGridSites( *tuple( modArgs ) )
    self.assertEqual( res[ 'Value' ], [ sol[ 0 ] ] )
    modArgs[ 0 ] = [ 'AUVERGRID', 'eGGs', None, 'INFN-T1' ]
    res = self.rsDB.getGridSites( *tuple( modArgs ) )
    self.assertEqual( res[ 'Value' ], sol )

    #Test second parameter ( gridTier )
    modArgs = initArgs[:]
    modArgs[ 1 ] = 'T2'
    res = self.rsDB.getGridSites( *tuple( modArgs ) )
    self.assertEqual( res[ 'Value' ], [ sol[ 0 ] ] )
    modArgs[ 1 ] = [ 'T2' ]
    res = self.rsDB.getGridSites( *tuple( modArgs ) )
    self.assertEqual( res[ 'Value' ], [ sol[ 0 ] ] )
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.getGridSites( *tuple( modArgs ) )
    self.assertEqual( res[ 'Value' ], [] )
    modArgs[ 1 ] = [ 'eGGs' ]
    res = self.rsDB.getGridSites( *tuple( modArgs ) )
    self.assertEqual( res[ 'Value' ], [] )
    modArgs[ 1 ] = [ 'T2', 'eGGs' ]
    res = self.rsDB.getGridSites( *tuple( modArgs ) )
    self.assertEqual( res[ 'Value' ], [] )
    modArgs[ 1 ] = [ 'T2', 'T1' ]
    res = self.rsDB.getGridSites( *tuple( modArgs ) )
    self.assertEqual( res[ 'Value' ], sol )
    modArgs[ 1 ] = [ None ]
    res = self.rsDB.getGridSites( *tuple( modArgs ) )
    self.assertEqual( res[ 'Value' ], [] )
    modArgs[ 1 ] = [ 'T2', 'eGGs', None ]
    res = self.rsDB.getGridSites( *tuple( modArgs ) )
    self.assertEqual( res[ 'Value' ], [ sol[ 0 ] ] )
    modArgs[ 1 ] = [ 'T2', 'eGGs', None, 'T1' ]
    res = self.rsDB.getGridSites( *tuple( modArgs ) )
    self.assertEqual( res[ 'Value' ], sol )
    
    '''
      Mixed tests
    '''
    
    '''
      Kwargs tests...
    '''
    
    res = self.rsDB.getGridSites( ['AUVERGRID','INFN-T1'], None )
    self.assertEqual( res[ 'Value' ], [['AUVERGRID', 'T2'],['INFN-T1', 'T1']] )
    res = self.rsDB.getGridSites( 'AUVERGRID', 'T2' )
    self.assertEqual( res[ 'Value' ], [['AUVERGRID', 'T2']] )

    kwargs = {}
    res = self.rsDB.getGridSites( None, None, **kwargs )  
    self.assertEqual( res[ 'Value' ], [['AUVERGRID', 'T2'], ['INFN-T1', 'T1']] )
    kwargs = { 'columns' : [ 'eGGS' ] }
    res = self.rsDB.getGridSites( None, None, **kwargs )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    kwargs = { 'columns' : 'GridSiteName' }
    res = self.rsDB.getGridSites( None, None, **kwargs )
    self.assertEqual( res[ 'Value' ], [['AUVERGRID'], ['INFN-T1']] ) 
    kwargs = { 'columns' : [ 'GridTier' ] }
    res = self.rsDB.getGridSites( None, None, **kwargs )
    self.assertEqual( res[ 'Value' ], [['T2'], ['T1']] )
    kwargs = { 'columns' : [ 'GridSiteName', 'GridTier' ] }
    res = self.rsDB.getGridSites( None, None, **kwargs )
    self.assertEqual( res[ 'Value' ], [['AUVERGRID', 'T2'], ['INFN-T1', 'T1']] )
    res = self.rsDB.getGridSites( 'AUVERGRID', 'T2', **kwargs )
    self.assertEqual( res[ 'Value' ], [['AUVERGRID', 'T2']] )
    kwargs = { 'columns' : [ 'GridTier', 'GridSiteName' ] }
    res = self.rsDB.getGridSites( None, None, **kwargs )
    self.assertEqual( res[ 'Value' ], [['T2','AUVERGRID'], ['T1','INFN-T1']] )
   
  def test_GridSites03_deleteGridSites( self ):
    '''
    deleteGridSites( self, gridSiteName )
    '''
    res = self.rsDB.deleteGridSites()
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.deleteGridSites( None )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.deleteGridSites( None, None )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    
    res = self.rsDB.deleteGridSites( 'eGGs' )
    self.assertEqual( res, S_OK() )
    res = self.rsDB.deleteGridSites( 'AUVERGRID' ) 
    self.assertEqual( res, S_OK() )
    res = self.rsDB.deleteGridSites( [ 'INFN-T1', 'eGGs' ] ) 
    self.assertEqual( res, S_OK() )   
   
  def test_GridSites04_flow( self ):
  
    res = self.rsDB.addOrModifyGridSite( 'AUVERGRID', 'T2' )
    self.assertEqual( res, S_OK() )
    res = self.rsDB.getGridSites( 'AUVERGRID', None )
    self.assertEqual( res[ 'Value' ], [[ 'AUVERGRID', 'T2' ]] )
    res = self.rsDB.addOrModifyGridSite( 'AUVERGRID', 'T1' )
    self.assertEqual( res, S_OK() )
    res = self.rsDB.getGridSites( 'AUVERGRID', None )
    self.assertEqual( res[ 'Value' ], [[ 'AUVERGRID', 'T1' ]] ) 
    res = self.rsDB.deleteGridSites( 'AUVERGRID' ) 
    self.assertEqual( res, S_OK() )
    res = self.rsDB.getGridSites( 'AUVERGRID', None )
    self.assertEqual( res[ 'Value' ], [] )
   
class Test_Sites( Test_ResourceStatusDB ):
  
  def test_Sites01_addOrModifySite( self ):
    '''
    addOrModifySite( self, siteName, siteType, gridSiteName )
    '''
    res = self.rsDB.addOrModifySite()
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.addOrModifySite( 'LCG.CERN.ch' )
    self.assertNotEqual( res.has_key( 'Value' ), True )          
    res = self.rsDB.addOrModifySite( 'LCG.CERN.ch', 'T0' )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.addOrModifySite( 'LCG.CERN.ch', 'T0', 'CERN-PROD', None )
    
    initArgs = [ None, None, None ]
    sol      = [
                [ 'LCG.CERN.ch', 'T0', 'CERN-PROD' ],
                [ 'LCG.CNAF.it', 'T1', 'INFN-T1' ]
               ]
    
    #Test first parameter ( siteName )
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'LCG.CERN.ch'
    res = self.rsDB.addOrModifySite( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 0 ] = [ 'LCG.CERN.ch' ]
    res = self.rsDB.addOrModifySite( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 0 ] = 'eGGs'
    res = self.rsDB.addOrModifySite( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 0 ] = [ 'eGGs' ]
    res = self.rsDB.addOrModifySite( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'eGGs' ]
    res = self.rsDB.addOrModifySite( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 0 ] = [ None ]
    res = self.rsDB.addOrModifySite( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'eGGs', None ]
    res = self.rsDB.addOrModifySite( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )

    #Test second parameter ( siteType )
    modArgs = initArgs[:]
    modArgs[ 1 ] = 'T0'
    res = self.rsDB.addOrModifySite( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 1 ] = [ 'T0' ]
    res = self.rsDB.addOrModifySite( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.addOrModifySite( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 1 ] = [ 'eGGs' ]
    res = self.rsDB.addOrModifySite( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 1 ] = [ 'T0', 'eGGs' ]
    res = self.rsDB.addOrModifySite( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 1 ] = [ None ]
    res = self.rsDB.addOrModifySite( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 1 ] = [ 'T0', 'eGGs', None ]
    res = self.rsDB.addOrModifySite( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )    

    #Test third parameter ( siteType )
    modArgs = initArgs[:]
    modArgs[ 2 ] = 'CERN-PROD'
    res = self.rsDB.addOrModifySite( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 2 ] = [ 'CERN-PROD' ]
    res = self.rsDB.addOrModifySite( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 2 ] = 'eGGs'
    res = self.rsDB.addOrModifySite( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 2 ] = [ 'eGGs' ]
    res = self.rsDB.addOrModifySite( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 2 ] = [ 'CERN-PROD', 'eGGs' ]
    res = self.rsDB.addOrModifySite( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 2 ] = [ None ]
    res = self.rsDB.addOrModifySite( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 2 ] = [ 'CERN-PROD', 'eGGs', None ]
    res = self.rsDB.addOrModifySite( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )     
    
    '''
      Mixed tests
    '''
    
    '''
      Kwargs tests...
    '''
    
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.addOrModifySite( None, None, None )
    self.assertNotEqual( res.has_key( 'Value' ), True )   
    res = self.rsDB.addOrModifySite( 'LCG.CERN.ch', None, None )
    self.assertNotEqual( res.has_key( 'Value' ), True )   
    res = self.rsDB.addOrModifySite( 'LCG.CERN.ch', None, 'CERN-PROD' )
    self.assertNotEqual( res.has_key( 'Value' ), True )   
    res = self.rsDB.addOrModifySite( 'LCG.CERN.ch', 'eGGs', 'CERN-PROD' )
    self.assertNotEqual( res.has_key( 'Value' ), True )   
    # This one looks correct, it is correct, but GridSites table is empty !!
    res = self.rsDB.addOrModifySite( 'LCG.CERN.ch', 'T0', 'CERN-PROD' )
    self.assertNotEqual( res.has_key( 'Value' ), True )   
    
    self.rsDB.addOrModifyGridSite( 'CERN-PROD', 'T0')
    self.rsDB.addOrModifyGridSite( 'INFN-T1',   'T1')
    
    res = self.rsDB.addOrModifySite( 'LCG.CERN.ch', 'T0', 'CERN-PROD' )
    self.assertEqual( res, S_OK() )   
    res = self.rsDB.addOrModifySite( 'LCG.CERN.ch', 'T0', 'CERN-PROD' )
    self.assertEqual( res, S_OK() )   
    res = self.rsDB.addOrModifySite( 'LCG.CERN.ch', 'T1', 'CERN-PROD' )
    self.assertEqual( res, S_OK() )   
    res = self.rsDB.addOrModifySite( 'LCG.CERN.ch', 'T1', 'INFN-T1' )
    self.assertEqual( res, S_OK() )   
    res = self.rsDB.addOrModifySite( 'LCG.CERN.ch', 'T0', 'CERN-PROD' )
    self.assertEqual( res, S_OK() )   
    res = self.rsDB.addOrModifySite( 'LCG.CNAF.it', 'T1', 'INFN-T1' )
    self.assertEqual( res, S_OK() )   
           
  def test_Sites02_setSiteStatus( self ):
    '''
    setSiteStatus( self, siteName, statusType, status, reason, dateCreated, 
                   dateEffective, dateEnd, lastCheckTime, tokenOwner, 
                   tokenExpiration ) 
    '''
    dNow = datetime.now()
    
    res = self.rsDB.setSiteStatus()
    self.assertNotEqual( res.has_key( 'Value' ), True )   
    res = self.rsDB.setSiteStatus( 'LCG.CERN.ch' )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.setSiteStatus( 'LCG.CERN.ch', '' )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.setSiteStatus( 'LCG.CERN.ch', '', 'eGGs reason' )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.setSiteStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.setSiteStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.setSiteStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow, dNow )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.setSiteStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow, dNow, dNow )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.setSiteStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow, dNow, dNow, 'token' )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.setSiteStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow, dNow, dNow, 'token', dNow, {}, dNow )
    
    initArgs = [ None, None, None, None, None, None, None, None, None, None ]
    
    dSol= datetime( 9999, 12, 11, 10, 9, 8 )
    sol = [
           ['LCG.CERN.ch', '', 'Active', 'Init', dSol, dSol, dSol, dSol, 'RS_SVC', dSol], 
           ['LCG.CNAF.it', '', 'Banned', 'Init', dSol, dSol, dSol, dSol, 'RS_SVC', dSol]
          ]
    
    res = self.rsDB.setSiteStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    
    #Test first parameter ( siteName )
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'LCG.CERN.ch'
    res = self.rsDB.setSiteStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 0 ] = [ 'LCG.CERN.ch' ]
    res = self.rsDB.setSiteStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 0 ] = 'eGGs'
    res = self.rsDB.setSiteStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 0 ] = [ 'eGGs' ]
    res = self.rsDB.setSiteStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'eGGs' ]
    res = self.rsDB.setSiteStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 0 ] = [ None ]
    res = self.rsDB.setSiteStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'eGGs', None ]
    res = self.rsDB.setSiteStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    
    #Test second parameter ( statusType )
    modArgs[ 1 ] = ''
    res = self.rsDB.setSiteStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 1 ] = [ '' ]
    res = self.rsDB.setSiteStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.setSiteStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 1 ] = [ 'eGGs' ]
    res = self.rsDB.setSiteStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 1 ] = [ '', 'eGGs' ]
    res = self.rsDB.setSiteStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 1 ] = [ None ]
    res = self.rsDB.setSiteStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 1 ] = [ '', 'eGGs', None ]
    res = self.rsDB.setSiteStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )

    #Test third parameter ( status )
    modArgs[ 2 ] = 'Active'
    res = self.rsDB.setSiteStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 2 ] = [ 'Active' ]
    res = self.rsDB.setSiteStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 2 ] = 'eGGs'
    res = self.rsDB.setSiteStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 2 ] = [ 'eGGs' ]
    res = self.rsDB.setSiteStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 2 ] = [ 'Active', 'eGGs' ]
    res = self.rsDB.setSiteStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 2 ] = [ None ]
    res = self.rsDB.setSiteStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 2 ] = [ 'Active', 'eGGs', None ]
    res = self.rsDB.setSiteStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    
    #Test forth parameter ( reason )
    modArgs = initArgs[:]
    modArgs[ 3 ] = 'Init'
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 3 ] = [ 'Init' ]    
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True ) 
    modArgs[ 3 ] = 'eGGs'
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 3 ] = [ 'eGGs' ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 3 ] = [ 'Init', 'eGGs' ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 3 ] = [ None ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )         
    modArgs[ 3 ] = [ 'Init', 'eGGs', None ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )  

    #Test fifth parameter ( dateCreated )
    modArgs = initArgs[:]
    modArgs[ 4 ] = dSol
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True ) 
    modArgs[ 4 ] = [ dSol ]    
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )     
    modArgs[ 4 ] = dNow
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True ) 
    modArgs[ 4 ] = [ dNow ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True ) 
    modArgs[ 4 ] = [ dSol, dNow ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True ) 
    modArgs[ 4 ] = [ None ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 4 ] = [ dSol, dNow, None ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True ) 
            
    #Test sixth parameter ( dateEffective ) 
    modArgs = initArgs[:]
    modArgs[ 5 ] = dSol
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )    
    modArgs[ 5 ] = [ dSol ]    
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )         
    modArgs[ 5 ] = dNow
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )  
    modArgs[ 5 ] = [ dNow ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True ) 
    modArgs[ 5 ] = [ dSol, dNow ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )     
    modArgs[ 5 ] = [ None ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 5 ] = [ dSol, dNow, None ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )     

    #Test seventh parameter ( dateEnd )
    modArgs = initArgs[:]
    modArgs[ 6 ] = dSol
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )   
    modArgs[ 6 ] = [ dSol ]    
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )        
    modArgs[ 6 ] = dNow
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True ) 
    modArgs[ 6 ] = [ dNow ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 6 ] = [ dSol, dNow ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True ) 
    modArgs[ 6 ] = [ None ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 6 ] = [ dSol, dNow, None ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True ) 
            
    #Test eighth parameter ( lastCheckTime )
    modArgs = initArgs[:]
    modArgs[ 7 ] = dSol
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )   
    modArgs[ 7 ] = [ dSol ]    
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )        
    modArgs[ 7 ] = dNow
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True ) 
    modArgs[ 7 ] = [ dNow ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 7 ] = [ dSol, dNow ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 7 ] = [ None ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 7 ] = [ dSol, dNow, None ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    
    #Test ninth parameter ( tokenOwner )
    modArgs = initArgs[:]
    modArgs[ 8 ] = 'RS_SVC'
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 8 ] = [ 'RS_SVC' ]   
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )   
    modArgs[ 8 ] = 'eGGs'   
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 8 ] = [ 'eGGs' ]   
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs' ]   
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 8 ] = [ None ]   
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs', None ]   
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    
    #Test tenth parameter ( tokenExpiration )
    modArgs = initArgs[:]
    modArgs[ 9 ] = dSol
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )   
    modArgs[ 9 ] = [ dSol ]    
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )        
    modArgs[ 9 ] = dNow
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True ) 
    modArgs[ 9 ] = [ dNow ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 9 ] = [ dSol, dNow ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 9 ] = [ None ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )    
    modArgs[ 9 ] = [ dSol, dNow, None ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    
    ##############
    
    '''
      Mixed tests
    '''
    
    '''
      Kwargs tests...
    '''
    
    res = self.rsDB.setSiteStatus( 'LCG.CERN.ch', '', 'Active', None, None,
                                   None,None,None,None,None )
    self.assertEqual( res, S_OK() )
    res = self.rsDB.setSiteStatus( 'LCG.CERN.ch', '', 'Active', None, None,
                                   None, None, None, None, None )
    self.assertEqual( res, S_OK() )
    
    res = self.rsDB.setSiteStatus( ['LCG.CERN.ch'], '', 'Active', None, None,
                                   None, None, None, None, None )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.setSiteStatus( 'LCG.CERN.ch', [''], 'Active', None, None,
                                   None, None, None, None, None )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.setSiteStatus( 'LCG.CERN.ch', '', ['Active'], None, None,
                                   None, None, None, None, None )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.setSiteStatus( 'LCG.CERN.ch', '', 'Active', None, 'eGGs',
                                   None, None, None, None, None )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.setSiteStatus( 'LCG.CERN.ch', '', 'Active', None, None,
                                   'eGGs', None, None, None, None )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.setSiteStatus( 'LCG.CERN.ch', '', 'Active', None, None,
                                   None, 'eGGs', None, None, None )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.setSiteStatus( 'LCG.CERN.ch', '', 'Active', None, None,
                                   None, None, 'eGGs', None, None )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.setSiteStatus( 'LCG.CERN.ch', '', 'Active', None, None,
                                   None, None, None, None, 'eGGs' )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    
    now = datetime.now()
    
    res = self.rsDB.setSiteStatus( 'LCG.CERN.ch', '', 'Active', None, now,
                                   now, now, now, None, now )
    self.assertEqual( res, S_OK() )
    res = self.rsDB.setSiteStatus( 'eGGs', '', 'Active', None, now,
                                   now, now, now, None, now )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.setSiteStatus( 'LCG.CERN.ch', 'eGGs', 'Active', None, now,
                                   now, now, now, None, now )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.setSiteStatus( 'LCG.CERN.ch', '', 'eGGs', None, now,
                                   now, now, now, None, now )
    self.assertNotEqual( res.has_key( 'Value' ), True )

  def test_Sites03_setSiteScheduledStatus( self ):
    '''
    setSiteScheduledStatus( self, siteName, statusType, status, reason, dateCreated, 
                            dateEffective, dateEnd, lastCheckTime, tokenOwner, 
                            tokenExpiration )
    '''   
    dNow = datetime.now()
    
    res = self.rsDB.setSiteScheduledStatus()
    self.assertNotEqual( res.has_key( 'Value' ), True )   
    res = self.rsDB.setSiteScheduledStatus( 'LCG.CERN.ch' )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.setSiteScheduledStatus( 'LCG.CERN.ch', '' )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.setSiteScheduledStatus( 'LCG.CERN.ch', '', 'eGGs reason' )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.setSiteScheduledStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.setSiteScheduledStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.setSiteScheduledStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow, dNow )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.setSiteScheduledStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow, dNow, dNow )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.setSiteScheduledStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow, dNow, dNow, 'token' )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.setSiteScheduledStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow, dNow, dNow, 'token', dNow, {}, dNow )
    
    initArgs = [ None, None, None, None, None, None, None, None, None, None ]
    
    dSol= datetime( 9999, 12, 11, 10, 9, 8 )
    sol = [
           ['LCG.CERN.ch', '', 'Active', 'Init', dSol, dSol, dSol, dSol, 'RS_SVC', dSol], 
           ['LCG.CNAF.it', '', 'Banned', 'Init', dSol, dSol, dSol, dSol, 'RS_SVC', dSol]
          ]
    
    res = self.rsDB.setSiteScheduledStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    
    #Test first parameter ( siteName )
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'LCG.CERN.ch'
    res = self.rsDB.setSiteScheduledStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 0 ] = [ 'LCG.CERN.ch' ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 0 ] = 'eGGs'
    res = self.rsDB.setSiteScheduledStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 0 ] = [ 'eGGs' ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'eGGs' ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 0 ] = [ None ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'eGGs', None ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    
    #Test second parameter ( statusType )
    modArgs[ 1 ] = ''
    res = self.rsDB.setSiteScheduledStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 1 ] = [ '' ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.setSiteScheduledStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 1 ] = [ 'eGGs' ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 1 ] = [ '', 'eGGs' ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 1 ] = [ None ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 1 ] = [ '', 'eGGs', None ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )

    #Test third parameter ( status )
    modArgs[ 2 ] = 'Active'
    res = self.rsDB.setSiteScheduledStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 2 ] = [ 'Active' ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 2 ] = 'eGGs'
    res = self.rsDB.setSiteScheduledStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 2 ] = [ 'eGGs' ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 2 ] = [ 'Active', 'eGGs' ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 2 ] = [ None ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 2 ] = [ 'Active', 'eGGs', None ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    
    #Test forth parameter ( reason )
    modArgs = initArgs[:]
    modArgs[ 3 ] = 'Init'
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 3 ] = [ 'Init' ]    
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True ) 
    modArgs[ 3 ] = 'eGGs'
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 3 ] = [ 'eGGs' ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 3 ] = [ 'Init', 'eGGs' ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 3 ] = [ None ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )         
    modArgs[ 3 ] = [ 'Init', 'eGGs', None ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )  

    #Test fifth parameter ( dateCreated )
    modArgs = initArgs[:]
    modArgs[ 4 ] = dSol
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True ) 
    modArgs[ 4 ] = [ dSol ]    
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )     
    modArgs[ 4 ] = dNow
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True ) 
    modArgs[ 4 ] = [ dNow ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True ) 
    modArgs[ 4 ] = [ dSol, dNow ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True ) 
    modArgs[ 4 ] = [ None ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 4 ] = [ dSol, dNow, None ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True ) 
            
    #Test sixth parameter ( dateEffective ) 
    modArgs = initArgs[:]
    modArgs[ 5 ] = dSol
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )    
    modArgs[ 5 ] = [ dSol ]    
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )         
    modArgs[ 5 ] = dNow
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )  
    modArgs[ 5 ] = [ dNow ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True ) 
    modArgs[ 5 ] = [ dSol, dNow ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )     
    modArgs[ 5 ] = [ None ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 5 ] = [ dSol, dNow, None ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )     

    #Test seventh parameter ( dateEnd )
    modArgs = initArgs[:]
    modArgs[ 6 ] = dSol
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )   
    modArgs[ 6 ] = [ dSol ]    
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )        
    modArgs[ 6 ] = dNow
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True ) 
    modArgs[ 6 ] = [ dNow ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 6 ] = [ dSol, dNow ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True ) 
    modArgs[ 6 ] = [ None ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 6 ] = [ dSol, dNow, None ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True ) 
            
    #Test eighth parameter ( lastCheckTime )
    modArgs = initArgs[:]
    modArgs[ 7 ] = dSol
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )   
    modArgs[ 7 ] = [ dSol ]    
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )        
    modArgs[ 7 ] = dNow
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True ) 
    modArgs[ 7 ] = [ dNow ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 7 ] = [ dSol, dNow ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 7 ] = [ None ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 7 ] = [ dSol, dNow, None ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    
    #Test ninth parameter ( tokenOwner )
    modArgs = initArgs[:]
    modArgs[ 8 ] = 'RS_SVC'
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 8 ] = [ 'RS_SVC' ]   
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )   
    modArgs[ 8 ] = 'eGGs'   
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 8 ] = [ 'eGGs' ]   
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs' ]   
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 8 ] = [ None ]   
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs', None ]   
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    
    #Test tenth parameter ( tokenExpiration )
    modArgs = initArgs[:]
    modArgs[ 9 ] = dSol
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )   
    modArgs[ 9 ] = [ dSol ]    
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )        
    modArgs[ 9 ] = dNow
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True ) 
    modArgs[ 9 ] = [ dNow ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 9 ] = [ dSol, dNow ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 9 ] = [ None ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )    
    modArgs[ 9 ] = [ dSol, dNow, None ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    
    '''
      Mixed tests
    '''
    
    '''
      Kwargs tests...
    '''
    
    now = datetime.now()
    
    res = self.rsDB.setSiteScheduledStatus( 'LCG.CERN.ch', '', 'Active', None, now,
                                            now, now, now, None, now )
    self.assertEqual( res, S_OK() )
    res = self.rsDB.setSiteScheduledStatus( 'eGGs', '', 'Active', None, now,
                                            now, now, now, None, now )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.setSiteScheduledStatus( 'LCG.CERN.ch', 'eGGs', 'Active', None, now,
                                            now, now, now, None, now )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.setSiteScheduledStatus( 'LCG.CERN.ch', '', 'eGGs', None, now,
                                            now, now, now, None, now )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    
  def test_Sites04_updateSiteStatus( self ):
    '''
    updateSiteStatus( self, siteName, statusType, status, reason, dateCreated, 
                      dateEffective, dateEnd, lastCheckTime, tokenOwner, 
                      tokenExpiration )
    '''
    dNow = datetime.now()
    
    res = self.rsDB.updateSiteStatus()
    self.assertNotEqual( res.has_key( 'Value' ), True )   
    res = self.rsDB.updateSiteStatus( 'LCG.CERN.ch' )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.updateSiteStatus( 'LCG.CERN.ch', '' )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.updateSiteStatus( 'LCG.CERN.ch', '', 'eGGs reason' )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.updateSiteStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.updateSiteStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.updateSiteStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow, dNow )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.updateSiteStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow, dNow, dNow )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.updateSiteStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow, dNow, dNow, 'token' )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.updateSiteStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow, dNow, dNow, 'token', dNow, {}, dNow )
    
    initArgs = [ None, None, None, None, None, None, None, None, None, None ]
    
    dSol= datetime( 9999, 12, 11, 10, 9, 8 )
    sol = [
           ['LCG.CERN.ch', '', 'Active', 'Init', dSol, dSol, dSol, dSol, 'RS_SVC', dSol], 
           ['LCG.CNAF.it', '', 'Banned', 'Init', dSol, dSol, dSol, dSol, 'RS_SVC', dSol]
          ]
    
    res = self.rsDB.updateSiteStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    
    '''
      Param tests
    '''    
    #Test first parameter ( siteName )
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'LCG.CERN.ch'
    res = self.rsDB.updateSiteStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 0 ] = [ 'LCG.CERN.ch' ]
    res = self.rsDB.updateSiteStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 0 ] = 'eGGs'
    res = self.rsDB.updateSiteStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 0 ] = [ 'eGGs' ]
    res = self.rsDB.updateSiteStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'eGGs' ]
    res = self.rsDB.updateSiteStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 0 ] = [ None ]
    res = self.rsDB.updateSiteStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'eGGs', None ]
    res = self.rsDB.updateSiteStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    
    #Test second parameter ( statusType )
    modArgs[ 1 ] = ''
    res = self.rsDB.updateSiteStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 1 ] = [ '' ]
    res = self.rsDB.updateSiteStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.updateSiteStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 1 ] = [ 'eGGs' ]
    res = self.rsDB.updateSiteStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 1 ] = [ '', 'eGGs' ]
    res = self.rsDB.updateSiteStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 1 ] = [ None ]
    res = self.rsDB.updateSiteStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 1 ] = [ '', 'eGGs', None ]
    res = self.rsDB.updateSiteStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )

    #Test third parameter ( status )
    modArgs[ 2 ] = 'Active'
    res = self.rsDB.updateSiteStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 2 ] = [ 'Active' ]
    res = self.rsDB.updateSiteStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 2 ] = 'eGGs'
    res = self.rsDB.updateSiteStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 2 ] = [ 'eGGs' ]
    res = self.rsDB.updateSiteStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 2 ] = [ 'Active', 'eGGs' ]
    res = self.rsDB.updateSiteStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 2 ] = [ None ]
    res = self.rsDB.updateSiteStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 2 ] = [ 'Active', 'eGGs', None ]
    res = self.rsDB.updateSiteStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    
    #Test forth parameter ( reason )
    modArgs = initArgs[:]
    modArgs[ 3 ] = 'Init'
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 3 ] = [ 'Init' ]    
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True ) 
    modArgs[ 3 ] = 'eGGs'
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 3 ] = [ 'eGGs' ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 3 ] = [ 'Init', 'eGGs' ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 3 ] = [ None ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )         
    modArgs[ 3 ] = [ 'Init', 'eGGs', None ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )  

    #Test fifth parameter ( dateCreated )
    modArgs = initArgs[:]
    modArgs[ 4 ] = dSol
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True ) 
    modArgs[ 4 ] = [ dSol ]    
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )     
    modArgs[ 4 ] = dNow
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True ) 
    modArgs[ 4 ] = [ dNow ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True ) 
    modArgs[ 4 ] = [ dSol, dNow ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True ) 
    modArgs[ 4 ] = [ None ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 4 ] = [ dSol, dNow, None ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True ) 
            
    #Test sixth parameter ( dateEffective ) 
    modArgs = initArgs[:]
    modArgs[ 5 ] = dSol
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )    
    modArgs[ 5 ] = [ dSol ]    
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )         
    modArgs[ 5 ] = dNow
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )  
    modArgs[ 5 ] = [ dNow ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True ) 
    modArgs[ 5 ] = [ dSol, dNow ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )     
    modArgs[ 5 ] = [ None ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 5 ] = [ dSol, dNow, None ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )     

    #Test seventh parameter ( dateEnd )
    modArgs = initArgs[:]
    modArgs[ 6 ] = dSol
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )   
    modArgs[ 6 ] = [ dSol ]    
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )        
    modArgs[ 6 ] = dNow
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True ) 
    modArgs[ 6 ] = [ dNow ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 6 ] = [ dSol, dNow ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True ) 
    modArgs[ 6 ] = [ None ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 6 ] = [ dSol, dNow, None ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True ) 
            
    #Test eighth parameter ( lastCheckTime )
    modArgs = initArgs[:]
    modArgs[ 7 ] = dSol
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )   
    modArgs[ 7 ] = [ dSol ]    
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )        
    modArgs[ 7 ] = dNow
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True ) 
    modArgs[ 7 ] = [ dNow ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 7 ] = [ dSol, dNow ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 7 ] = [ None ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 7 ] = [ dSol, dNow, None ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    
    #Test ninth parameter ( tokenOwner )
    modArgs = initArgs[:]
    modArgs[ 8 ] = 'RS_SVC'
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 8 ] = [ 'RS_SVC' ]   
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )   
    modArgs[ 8 ] = 'eGGs'   
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 8 ] = [ 'eGGs' ]   
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs' ]   
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 8 ] = [ None ]   
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs', None ]   
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    
    #Test tenth parameter ( tokenExpiration )
    modArgs = initArgs[:]
    modArgs[ 9 ] = dSol
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )   
    modArgs[ 9 ] = [ dSol ]    
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )        
    modArgs[ 9 ] = dNow
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True ) 
    modArgs[ 9 ] = [ dNow ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 9 ] = [ dSol, dNow ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 9 ] = [ None ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )    
    modArgs[ 9 ] = [ dSol, dNow, None ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    
    '''
      Mixed tests
    '''
    
    '''
      Kwargs tests... TO BE IMPROVED
    '''
    
    
    res = self.rsDB.updateSiteStatus( 'LCG.CERN.ch', '', 'Active', None, None,
                                       None,None,None,None,None )
    self.assertEqual( res, S_OK() )
    
    res = self.rsDB.updateSiteStatus( ['LCG.CERN.ch'], '', 'Active', None, None,
                                            None, None, None, None, None )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.updateSiteStatus( 'LCG.CERN.ch', [''], 'Active', None, None,
                                            None, None, None, None, None )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.updateSiteStatus( 'LCG.CERN.ch', '', ['Active'], None, None,
                                            None, None, None, None, None )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.updateSiteStatus( 'LCG.CERN.ch', '', 'Active', None, 'eGGs',
                                            None, None, None, None, None )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.updateSiteStatus( 'LCG.CERN.ch', '', 'Active', None, None,
                                            'eGGs', None, None, None, None )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.updateSiteStatus( 'LCG.CERN.ch', '', 'Active', None, None,
                                            None, 'eGGs', None, None, None )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.updateSiteStatus( 'LCG.CERN.ch', '', 'Active', None, None,
                                            None, None, 'eGGs', None, None )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.updateSiteStatus( 'LCG.CERN.ch', '', 'Active', None, None,
                                            None, None, None, None, 'eGGs' )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    
    now = datetime.now()
    
    res = self.rsDB.updateSiteStatus( 'LCG.CERN.ch', '', 'Active', None, now,
                                            now, now, now, None, now )
    self.assertEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.updateSiteStatus( 'eGGs', '', 'Active', None, now,
                                            now, now, now, None, now )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.updateSiteStatus( 'LCG.CERN.ch', 'eGGs', 'Active', None, now,
                                            now, now, now, None, now )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.updateSiteStatus( 'LCG.CERN.ch', '', 'eGGs', None, now,
                                       now, now, now, None, now )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    
    
    # Used for next rounds
    
    res = self.rsDB.setSiteStatus( *tuple( sol[ 0 ] ) )
    self.assertEqual( res, S_OK() ) 
    res = self.rsDB.setSiteStatus( *tuple( sol[ 1 ] ) )
    self.assertEqual( res, S_OK() )    
    
    
  def test_Sites05_getSites( self ):
    '''
    getSites( self, siteName, siteType, gridSiteName, **kwargs )
    '''
    res = self.rsDB.getSites()
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.getSites( 'LCG.CERN.ch' )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.getSites( 'LCG.CERN.ch', 'T0' )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.getSites( 'LCG.CERN.ch', 'T0', 'eGGs', 'eGGs' )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    
    initArgs = [ None, None, None ]
    sol      = [
                ['LCG.CERN.ch', 'T0', 'CERN-PROD'],
                ['LCG.CNAF.it', 'T1', 'INFN-T1'] 
               ]
    
    res = self.rsDB.getSites( *tuple( initArgs ) )
    self.assertEquals( res[ 'Value' ], sol )  
    
    '''
      Param tests
    '''    
    #Test first param ( siteName )
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'LCG.CERN.ch'
    res = self.rsDB.getSites( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [ sol[ 0 ] ] )  
    modArgs[ 0 ] = [ 'LCG.CNAF.it' ]
    res = self.rsDB.getSites( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [ sol[ 1 ] ] )  
    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'LCG.CNAF.it' ]
    res = self.rsDB.getSites( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )  
    modArgs[ 0 ] = 'eGGs'
    res = self.rsDB.getSites( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 0 ] = [ 'eGGs' ]
    res = self.rsDB.getSites( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'LCG.CNAF.it', 'eGGs' ]
    res = self.rsDB.getSites( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )  
    modArgs[ 0 ] = [ None ]
    res = self.rsDB.getSites( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'LCG.CNAF.it', 'eGGs', None ]
    res = self.rsDB.getSites( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )  

    #Test second param ( siteType )
    modArgs = initArgs[:]
    modArgs[ 1 ] = 'T0'
    res = self.rsDB.getSites( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [ sol[ 0 ] ] )
    modArgs[ 1 ] = [ 'T1' ]
    res = self.rsDB.getSites( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [ sol[ 1 ] ] )
    modArgs[ 1 ] = [ 'T0', 'T1' ]
    res = self.rsDB.getSites( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.getSites( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 1 ] = [ 'eGGs' ]
    res = self.rsDB.getSites( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 1 ] = [ 'T0', 'T1', 'eGGs' ]
    res = self.rsDB.getSites( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )
    modArgs[ 1 ] = [ None ]
    res = self.rsDB.getSites( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 1 ] = [ 'T0', 'T1', 'eGGs', None ]
    res = self.rsDB.getSites( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )

    #Test third param ( gridSiteName )
    modArgs = initArgs[:]
    modArgs[ 2 ] = 'CERN-PROD'
    res = self.rsDB.getSites( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [ sol[ 0 ] ] )
    modArgs[ 2 ] = [ 'INFN-T1' ]
    res = self.rsDB.getSites( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [ sol[ 1 ] ] )
    modArgs[ 2 ] = [ 'CERN-PROD', 'INFN-T1' ]
    res = self.rsDB.getSites( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )
    modArgs[ 2 ] = 'eGGs'
    res = self.rsDB.getSites( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 2 ] = [ 'eGGs' ]
    res = self.rsDB.getSites( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 2 ] = [ 'CERN-PROD', 'INFN-T1', 'eGGs' ]
    res = self.rsDB.getSites( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )
    modArgs[ 2 ] = [ None ]
    res = self.rsDB.getSites( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 2 ] = [ 'CERN-PROD', 'INFN-T1', 'eGGs', None ]
    res = self.rsDB.getSites( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )
    
    '''
      Mixed tests
    '''
    
    '''
      Kwargs tests... TO BE IMPROVED
    '''
    
    kwargs = {}
    res = self.rsDB.getSites( None, None, None, **kwargs )
    self.assertEqual( res[ 'Value' ], [['LCG.CERN.ch', 'T0', 'CERN-PROD'], ['LCG.CNAF.it', 'T1', 'INFN-T1']] )
    kwargs = { 'columns' : 'eGGs' }
    res = self.rsDB.getSites( None, None, None, **kwargs )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    kwargs = { 'columns' : 'SiteName' }
    res = self.rsDB.getSites( None, None, None, **kwargs )
    self.assertEqual( res[ 'Value' ], [['LCG.CERN.ch'], ['LCG.CNAF.it']] )
    res = self.rsDB.getSites( 'LCG.CERN.ch', None, None, **kwargs )
    self.assertEqual( res[ 'Value' ], [['LCG.CERN.ch']] )
    res = self.rsDB.getSites( [ 'LCG.CERN.ch' ], None, None, **kwargs )
    self.assertEqual( res[ 'Value' ], [['LCG.CERN.ch']] )
    res = self.rsDB.getSites( [ 'LCG.CERN.ch', 'LCG.CNAF.it' ], None, None, **kwargs )
    self.assertEqual( res[ 'Value' ], [['LCG.CERN.ch'], ['LCG.CNAF.it']] )
    res = self.rsDB.getSites( None, 'T0', None, **kwargs )
    self.assertEqual( res[ 'Value' ], [['LCG.CERN.ch']] )
    res = self.rsDB.getSites( None, None, 'CERN-PROD', **kwargs )
    self.assertEqual( res[ 'Value' ], [['LCG.CERN.ch']] )
    kwargs = { 'columns' : 'SiteType' }
    res = self.rsDB.getSites( None, None, None, **kwargs )
    self.assertEqual( res[ 'Value' ], [['T0'], ['T1']] )
    kwargs = { 'columns' : [ 'SiteName', 'SiteType' ] }
    res = self.rsDB.getSites( None, None, None, **kwargs )
    self.assertEqual( res[ 'Value' ], [['LCG.CERN.ch', 'T0'], ['LCG.CNAF.it', 'T1']] )
    kwargs = { 'columns' : [ 'SiteType', 'SiteName' ] }
    res = self.rsDB.getSites( None, None, None, **kwargs )
    self.assertEqual( res[ 'Value' ], [['T0','LCG.CERN.ch'], ['T1','LCG.CNAF.it']] )
    kwargs = { 'columns' : [ 'SiteName', 'SiteType', 'GridSiteName',  ] }
    res = self.rsDB.getSites( None, None, None, **kwargs )
    self.assertEqual( res[ 'Value' ], [['LCG.CERN.ch', 'T0', 'CERN-PROD'], ['LCG.CNAF.it', 'T1', 'INFN-T1']] )

  def test_Sites06_getSitesStatus( self ):
    '''
    getSitesStatus( self, siteName, statusType, status, reason, dateCreated, 
                    dateEffective, dateEnd, lastCheckTime, tokenOwner, 
                    tokenExpiration, **kwargs )
    '''

    dNow = datetime.now()
    
    res = self.rsDB.getSitesStatus()
    self.assertNotEqual( res.has_key( 'Value' ), True )   
    res = self.rsDB.getSitesStatus( 'LCG.CERN.ch' )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.getSitesStatus( 'LCG.CERN.ch', '' )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.getSitesStatus( 'LCG.CERN.ch', '', 'eGGs reason' )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.getSitesStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.getSitesStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.getSitesStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow, dNow )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.getSitesStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow, dNow, dNow )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.getSitesStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow, dNow, dNow, 'token' )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.getSitesStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow, dNow, dNow, 'token', dNow, {}, dNow )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    
    initArgs = [ None,None,None,None,None,None,None,None,None,None ]
    dSol     = datetime(9999, 12, 11, 10, 9, 8)
    sol      = [
                [1L, 'LCG.CERN.ch', '', 'Active', 'Init', dSol, dSol, dSol, dSol, 'RS_SVC', dSol], 
                [2L, 'LCG.CNAF.it', '', 'Banned', 'Init', dSol, dSol, dSol, dSol, 'RS_SVC', dSol]
               ]
        
    res = self.rsDB.getSitesStatus( *tuple( initArgs ) )
    self.assertEquals( res[ 'Value' ], sol )       
    
    '''
      Param tests
    '''    
    #Test first param ( siteName )
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'LCG.CERN.ch'
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [ sol[ 0 ] ] )
    modArgs[ 0 ] = [ 'LCG.CERN.ch' ]    
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [ sol[ 0 ] ] )        
    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'LCG.CNAF.it' ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )
    modArgs[ 0 ] = 'LCG.eGGs.xy'
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 0 ] = [ 'LCG.eGGs.xy' ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'LCG.eGGs.xy' ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [ sol[0] ] )
    modArgs[ 0 ] = [ None ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'LCG.eGGs.xy', None ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [ sol[0] ] )

    #Test second param ( statusType ) 
    modArgs = initArgs[:]
    modArgs[ 1 ] = ''
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )
    modArgs[ 1 ] = [ '' ]    
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )        
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 1 ] = [ 'eGGs' ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 1 ] = [ '', 'eGGs' ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )
    modArgs[ 1 ] = [ None ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 1 ] = [ '', 'eGGs', None ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )

    #Test third parameter ( status )
    modArgs = initArgs[:]
    modArgs[ 2 ] = 'Active'
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [ sol[0] ] )
    modArgs[ 2 ] = [ 'Active' ]    
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [ sol[0] ] )        
    modArgs[ 2 ] = 'Banned'
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [ sol[ 1 ] ] ) 
    modArgs[ 2 ] = [ 'Banned' ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [ sol[ 1 ] ] )
    modArgs[ 2 ] = [ 'Active', 'Banned' ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )
    modArgs[ 2 ] = 'eGGs'
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [])
    modArgs[ 2 ] = [ 'eGGs' ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )      
    modArgs[ 2 ] = [ 'eGGs', 'Banned' ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [ sol[ 1 ] ] )        
    modArgs[ 2 ] = [ None ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )      
    modArgs[ 2 ] = [ 'eGGs', 'Banned', None ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [ sol[ 1 ] ] )

    #Test forth parameter ( reason )
    modArgs = initArgs[:]
    modArgs[ 3 ] = 'Init'
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )
    modArgs[ 3 ] = [ 'Init' ]    
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )        
    modArgs[ 3 ] = 'eGGs'
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] ) 
    modArgs[ 3 ] = [ 'eGGs' ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 3 ] = [ 'Init', 'eGGs' ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )           
    modArgs[ 3 ] = [ None ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 3 ] = [ 'Init', 'eGGs', None ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )

    #Test fifth parameter ( dateCreated )
    modArgs = initArgs[:]
    modArgs[ 4 ] = dSol
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )   
    modArgs[ 4 ] = [ dSol ]    
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )        
    modArgs[ 4 ] = dNow
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] ) 
    modArgs[ 4 ] = [ dNow ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 4 ] = [ dSol, dNow ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )     
    modArgs[ 4 ] = [ None ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 4 ] = [ dSol, dNow, None ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
            
    #Test sixth parameter ( dateEffective ) 
    modArgs = initArgs[:]
    modArgs[ 5 ] = dSol
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )   
    modArgs[ 5 ] = [ dSol ]    
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )        
    modArgs[ 5 ] = dNow
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] ) 
    modArgs[ 5 ] = [ dNow ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 5 ] = [ dSol, dNow ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )     
    modArgs[ 5 ] = [ None ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 5 ] = [ dSol, dNow, None ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )

    #Test seventh parameter ( dateEnd )
    modArgs = initArgs[:]
    modArgs[ 6 ] = dSol
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )   
    modArgs[ 6 ] = [ dSol ]    
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )        
    modArgs[ 6 ] = dNow
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] ) 
    modArgs[ 6 ] = [ dNow ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 6 ] = [ dSol, dNow ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )     
    modArgs[ 6 ] = [ None ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 6 ] = [ dSol, dNow, None ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
                
    #Test eighth parameter ( lastCheckTime )
    modArgs = initArgs[:]
    modArgs[ 6 ] = dSol
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )   
    modArgs[ 6 ] = [ dSol ]    
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )        
    modArgs[ 6 ] = dNow
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] ) 
    modArgs[ 6 ] = [ dNow ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 6 ] = [ dSol, dNow ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )     
    modArgs[ 6 ] = [ None ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 6 ] = [ dSol, dNow, None ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
        
    #Test ninth parameter ( tokenOwner )
    modArgs = initArgs[:]
    modArgs[ 8 ] = 'RS_SVC'
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )
    modArgs[ 8 ] = [ 'RS_SVC' ]   
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )   
    modArgs[ 8 ] = 'eGGs'   
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 8 ] = [ 'eGGs' ]   
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs' ]   
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )
    modArgs[ 8 ] = [ None ]   
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs', None ]   
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )
    
    #Test tenth parameter ( tokenExpiration )
    modArgs = initArgs[:]
    modArgs[ 9 ] = dSol
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )   
    modArgs[ 9 ] = [ dSol ]    
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )        
    modArgs[ 9 ] = dNow
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] ) 
    modArgs[ 9 ] = [ dNow ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 9 ] = [ dSol, dNow ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )     
    modArgs[ 9 ] = [ None ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 9 ] = [ dSol, dNow, None ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
         
    '''
      Mixed tests
    '''
    
    '''
      Kwargs tests...
    '''         
             
#  def test_Sites07_getSitesHistory( self ):
#    pass 
#  def test_Sites08_getSitesScheduledStatus( self ):
#    pass
#  def test_Sites09_getSitesPresent( self ):
#    pass
#  def test_Sites10_deleteSites( self ):
#    pass
#  def test_Sites11_deleteSitesScheduledStatus( self ):
#    pass
#  def test_Sites12_deleteSitesHistory( self ):
#    pass          

################################################################################    



#def permutator( solution, wrong ):
#
#  def fillDict( permDict, iList, label ):
#    
#    for i in iList:
#      
#      counter = 0
#      if not permDict.has_key( counter ):
#        permDict[ counter ] = { }
#        
#      for ii in i:
#        if not permDict[ counter ].has_key( label ):
#          permDict[ counter ][ label ] = []      
#        permDict[ counter ][ label ].append( ii )
#            
#      counter += 1    
#    return permDict
#   
#  permDict = fillDict( {}, solution, 's')
#  permDict = fillDict( permDict, wrong, 'w')
#   
#  print permDict  
#   
#  for k in permDict.keys():
#    
#    inputs = permDict[ k ][ 's' ] + permDict[ k ][ 'w' ] 
#    permDict[ k ][ 'p' ] = inputs
#  
#    localPerm = []
#    for lp_ in xrange( 1, len(inputs) + 1):
#      #localPerm +=  list( itertools.permutations( inputs, lp_ ) )           
#      localPerm +=  list( itertools.product( inputs, repeat = 4 ) )
#    permDict[ k ][ 'p' ] += localPerm
#       
#  print permDict         

#def semipermuter( solution, wrong ):
#  
#  perms      = {}
#
#  solCounter = 0
#
#  for sol in solution:
#    counter = 0  
#    for s in sol:     
#      if not perms.has_key( counter ):
#        perms[ counter ] = {}
#      if not perms[ counter ].has_key( s ):
#        perms[ counter ][ s ] = []   
#      
#      perms[ counter ][ s ].append( solCounter ) 
#      counter += 1    
#    solCounter += 1  
#
#  for wrg in wrong:
#    counter = 0  
#    for w in wrg:
#      
#      if not perms[ counter ].has_key( w ):
#        perms[ counter ][ w ] = []       
#      
#      counter += 1    
##    maxCounter = counter
#
#  mixer( perms, 1 )
#
#  return perms
#
#def mixer( perms, iterations ):
#  
#  print perms
##  print iterations
#  
#  import random
#  
#  initArgs   = [ None for _k in perms.keys() ]
#  
#  sols  = []
#  sols2 = []
#  sols3 = []
#  
#  for pos,values in perms.items():
#    
#    for value,solution in values.items():
#      
#      modArgs = initArgs[:] # Slice of entire list == copy.copy
#      modArgs[ pos ] = [ value ] 
#      lst = [ modArgs, solution ]
#      sols.append( lst )
#      
#      # WE keep this apart
#      modArgs = modArgs[:]
#      modArgs[ pos ] = value
#      lst2 = [ modArgs, solution ]
#      sols2.append( lst2 ) 
#
#  for _i in xrange( 0, iterations ):
#    n = random.randint( 1, len( sols ) )
#    print '---'
#    print n
#    print '---'
#    
#    iterSol = [initArgs[:],[]]
#    
#    for _n in xrange( 0, n ):
#      p = random.randint( 0, len( sols ) - 1 )
#      print p
#      
#      print sols[ p ][ 0 ]
#      print sols[ p ][ 1 ]
#      
#      co = -1
#      for e in sols[ p ][ 0 ]:
#        co += 1
#        if e is None:
#          continue
#        if iterSol[ 0 ][ co ] is None:
#          iterSol[ 0 ][ co ] = []
#        iterSol[ 0 ][ co ].append( e[0] )   
#        
#      #co = -1
#      #for e in sols[ p ][ 1 ]:
#      #  co += 1
#      #  if not e:
#      #    continue
#      #  #if iterSol[ 1 ][ co ] is None:
#      #  #  iterSol[ 1 ][ co ] = []
#      #  iterSol[ 1 ][ co ].append( e[0] )  
#            
#      #iterSol[ 0 ].append( sols[ p ][ 0 ] )
#      
#      #print sols[ p ][ 0 ]
#      iterSol[ 1 ] += sols[ p ][ 1 ] 
#      #print sols[ p ][ 1 ]
#     
#    print iterSol  
#    #sols3.append( iterSol )  
#    
#  print '-------------'  
#  print sols
#  print sols2
#  print sols3
#  print '-------------'

#  initArgs = []
#  for mC in xrange( 0, maxCounter ):
#    initArgs.append( None )

#  pos,sols = perms.items()
  
#  print zip( pos, sols )    
      
#  sols = []
#  for position, values in perms.items():
#    for k,v in values.items():
#      modArgs = initArgs
      
        
  

#  for x in xrange( 0, maxCounter ):
#    vals  = [ ( [perm[0]], perm[1] ) for perm in perms[x] ]
#    vals2 = ( [ perm[0] for perm in perms[ x ] ], [ perm[1][ 0 ] for perm in perms[ x ] if perm[1][0] is not None ] )    
#    perms[ x ] += vals
#    perms[ x ].append( vals2 )
     
#  return None

#def semipermuter( solution, wrong ):
#  
#  perms      = {}
#  maxCounter = 0
#
#  solCounter = 0
#
#  for sol in solution:
#    counter = 0  
#    for s in sol:
#      if not perms.has_key( counter ):
#        perms[ counter ] = []
#      perms[ counter ].append( ( s, [ solCounter ] ) )
#      counter += 1    
#    solCounter += 1  
#
#  for wrg in wrong:
#    counter = 0  
#    for w in wrg:    
#      perms[ counter ].append( ( w, [ None ] ) )
#      counter += 1    
#    maxCounter = counter
#
#  for x in xrange( 0, maxCounter ):
#    vals  = [ ( [perm[0]], perm[1] ) for perm in perms[x] ]
#    vals2 = ( [ perm[0] for perm in perms[ x ] ], [ perm[1][ 0 ] for perm in perms[ x ] if perm[1][0] is not None ] )    
#    perms[ x ] += vals
#    perms[ x ].append( vals2 )
#     
#  return perms
    

################################################################################

def cleanDB():

  ## CLEAN UP DB FOR NEXT TEST
  print '\n----------------------------------------------------------------------'
  print 'Cleaning db for next tests'
  
  import MySQLdb
  db = MySQLdb.connect(host='localhost',user='test',passwd='test',db='ResourceStatusTestDB')
  cursor = db.cursor()
  
  elements = [ 'Site', 'Service', 'Resource', 'StorageElement' ]
    
  items = [ 'GridSite' ]
  
  for element in elements:
    items.append( '%sStatus' % element )
    items.append( '%sHistory' % element )
    items.append( '%sScheduledStatus' % element )
    items.append( '%s' % element )
  
  for item in items:
    cursor.execute( 'TRUNCATE TABLE %s;' % item)

  print '----------------------------------------------------------------------\n'

################################################################################

def runTests():

  suite = unittest.defaultTestLoader.loadTestsFromTestCase(Test_ResourceStatusDB)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(Test_GridSites))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(Test_Sites))
  
  unittest.TextTestRunner(verbosity=2).run(suite)    


################################################################################    
    
if __name__ == '__main__':

#  sTime = datetime.now()
#  semipermuter( [['A','B'],['C','D']], [[5,6],[7,8]] )
#  eTime = datetime.now()

  cleanDB()
  runTests() 
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF    