import unittest

from datetime import datetime

from DIRAC.Core.Base import Script
Script.parseCommandLine()

from DIRAC.ResourceStatusSystem.DB.ResourceStatusDB import ResourceStatusDB
from DIRAC.Core.Utilities.MySQL import MySQL

from DIRAC import S_OK

#from DIRAC.ResourceStatusSystem.DB.test.TestCases import Test_ResourceStatusDB

################################################################################

class Test_ResourceStatusDB( unittest.TestCase ):
  
  def setUp( self ):   
    db        = MySQL( 'localhost', 'test', 'test', 'ResourceStatusTestDB' )
    self.rsDB = ResourceStatusDB( DBin = db )
    
################################################################################         
    
class Test_GridSitesInput( Test_ResourceStatusDB ):  
  
  def test_01_GridSitesInput_addOrModifyGridSite( self ):
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
                
  def test_02_GridSitesInput_getGridSites( self ):
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
    self.assertEqual( res[ 'Value' ], [ ] )
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
    self.assertEqual( res[ 'Value' ], [ sol[ 0 ] ] )
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
   
  def test_03_GridSitesInput_deleteGridSites( self ):
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
    res = self.rsDB.deleteGridSites( [ 'AUVERGRID' ] ) 
    self.assertEqual( res, S_OK() )
    res = self.rsDB.deleteGridSites( [ 'INFN-T1', 'eGGs' ] ) 
    self.assertEqual( res, S_OK() )
    res = self.rsDB.deleteGridSites( None ) 
    self.assertNotEqual( res[ 'OK' ], True )   
    res = self.rsDB.deleteGridSites( [ None ] ) 
    self.assertEqual( res, S_OK() )
    res = self.rsDB.deleteGridSites( [ 'AUVERGRID', None ] ) 
    self.assertEqual( res, S_OK() )
   
#  def test_04_GridSitesInput_flow( self ):
#  
#    res = self.rsDB.addOrModifyGridSite( 'AUVERGRID', 'T2' )
#    self.assertEqual( res, S_OK() )
#    res = self.rsDB.getGridSites( 'AUVERGRID', None )
#    self.assertEqual( res[ 'Value' ], [[ 'AUVERGRID', 'T2' ]] )
#    res = self.rsDB.addOrModifyGridSite( 'AUVERGRID', 'T1' )
#    self.assertEqual( res, S_OK() )
#    res = self.rsDB.getGridSites( 'AUVERGRID', None )
#    self.assertEqual( res[ 'Value' ], [[ 'AUVERGRID', 'T1' ]] ) 
#    res = self.rsDB.deleteGridSites( 'AUVERGRID' ) 
#    self.assertEqual( res, S_OK() )
#    res = self.rsDB.getGridSites( 'AUVERGRID', None )
#    self.assertEqual( res[ 'Value' ], [] )

class Test_SitesInput( Test_ResourceStatusDB ):
  
  def test_01_SitesInput_addOrModifySite( self ):
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
           
  def test_02_SitesInput_setSiteStatus( self ):
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
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 0 ] = [ 'LCG.CERN.ch' ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 0 ] = 'eGGs'
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 0 ] = [ 'eGGs' ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'eGGs' ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 0 ] = [ None ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'eGGs', None ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    
    #Test second parameter ( statusType )
    modArgs[ 1 ] = ''
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 1 ] = [ '' ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 1 ] = [ 'eGGs' ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 1 ] = [ '', 'eGGs' ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 1 ] = [ None ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 1 ] = [ '', 'eGGs', None ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )

    #Test third parameter ( status )
    modArgs[ 2 ] = 'Active'
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 2 ] = [ 'Active' ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 2 ] = 'eGGs'
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 2 ] = [ 'eGGs' ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 2 ] = [ 'Active', 'eGGs' ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 2 ] = [ None ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 2 ] = [ 'Active', 'eGGs', None ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
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

  def test_03_SitesInput_setSiteScheduledStatus( self ):
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
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 0 ] = [ 'LCG.CERN.ch' ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 0 ] = 'eGGs'
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 0 ] = [ 'eGGs' ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'eGGs' ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 0 ] = [ None ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'eGGs', None ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    
    #Test second parameter ( statusType )
    modArgs[ 1 ] = ''
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 1 ] = [ '' ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 1 ] = [ 'eGGs' ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 1 ] = [ '', 'eGGs' ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 1 ] = [ None ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 1 ] = [ '', 'eGGs', None ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )

    #Test third parameter ( status )
    modArgs[ 2 ] = 'Active'
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 2 ] = [ 'Active' ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 2 ] = 'eGGs'
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 2 ] = [ 'eGGs' ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 2 ] = [ 'Active', 'eGGs' ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 2 ] = [ None ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 2 ] = [ 'Active', 'eGGs', None ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
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
    
#    res = self.rsDB.setSiteScheduledStatus( 'LCG.CERN.ch', '', 'Active', None, now,
#                                            now, now, now, None, now )
#    self.assertEqual( res, S_OK() )
    res = self.rsDB.setSiteScheduledStatus( 'eGGs', '', 'Active', None, now,
                                            now, now, now, None, now )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.setSiteScheduledStatus( 'LCG.CERN.ch', 'eGGs', 'Active', None, now,
                                            now, now, now, None, now )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.setSiteScheduledStatus( 'LCG.CERN.ch', '', 'eGGs', None, now,
                                            now, now, now, None, now )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    
  def test_04_SitesInput_updateSiteStatus( self ):
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
           [ 1L, 'LCG.CERN.ch', '', 'Active', 'Init', dSol, dSol, dSol, dSol, 'RS_SVC', dSol], 
           [ 2L, 'LCG.CNAF.it', '', 'Banned', 'Init', dSol, dSol, dSol, dSol, 'RS_SVC', dSol]
          ]
    
    res = self.rsDB.updateSiteStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    
    '''
      Param tests
    '''    
    #Test first parameter ( siteName )
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'LCG.CERN.ch'
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 0 ] = [ 'LCG.CERN.ch' ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 0 ] = 'eGGs'
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 0 ] = [ 'eGGs' ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'eGGs' ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 0 ] = [ None ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'eGGs', None ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    
    #Test second parameter ( statusType )
    modArgs[ 1 ] = ''
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 1 ] = [ '' ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 1 ] = [ 'eGGs' ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 1 ] = [ '', 'eGGs' ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 1 ] = [ None ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 1 ] = [ '', 'eGGs', None ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )

    #Test third parameter ( status )
    modArgs[ 2 ] = 'Active'
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 2 ] = [ 'Active' ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 2 ] = 'eGGs'
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 2 ] = [ 'eGGs' ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 2 ] = [ 'Active', 'eGGs' ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 2 ] = [ None ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 2 ] = [ 'Active', 'eGGs', None ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
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
    
    res = self.rsDB.setSiteStatus( *tuple( sol[ 0 ][1:] ) )
    self.assertEqual( res, S_OK() ) 
    res = self.rsDB.setSiteStatus( *tuple( sol[ 1 ][1:] ) )
    self.assertEqual( res, S_OK() )    
        
  def test_05_SitesInput_getSites( self ):
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

  def test_06_SitesInput_getSitesStatus( self ):
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
    modArgs[ 7 ] = dSol
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )   
    modArgs[ 7 ] = [ dSol ]    
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )        
    modArgs[ 7 ] = dNow
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] ) 
    modArgs[ 7 ] = [ dNow ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 7 ] = [ dSol, dNow ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )     
    modArgs[ 7 ] = [ None ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 7 ] = [ dSol, dNow, None ]
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
             
  def test_07_SitesInput_getSitesHistory( self ):
    '''
    getSitesHistory( self, siteName, statusType, status, reason, dateCreated, 
                       dateEffective, dateEnd, lastCheckTime, tokenOwner, 
                       tokenExpiration, **kwargs )
    '''
    dNow = datetime.now()
    
    res = self.rsDB.getSitesHistory()
    self.assertNotEqual( res.has_key( 'Value' ), True )   
    res = self.rsDB.getSitesHistory( 'LCG.CERN.ch' )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.getSitesHistory( 'LCG.CERN.ch', '' )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.getSitesHistory( 'LCG.CERN.ch', '', 'eGGs reason' )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.getSitesHistory( 'LCG.CERN.ch', '', 'eGGs reason', dNow )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.getSitesHistory( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.getSitesHistory( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow, dNow )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.getSitesHistory( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow, dNow, dNow )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.getSitesHistory( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow, dNow, dNow, 'token' )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.getSitesHistory( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow, dNow, dNow, 'token', dNow, {}, dNow )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    
    initArgs = [ None,None,None,None,None,None,None,None,None,None ]
    dSol     = datetime(9999, 12, 11, 10, 9, 8)
    sol      = [
                [10L, 'LCG.CERN.ch', '', 'Active', 'Init', dSol, dSol, dSol, dSol, 'RS_SVC', dSol], 
                [11L, 'LCG.CNAF.it', '', 'Banned', 'Init', dSol, dSol, dSol, dSol, 'RS_SVC', dSol]
               ]
        
    self.rsDB.deleteSitesHistory( 'LCG.CERN.ch', None, None, None, None, None, None, None, None, None )    
    self.rsDB.deleteSitesHistory( 'LCG.CNAF.it', None, None, None, None, None, None, None, None, None )
    
    # This is risky.. if we do not hit the same second, tachan !!
    now = datetime.utcnow().replace( microsecond = 0 )
    
    import copy
    
    
    sol2 = copy.deepcopy( sol )
    sol2[ 0 ][ 6 ] = now   
    sol2[ 1 ][ 6 ] = now
    self.rsDB.setSiteStatus( *tuple( sol2[ 0 ][1:] ) )
    self.rsDB.setSiteStatus( *tuple( sol2[ 1 ][1:] ) )

    sol[ 0 ][ 7 ] = now   
    sol[ 1 ][ 7 ] = now
    
    res = self.rsDB.getSitesHistory( *tuple( initArgs ) )
    self.assertEquals( res[ 'Value' ], sol )       
    
    '''
      Param tests
    '''    
    #Test first param ( siteName )
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'LCG.CERN.ch'
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [ sol[ 0 ] ] )
    modArgs[ 0 ] = [ 'LCG.CERN.ch' ]    
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [ sol[ 0 ] ] )        
    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'LCG.CNAF.it' ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )
    modArgs[ 0 ] = 'LCG.eGGs.xy'
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 0 ] = [ 'LCG.eGGs.xy' ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'LCG.eGGs.xy' ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [ sol[0] ] )
    modArgs[ 0 ] = [ None ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'LCG.eGGs.xy', None ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [ sol[0] ] )

    #Test second param ( statusType ) 
    modArgs = initArgs[:]
    modArgs[ 1 ] = ''
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )
    modArgs[ 1 ] = [ '' ]    
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )        
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 1 ] = [ 'eGGs' ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 1 ] = [ '', 'eGGs' ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )
    modArgs[ 1 ] = [ None ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 1 ] = [ '', 'eGGs', None ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )

    #Test third parameter ( status )
    modArgs = initArgs[:]
    modArgs[ 2 ] = 'Active'
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [ sol[0] ] )
    modArgs[ 2 ] = [ 'Active' ]    
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [ sol[0] ] )        
    modArgs[ 2 ] = 'Banned'
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [ sol[ 1 ] ] ) 
    modArgs[ 2 ] = [ 'Banned' ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [ sol[ 1 ] ] )
    modArgs[ 2 ] = [ 'Active', 'Banned' ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )
    modArgs[ 2 ] = 'eGGs'
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [])
    modArgs[ 2 ] = [ 'eGGs' ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )      
    modArgs[ 2 ] = [ 'eGGs', 'Banned' ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [ sol[ 1 ] ] )        
    modArgs[ 2 ] = [ None ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )      
    modArgs[ 2 ] = [ 'eGGs', 'Banned', None ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [ sol[ 1 ] ] )

    #Test forth parameter ( reason )
    modArgs = initArgs[:]
    modArgs[ 3 ] = 'Init'
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )
    modArgs[ 3 ] = [ 'Init' ]    
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )        
    modArgs[ 3 ] = 'eGGs'
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] ) 
    modArgs[ 3 ] = [ 'eGGs' ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 3 ] = [ 'Init', 'eGGs' ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )           
    modArgs[ 3 ] = [ None ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 3 ] = [ 'Init', 'eGGs', None ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )

    #Test fifth parameter ( dateCreated )
    modArgs = initArgs[:]
    modArgs[ 4 ] = dSol
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )   
    modArgs[ 4 ] = [ dSol ]    
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )        
    modArgs[ 4 ] = dNow
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] ) 
    modArgs[ 4 ] = [ dNow ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 4 ] = [ dSol, dNow ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )     
    modArgs[ 4 ] = [ None ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 4 ] = [ dSol, dNow, None ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
            
    #Test sixth parameter ( dateEffective ) 
    modArgs = initArgs[:]
    modArgs[ 5 ] = dSol
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )   
    modArgs[ 5 ] = [ dSol ]    
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )        
    modArgs[ 5 ] = dNow
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] ) 
    modArgs[ 5 ] = [ dNow ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 5 ] = [ dSol, dNow ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )     
    modArgs[ 5 ] = [ None ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 5 ] = [ dSol, dNow, None ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )

    #Test seventh parameter ( dateEnd )
    modArgs = initArgs[:]
    modArgs[ 6 ] = now
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )   
    modArgs[ 6 ] = [ now ]    
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )        
    modArgs[ 6 ] = dNow
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] ) 
    modArgs[ 6 ] = [ dNow ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 6 ] = [ now, dNow ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )     
    modArgs[ 6 ] = [ None ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 6 ] = [ now, dNow, None ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
                
    #Test eighth parameter ( lastCheckTime )
    modArgs = initArgs[:]
    modArgs[ 7 ] = dSol
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )   
    modArgs[ 7 ] = [ dSol ]    
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )        
    modArgs[ 7 ] = dNow
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] ) 
    modArgs[ 7 ] = [ dNow ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 7 ] = [ dSol, dNow ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )     
    modArgs[ 7 ] = [ None ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 7 ] = [ dSol, dNow, None ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
        
    #Test ninth parameter ( tokenOwner )
    modArgs = initArgs[:]
    modArgs[ 8 ] = 'RS_SVC'
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )
    modArgs[ 8 ] = [ 'RS_SVC' ]   
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )   
    modArgs[ 8 ] = 'eGGs'   
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 8 ] = [ 'eGGs' ]   
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs' ]   
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )
    modArgs[ 8 ] = [ None ]   
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs', None ]   
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )
    
    #Test tenth parameter ( tokenExpiration )
    modArgs = initArgs[:]
    modArgs[ 9 ] = dSol
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )   
    modArgs[ 9 ] = [ dSol ]    
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )        
    modArgs[ 9 ] = dNow
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] ) 
    modArgs[ 9 ] = [ dNow ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 9 ] = [ dSol, dNow ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )     
    modArgs[ 9 ] = [ None ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 9 ] = [ dSol, dNow, None ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
         
    '''
      Mixed tests
    '''
    
    '''
      Kwargs tests...
    '''         

  def test_08_SitesInput_getSitesScheduledStatus( self ):
    '''
    getSitesScheduledStatus( self, siteName, statusType, status, reason, dateCreated, 
                             dateEffective, dateEnd, lastCheckTime, tokenOwner, 
                             tokenExpiration, **kwargs )
    '''
    dNow = datetime.now()
    
    res = self.rsDB.getSitesScheduledStatus()
    self.assertNotEqual( res.has_key( 'Value' ), True )   
    res = self.rsDB.getSitesScheduledStatus( 'LCG.CERN.ch' )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.getSitesScheduledStatus( 'LCG.CERN.ch', '' )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.getSitesScheduledStatus( 'LCG.CERN.ch', '', 'eGGs reason' )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.getSitesScheduledStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.getSitesScheduledStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.getSitesScheduledStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow, dNow )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.getSitesScheduledStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow, dNow, dNow )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.getSitesScheduledStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow, dNow, dNow, 'token' )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.getSitesScheduledStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow, dNow, dNow, 'token', dNow, {}, dNow )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    
    initArgs = [ None,None,None,None,None,None,None,None,None,None ]
    dSol     = datetime(9999, 12, 11, 10, 9, 8)
    sol      = [
                [1L, 'LCG.CERN.ch', '', 'Active', 'Init', dSol, dSol, dSol, dSol, 'RS_SVC', dSol], 
                [2L, 'LCG.CNAF.it', '', 'Banned', 'Init', dSol, dSol, dSol, dSol, 'RS_SVC', dSol]
               ]
        
    self.rsDB.setSiteScheduledStatus( *tuple( sol[ 0 ][1:] ) )
    self.rsDB.setSiteScheduledStatus( *tuple( sol[ 1 ][1:] ) )
    
    # This is risky.. if we do not hit the same second, tachan !!
    #now = datetime.utcnow().replace( microsecond = 0 )
    #sol[ 0 ][ 7 ] = now   
    #sol[ 1 ][ 7 ] = now  
        
    res = self.rsDB.getSitesScheduledStatus( *tuple( initArgs ) )
    self.assertEquals( res[ 'Value' ], sol )       
    
    '''
      Param tests
    '''    
    #Test first param ( siteName )
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'LCG.CERN.ch'
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [ sol[ 0 ] ] )
    modArgs[ 0 ] = [ 'LCG.CERN.ch' ]    
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [ sol[ 0 ] ] )        
    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'LCG.CNAF.it' ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )
    modArgs[ 0 ] = 'LCG.eGGs.xy'
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 0 ] = [ 'LCG.eGGs.xy' ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'LCG.eGGs.xy' ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [ sol[0] ] )
    modArgs[ 0 ] = [ None ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'LCG.eGGs.xy', None ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [ sol[0] ] )

    #Test second param ( statusType ) 
    modArgs = initArgs[:]
    modArgs[ 1 ] = ''
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )
    modArgs[ 1 ] = [ '' ]    
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )        
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 1 ] = [ 'eGGs' ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 1 ] = [ '', 'eGGs' ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )
    modArgs[ 1 ] = [ None ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 1 ] = [ '', 'eGGs', None ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )

    #Test third parameter ( status )
    modArgs = initArgs[:]
    modArgs[ 2 ] = 'Active'
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [ sol[0] ] )
    modArgs[ 2 ] = [ 'Active' ]    
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [ sol[0] ] )        
    modArgs[ 2 ] = 'Banned'
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [ sol[ 1 ] ] ) 
    modArgs[ 2 ] = [ 'Banned' ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [ sol[ 1 ] ] )
    modArgs[ 2 ] = [ 'Active', 'Banned' ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )
    modArgs[ 2 ] = 'eGGs'
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [])
    modArgs[ 2 ] = [ 'eGGs' ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )      
    modArgs[ 2 ] = [ 'eGGs', 'Banned' ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [ sol[ 1 ] ] )        
    modArgs[ 2 ] = [ None ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )      
    modArgs[ 2 ] = [ 'eGGs', 'Banned', None ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [ sol[ 1 ] ] )

    #Test forth parameter ( reason )
    modArgs = initArgs[:]
    modArgs[ 3 ] = 'Init'
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )
    modArgs[ 3 ] = [ 'Init' ]    
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )        
    modArgs[ 3 ] = 'eGGs'
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] ) 
    modArgs[ 3 ] = [ 'eGGs' ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 3 ] = [ 'Init', 'eGGs' ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )           
    modArgs[ 3 ] = [ None ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 3 ] = [ 'Init', 'eGGs', None ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )

    #Test fifth parameter ( dateCreated )
    modArgs = initArgs[:]
    modArgs[ 4 ] = dSol
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )   
    modArgs[ 4 ] = [ dSol ]    
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )        
    modArgs[ 4 ] = dNow
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] ) 
    modArgs[ 4 ] = [ dNow ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 4 ] = [ dSol, dNow ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )     
    modArgs[ 4 ] = [ None ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 4 ] = [ dSol, dNow, None ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
            
    #Test sixth parameter ( dateEffective ) 
    modArgs = initArgs[:]
    modArgs[ 5 ] = dSol
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )   
    modArgs[ 5 ] = [ dSol ]    
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )        
    modArgs[ 5 ] = dNow
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] ) 
    modArgs[ 5 ] = [ dNow ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 5 ] = [ dSol, dNow ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )     
    modArgs[ 5 ] = [ None ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 5 ] = [ dSol, dNow, None ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )

    #Test seventh parameter ( dateEnd )
    modArgs = initArgs[:]
    modArgs[ 6 ] = dSol
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )   
    modArgs[ 6 ] = [ dSol ]    
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )        
    modArgs[ 6 ] = dNow
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] ) 
    modArgs[ 6 ] = [ dNow ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 6 ] = [ dSol, dNow ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )     
    modArgs[ 6 ] = [ None ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 6 ] = [ dSol, dNow, None ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
                
    #Test eighth parameter ( lastCheckTime )
    modArgs = initArgs[:]
    modArgs[ 7 ] = dSol
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )   
    modArgs[ 7 ] = [ dSol ]    
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )        
    modArgs[ 7 ] = dNow
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] ) 
    modArgs[ 7 ] = [ dNow ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 7 ] = [ dSol, dNow ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )     
    modArgs[ 7 ] = [ None ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 7 ] = [ dSol, dNow, None ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
        
    #Test ninth parameter ( tokenOwner )
    modArgs = initArgs[:]
    modArgs[ 8 ] = 'RS_SVC'
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )
    modArgs[ 8 ] = [ 'RS_SVC' ]   
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )   
    modArgs[ 8 ] = 'eGGs'   
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 8 ] = [ 'eGGs' ]   
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs' ]   
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )
    modArgs[ 8 ] = [ None ]   
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs', None ]   
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )
    
    #Test tenth parameter ( tokenExpiration )
    modArgs = initArgs[:]
    modArgs[ 9 ] = dSol
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )   
    modArgs[ 9 ] = [ dSol ]    
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )        
    modArgs[ 9 ] = dNow
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] ) 
    modArgs[ 9 ] = [ dNow ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 9 ] = [ dSol, dNow ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], sol )     
    modArgs[ 9 ] = [ None ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 9 ] = [ dSol, dNow, None ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
         
    '''
      Mixed tests
    '''
    
    '''
      Kwargs tests...
    '''         

  def test_09_SitesInput_getSitesPresent( self ):
    '''
    getSitesPresent( self, siteName, siteType, gridSiteName, gridTier, 
                     statusType, status, dateEffective, reason, lastCheckTime, 
                     tokenOwner, tokenExpiration, formerStatus, **kwargs )
    '''
    dNow = datetime.now()
    
    res = self.rsDB.getSitesPresent()
    self.assertNotEqual( res.has_key( 'Value' ), True )   
    res = self.rsDB.getSitesPresent( 'LCG.CERN.ch' )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.getSitesPresent( 'LCG.CERN.ch', 'T0' )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.getSitesPresent( 'LCG.CERN.ch', 'T0', 'CERN-PROD' )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.getSitesPresent( 'LCG.CERN.ch', 'T0', 'CERN-PROD', 'T0' )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.getSitesPresent( 'LCG.CERN.ch', 'T0', 'CERN-PROD', 'T0', '' )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.getSitesPresent( 'LCG.CERN.ch', 'T0', 'CERN-PROD', 'T0', '', 'Active' )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.getSitesPresent( 'LCG.CERN.ch', 'T0', 'CERN-PROD', 'T0', '', 
                                             'Active', dNow )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.getSitesPresent( 'LCG.CERN.ch', 'T0', 'CERN-PROD', 'T0', '', 
                                             'Active', dNow, 'eGGs reason' )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.getSitesPresent( 'LCG.CERN.ch', 'T0', 'CERN-PROD', 'T0', '', 
                                             'Active', dNow, 'eGGs reason', dNow )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.getSitesPresent( 'LCG.CERN.ch', 'T0', 'CERN-PROD', 'T0', '', 
                                             'Active', dNow, 'eGGs reason', dNow, 'token' )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.getSitesPresent( 'LCG.CERN.ch', 'T0', 'CERN-PROD', 'T0', '', 
                                             'Active', dNow, 'eGGs reason', dNow, 'token', dNow )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.getSitesPresent( 'LCG.CERN.ch', 'T0', 'CERN-PROD', 'T0', '', 
                                             'Active', dNow, 'eGGs reason', dNow, 'token', dNow, 'Banned', {} )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.getSitesPresent( 'LCG.CERN.ch', 'T0', 'CERN-PROD', 'T0', '', 
                                             'Active', dNow, 'eGGs reason', dNow, 'token', dNow, 'Banned', {}, dNow )
    self.assertNotEqual( res.has_key( 'Value' ), True )

    
    initArgs = [ None,None,None,None,None,None,None,None,None,None, None, None ]
    dSol     = datetime(9999, 12, 11, 10, 9, 8)
    sol      = [
                [1L, 'LCG.CERN.ch', '', 'Active', 'Init', dSol, dSol, dSol, dSol, 'RS_SVC', dSol], 
                [2L, 'LCG.CNAF.it', '', 'Banned', 'Init', dSol, dSol, dSol, dSol, 'RS_SVC', dSol]
               ]

    '''
    [
     ['LCG.CERN.ch', 'T0', 'CERN-PROD', 'T0', '', 'Active', datetime.datetime(2011, 10, 5, 15, 14, 49), 'Init', 
         datetime.datetime(9999, 12, 11, 10, 9, 8), 'RS_SVC', datetime.datetime(9999, 12, 11, 10, 9, 8), 'Active'], 
     ['LCG.CNAF.it', 'T1', 'INFN-T1', 'T1', '', 'Banned', datetime.datetime(2011, 10, 5, 15, 14, 49), 'Init', 
        datetime.datetime(9999, 12, 11, 10, 9, 8), 'RS_SVC', datetime.datetime(9999, 12, 11, 10, 9, 8), 'Banned']]
    '''

    '''
      WTF !! This is bizarre !!
    '''

    #print self.rsDB.getSites( *tuple( initArgs[9:]) )
    #print self.rsDB.getGridSites( None, None )
    #print self.rsDB.getSitesStatus( *tuple( initArgs[2:]))
    #print self.rsDB.getSitesHistory( *tuple( initArgs[2:]))

    #res = self.rsDB.getSitesPresent( None,None,None,None,None,None,None,None,None,None, None, None )
    #print res[ 'Value' ]
    #res = self.rsDB.getSitesPresent( 'LCG.CERN.ch',None,None,None,None,None,None,None,None,None, None, None )
    #print res[ 'Value' ]

  def test_10_SitesInput_deleteSitesScheduledStatus( self ):
    '''
    deleteSitesScheduledStatus( self, siteName, statusType, status, reason, 
                                dateCreated, dateEffective, dateEnd, 
                                lastCheckTime, tokenOwner, tokenExpiration
    '''
    dNow = datetime.now()
    
    res = self.rsDB.deleteSitesScheduledStatus()
    self.assertNotEqual( res.has_key( 'Value' ), True )   
    res = self.rsDB.deleteSitesScheduledStatus( 'LCG.CERN.ch' )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.deleteSitesScheduledStatus( 'LCG.CERN.ch', '' )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.deleteSitesScheduledStatus( 'LCG.CERN.ch', '', 'eGGs reason' )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.deleteSitesScheduledStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.deleteSitesScheduledStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.deleteSitesScheduledStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow, dNow )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.deleteSitesScheduledStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow, dNow, dNow )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.deleteSitesScheduledStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow, dNow, dNow, 'token' )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.deleteSitesScheduledStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow, dNow, dNow, 'token', dNow, {}, dNow )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    
    initArgs = [ None,None,None,None,None,None,None,None,None,None ]
    dSol     = datetime(9999, 12, 11, 10, 9, 8)
    sol      = [
                [1L, 'LCG.CERN.ch', '', 'Active', 'Init', dSol, dSol, dSol, dSol, 'RS_SVC', dSol], 
                [2L, 'LCG.CNAF.it', '', 'Banned', 'Init', dSol, dSol, dSol, dSol, 'RS_SVC', dSol]
               ]
        
       
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( initArgs ) )
    self.assertNotEquals( res[ 'OK' ], True )       
    
    '''
      Param tests
    '''    
    #Test first param ( siteName )
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'LCG.CERN.ch'
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    modArgs[ 0 ] = [ 'LCG.CERN.ch' ]    
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() )        
    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'LCG.CNAF.it' ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    modArgs[ 0 ] = 'LCG.eGGs.xy'
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    modArgs[ 0 ] = [ 'LCG.eGGs.xy' ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'LCG.eGGs.xy' ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    modArgs[ 0 ] = [ None ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'LCG.eGGs.xy', None ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 

    #Test second param ( statusType ) 
    modArgs = initArgs[:]
    modArgs[ 1 ] = ''
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    modArgs[ 1 ] = [ '' ]    
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() )         
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    modArgs[ 1 ] = [ 'eGGs' ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    modArgs[ 1 ] = [ '', 'eGGs' ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    modArgs[ 1 ] = [ None ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    modArgs[ 1 ] = [ '', 'eGGs', None ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 

    #Test third parameter ( status )
    modArgs = initArgs[:]
    modArgs[ 2 ] = 'Active'
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    modArgs[ 2 ] = [ 'Active' ]    
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() )         
    modArgs[ 2 ] = 'Banned'
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    modArgs[ 2 ] = [ 'Banned' ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    modArgs[ 2 ] = [ 'Active', 'Banned' ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    modArgs[ 2 ] = 'eGGs'
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    modArgs[ 2 ] = [ 'eGGs' ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() )      
    modArgs[ 2 ] = [ 'eGGs', 'Banned' ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() )         
    modArgs[ 2 ] = [ None ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() )      
    modArgs[ 2 ] = [ 'eGGs', 'Banned', None ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 

    #Test forth parameter ( reason )
    modArgs = initArgs[:]
    modArgs[ 3 ] = 'Init'
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    modArgs[ 3 ] = [ 'Init' ]    
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() )        
    modArgs[ 3 ] = 'eGGs'
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    modArgs[ 3 ] = [ 'eGGs' ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    modArgs[ 3 ] = [ 'Init', 'eGGs' ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() )           
    modArgs[ 3 ] = [ None ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    modArgs[ 3 ] = [ 'Init', 'eGGs', None ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 

    #Test fifth parameter ( dateCreated )
    modArgs = initArgs[:]
    modArgs[ 4 ] = dSol
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() )   
    modArgs[ 4 ] = [ dSol ]    
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() )         
    modArgs[ 4 ] = dNow
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() )  
    modArgs[ 4 ] = [ dNow ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    modArgs[ 4 ] = [ dSol, dNow ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() )      
    modArgs[ 4 ] = [ None ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertNotEquals( res[ 'OK' ], True ) 
    modArgs[ 4 ] = [ dSol, dNow, None ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertNotEquals( res[ 'OK' ], True ) 
            
    #Test sixth parameter ( dateEffective ) 
    modArgs = initArgs[:]
    modArgs[ 5 ] = dSol
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() )   
    modArgs[ 5 ] = [ dSol ]    
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() )         
    modArgs[ 5 ] = dNow
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() )  
    modArgs[ 5 ] = [ dNow ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    modArgs[ 5 ] = [ dSol, dNow ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() )    
    modArgs[ 5 ] = [ None ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertNotEquals( res[ 'OK' ], True ) 
    modArgs[ 5 ] = [ dSol, dNow, None ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertNotEquals( res[ 'OK' ], True ) 

    #Test seventh parameter ( dateEnd )
    modArgs = initArgs[:]
    modArgs[ 6 ] = dSol
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() )   
    modArgs[ 6 ] = [ dSol ]    
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() )        
    modArgs[ 6 ] = dNow
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    modArgs[ 6 ] = [ dNow ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    modArgs[ 6 ] = [ dSol, dNow ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() )      
    modArgs[ 6 ] = [ None ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertNotEquals( res[ 'OK' ], True ) 
    modArgs[ 6 ] = [ dSol, dNow, None ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertNotEquals( res[ 'OK' ], True ) 
                
    #Test eighth parameter ( lastCheckTime )
    modArgs = initArgs[:]
    modArgs[ 7 ] = dSol
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() )   
    modArgs[ 7 ] = [ dSol ]    
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() )         
    modArgs[ 7 ] = dNow
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() )  
    modArgs[ 7 ] = [ dNow ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    modArgs[ 7 ] = [ dSol, dNow ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() )      
    modArgs[ 7 ] = [ None ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertNotEquals( res[ 'OK' ], True ) 
    modArgs[ 7 ] = [ dSol, dNow, None ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertNotEquals( res[ 'OK' ], True ) 
        
    #Test ninth parameter ( tokenOwner )
    modArgs = initArgs[:]
    modArgs[ 8 ] = 'RS_SVC'
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    modArgs[ 8 ] = [ 'RS_SVC' ]   
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() )    
    modArgs[ 8 ] = 'eGGs'   
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    modArgs[ 8 ] = [ 'eGGs' ]   
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    modArgs[ 8 ] = [ 'RS_SVC','eGGs' ]   
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    modArgs[ 8 ] = [ None ]   
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    modArgs[ 8 ] = [ 'RS_SVC','eGGs', None ]   
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    
    #Test tenth parameter ( tokenExpiration )
    modArgs = initArgs[:]
    modArgs[ 9 ] = dSol
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() )    
    modArgs[ 9 ] = [ dSol ]    
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() )         
    modArgs[ 9 ] = dNow
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    modArgs[ 9 ] = [ dNow ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    modArgs[ 9 ] = [ dSol, dNow ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() )     
    modArgs[ 9 ] = [ None ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertNotEquals( res[ 'OK' ], True ) 
    modArgs[ 9 ] = [ dSol, dNow, None ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertNotEquals( res[ 'OK' ], True ) 

  def test_11_SitesInput_deleteSitesHistory( self ):
    '''
    deleteSitesHistory( self, siteName, statusType, status, reason, 
                        dateCreated, dateEffective, dateEnd, 
                        lastCheckTime, tokenOwner, tokenExpiration, kwargs )
    '''
    dNow = datetime.now()
    
    res = self.rsDB.deleteSitesHistory()
    self.assertNotEqual( res.has_key( 'Value' ), True )   
    res = self.rsDB.deleteSitesHistory( 'LCG.CERN.ch' )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.deleteSitesHistory( 'LCG.CERN.ch', '' )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.deleteSitesHistory( 'LCG.CERN.ch', '', 'eGGs reason' )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.deleteSitesHistory( 'LCG.CERN.ch', '', 'eGGs reason', dNow )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.deleteSitesHistory( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.deleteSitesHistory( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow, dNow )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.deleteSitesHistory( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow, dNow, dNow )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.deleteSitesHistory( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow, dNow, dNow, 'token' )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.deleteSitesHistory( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow, dNow, dNow, 'token', dNow, {}, dNow )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    
    initArgs = [ None,None,None,None,None,None,None,None,None,None ]
    dSol     = datetime(9999, 12, 11, 10, 9, 8)
    sol      = [
                [1L, 'LCG.CERN.ch', '', 'Active', 'Init', dSol, dSol, dSol, dSol, 'RS_SVC', dSol], 
                [2L, 'LCG.CNAF.it', '', 'Banned', 'Init', dSol, dSol, dSol, dSol, 'RS_SVC', dSol]
               ]
        
       
    res = self.rsDB.deleteSitesHistory( *tuple( initArgs ) )
    self.assertNotEquals( res[ 'OK' ], True )       
    
    '''
      Param tests
    '''    
    #Test first param ( siteName )
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'LCG.CERN.ch'
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    modArgs[ 0 ] = [ 'LCG.CERN.ch' ]    
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() )        
    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'LCG.CNAF.it' ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    modArgs[ 0 ] = 'LCG.eGGs.xy'
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    modArgs[ 0 ] = [ 'LCG.eGGs.xy' ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'LCG.eGGs.xy' ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    modArgs[ 0 ] = [ None ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'LCG.eGGs.xy', None ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 

    #Test second param ( statusType ) 
    modArgs = initArgs[:]
    modArgs[ 1 ] = ''
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    modArgs[ 1 ] = [ '' ]    
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() )         
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    modArgs[ 1 ] = [ 'eGGs' ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    modArgs[ 1 ] = [ '', 'eGGs' ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    modArgs[ 1 ] = [ None ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    modArgs[ 1 ] = [ '', 'eGGs', None ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 

    #Test third parameter ( status )
    modArgs = initArgs[:]
    modArgs[ 2 ] = 'Active'
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    modArgs[ 2 ] = [ 'Active' ]    
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() )         
    modArgs[ 2 ] = 'Banned'
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    modArgs[ 2 ] = [ 'Banned' ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    modArgs[ 2 ] = [ 'Active', 'Banned' ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    modArgs[ 2 ] = 'eGGs'
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    modArgs[ 2 ] = [ 'eGGs' ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() )      
    modArgs[ 2 ] = [ 'eGGs', 'Banned' ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() )         
    modArgs[ 2 ] = [ None ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() )      
    modArgs[ 2 ] = [ 'eGGs', 'Banned', None ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 

    #Test forth parameter ( reason )
    modArgs = initArgs[:]
    modArgs[ 3 ] = 'Init'
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    modArgs[ 3 ] = [ 'Init' ]    
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() )        
    modArgs[ 3 ] = 'eGGs'
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    modArgs[ 3 ] = [ 'eGGs' ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    modArgs[ 3 ] = [ 'Init', 'eGGs' ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() )           
    modArgs[ 3 ] = [ None ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    modArgs[ 3 ] = [ 'Init', 'eGGs', None ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 

    #Test fifth parameter ( dateCreated )
    modArgs = initArgs[:]
    modArgs[ 4 ] = dSol
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() )   
    modArgs[ 4 ] = [ dSol ]    
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() )         
    modArgs[ 4 ] = dNow
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() )  
    modArgs[ 4 ] = [ dNow ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    modArgs[ 4 ] = [ dSol, dNow ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() )      
    modArgs[ 4 ] = [ None ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertNotEquals( res[ 'OK' ], True ) 
    modArgs[ 4 ] = [ dSol, dNow, None ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertNotEquals( res[ 'OK' ], True ) 
            
    #Test sixth parameter ( dateEffective ) 
    modArgs = initArgs[:]
    modArgs[ 5 ] = dSol
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() )   
    modArgs[ 5 ] = [ dSol ]    
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() )         
    modArgs[ 5 ] = dNow
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() )  
    modArgs[ 5 ] = [ dNow ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    modArgs[ 5 ] = [ dSol, dNow ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() )    
    modArgs[ 5 ] = [ None ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertNotEquals( res[ 'OK' ], True ) 
    modArgs[ 5 ] = [ dSol, dNow, None ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertNotEquals( res[ 'OK' ], True ) 

    #Test seventh parameter ( dateEnd )
    modArgs = initArgs[:]
    modArgs[ 6 ] = dSol
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() )   
    modArgs[ 6 ] = [ dSol ]    
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() )        
    modArgs[ 6 ] = dNow
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    modArgs[ 6 ] = [ dNow ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    modArgs[ 6 ] = [ dSol, dNow ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() )      
    modArgs[ 6 ] = [ None ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertNotEquals( res[ 'OK' ], True ) 
    modArgs[ 6 ] = [ dSol, dNow, None ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertNotEquals( res[ 'OK' ], True ) 
                
    #Test eighth parameter ( lastCheckTime )
    modArgs = initArgs[:]
    modArgs[ 7 ] = dSol
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() )   
    modArgs[ 7 ] = [ dSol ]    
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() )         
    modArgs[ 7 ] = dNow
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() )  
    modArgs[ 7 ] = [ dNow ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    modArgs[ 7 ] = [ dSol, dNow ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() )      
    modArgs[ 7 ] = [ None ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertNotEquals( res[ 'OK' ], True ) 
    modArgs[ 7 ] = [ dSol, dNow, None ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertNotEquals( res[ 'OK' ], True ) 
        
    #Test ninth parameter ( tokenOwner )
    modArgs = initArgs[:]
    modArgs[ 8 ] = 'RS_SVC'
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    modArgs[ 8 ] = [ 'RS_SVC' ]   
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() )    
    modArgs[ 8 ] = 'eGGs'   
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    modArgs[ 8 ] = [ 'eGGs' ]   
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    modArgs[ 8 ] = [ 'RS_SVC','eGGs' ]   
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    modArgs[ 8 ] = [ None ]   
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    modArgs[ 8 ] = [ 'RS_SVC','eGGs', None ]   
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    
    #Test tenth parameter ( tokenExpiration )
    modArgs = initArgs[:]
    modArgs[ 9 ] = dSol
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() )    
    modArgs[ 9 ] = [ dSol ]    
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() )         
    modArgs[ 9 ] = dNow
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    modArgs[ 9 ] = [ dNow ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() ) 
    modArgs[ 9 ] = [ dSol, dNow ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res, S_OK() )     
    modArgs[ 9 ] = [ None ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertNotEquals( res[ 'OK' ], True ) 
    modArgs[ 9 ] = [ dSol, dNow, None ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertNotEquals( res[ 'OK' ], True )       

  def test_12_SitesInput_deleteSites( self ):
    '''
    deleteSites( self, siteName )
    '''

    res = self.rsDB.deleteSites()
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.deleteSites( 'eGGs', None )
    self.assertNotEqual( res.has_key( 'Value' ), True )

    #Test first param
    res = self.rsDB.deleteSites( 'LCG.CERN.ch' )
    self.assertEqual( res, S_OK() )
    res = self.rsDB.deleteSites( [ 'LCG.CERN.ch' ] )
    self.assertEqual( res, S_OK() )
    res = self.rsDB.deleteSites( 'eGGs' )
    self.assertEqual( res, S_OK() )
    res = self.rsDB.deleteSites( [ 'eGGs' ] )
    self.assertEqual( res, S_OK() )
    res = self.rsDB.deleteSites( None )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.deleteSites( [ None ] )
    self.assertEqual( res, S_OK() )
    res = self.rsDB.deleteSites( [ 'LCG.CERN.ch', None ] )
    self.assertEqual( res, S_OK() )

class Test_ServicesInput( Test_ResourceStatusDB ):
  
  def test_01_ServicesInput_addOrModifyService( self ):
    '''
    addOrModifySite( self, serviceName, serviceType, siteName )
    '''
    res = self.rsDB.addOrModifyService()
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.addOrModifyService( 'Computing@LCG.CERN.ch' )
    self.assertNotEqual( res.has_key( 'Value' ), True )          
    res = self.rsDB.addOrModifyService( 'Computing@LCG.CERN.ch', 'Computing' )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.addOrModifyService( 'Computing@LCG.CERN.ch', 'Computing', 'LCG.CERN.ch', None )
    
    initArgs = [ None, None, None ]
    sol      = [
                [ 'Computing@LCG.CERN.ch', 'Computing', 'LCG.CERN.ch' ],
                [ 'Storage@LCG.CERN.ch', 'Storage', 'LCG.CERN.ch' ]
               ]
    
    #Test first parameter ( siteName )
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'Computing@LCG.CERN.ch'
    res = self.rsDB.addOrModifyService( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 0 ] = [ 'Computing@LCG.CERN.ch' ]
    res = self.rsDB.addOrModifyService( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 0 ] = 'eGGs'
    res = self.rsDB.addOrModifyService( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 0 ] = [ 'eGGs' ]
    res = self.rsDB.addOrModifyService( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 0 ] = [ 'Computing@LCG.CERN.ch', 'eGGs' ]
    res = self.rsDB.addOrModifyService( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 0 ] = [ None ]
    res = self.rsDB.addOrModifyService( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 0 ] = [ 'Computing@LCG.CERN.ch', 'eGGs', None ]
    res = self.rsDB.addOrModifyService( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )

    #Test second parameter ( siteType )
    modArgs = initArgs[:]
    modArgs[ 1 ] = 'Computing'
    res = self.rsDB.addOrModifyService( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 1 ] = [ 'Computing' ]
    res = self.rsDB.addOrModifyService( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.addOrModifyService( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 1 ] = [ 'eGGs' ]
    res = self.rsDB.addOrModifyService( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 1 ] = [ 'Computing', 'eGGs' ]
    res = self.rsDB.addOrModifyService( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 1 ] = [ None ]
    res = self.rsDB.addOrModifyService( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 1 ] = [ 'Computing', 'eGGs', None ]
    res = self.rsDB.addOrModifyService( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )    

    #Test third parameter ( siteType )
    modArgs = initArgs[:]
    modArgs[ 2 ] = 'LCG.CERN.ch'
    res = self.rsDB.addOrModifyService( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 2 ] = [ 'LCG.CERN.ch' ]
    res = self.rsDB.addOrModifyService( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 2 ] = 'eGGs'
    res = self.rsDB.addOrModifyService( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 2 ] = [ 'eGGs' ]
    res = self.rsDB.addOrModifyService( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 2 ] = [ 'LCG.CERN.ch', 'eGGs' ]
    res = self.rsDB.addOrModifyService( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 2 ] = [ None ]
    res = self.rsDB.addOrModifyService( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    modArgs[ 2 ] = [ 'LCG.CERN.ch', 'eGGs', None ]
    res = self.rsDB.addOrModifyService( *tuple( modArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )     
    
    '''
      Mixed tests
    '''
    
    '''
      Kwargs tests...
    '''
    
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.addOrModifyService( None, None, None )
    self.assertNotEqual( res.has_key( 'Value' ), True )   
    res = self.rsDB.addOrModifyService( 'Computing@LCG.CERN.ch', None, None )
    self.assertNotEqual( res.has_key( 'Value' ), True )   
    res = self.rsDB.addOrModifyService( 'Computing@LCG.CERN.ch', None, 'LCG.CERN.ch' )
    self.assertNotEqual( res.has_key( 'Value' ), True )   
    res = self.rsDB.addOrModifyService( 'Computing@LCG.CERN.ch', 'eGGs', 'LCG.CERN.ch' )
    self.assertNotEqual( res.has_key( 'Value' ), True )   
    # This one looks correct, it is correct, but Sites table is empty !!
    res = self.rsDB.addOrModifyService( 'Computing@LCG.CERN.ch', 'Computing', 'LCG.CERN.ch' )
    self.assertNotEqual( res.has_key( 'Value' ), True )   
    
    #self.rsDB.addOrModifyGridSite( 'CERN-PROD', 'T0')
    #self.rsDB.addOrModifyGridSite( 'INFN-T1',   'T1')
    
    res = self.rsDB.addOrModifyService( *tuple( sol[ 0 ]) )
    self.assertEqual( res, S_OK() )   
    res = self.rsDB.addOrModifyService( *tuple( sol[ 1 ]) )
    self.assertEqual( res, S_OK() )   
    modArgs = sol[0][:]
    modArgs[ 1 ] = 'Storage'
    res = self.rsDB.addOrModifyService( *tuple( modArgs ) )
    self.assertEqual( res, S_OK() )   
    modArgs[ 2 ] = 'LCG.CNAF.it'
    res = self.rsDB.addOrModifyService( *tuple( modArgs ) )
    self.assertEqual( res, S_OK() )   
    res = self.rsDB.addOrModifyService( *tuple( sol[ 0 ] ) )
    self.assertEqual( res, S_OK() )   
    res = self.rsDB.addOrModifyService( *tuple( sol[ 0 ] ) )
    self.assertEqual( res, S_OK() )   
           
#  def test_02_SitesInput_setSiteStatus( self ):
#    '''
#    setSiteStatus( self, siteName, statusType, status, reason, dateCreated, 
#                   dateEffective, dateEnd, lastCheckTime, tokenOwner, 
#                   tokenExpiration ) 
#    '''
#    dNow = datetime.now()
#    
#    res = self.rsDB.setSiteStatus()
#    self.assertNotEqual( res.has_key( 'Value' ), True )   
#    res = self.rsDB.setSiteStatus( 'LCG.CERN.ch' )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.setSiteStatus( 'LCG.CERN.ch', '' )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.setSiteStatus( 'LCG.CERN.ch', '', 'eGGs reason' )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.setSiteStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.setSiteStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.setSiteStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow, dNow )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.setSiteStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow, dNow, dNow )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.setSiteStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow, dNow, dNow, 'token' )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.setSiteStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow, dNow, dNow, 'token', dNow, {}, dNow )
#    
#    initArgs = [ None, None, None, None, None, None, None, None, None, None ]
#    
#    dSol= datetime( 9999, 12, 11, 10, 9, 8 )
#    sol = [
#           ['LCG.CERN.ch', '', 'Active', 'Init', dSol, dSol, dSol, dSol, 'RS_SVC', dSol], 
#           ['LCG.CNAF.it', '', 'Banned', 'Init', dSol, dSol, dSol, dSol, 'RS_SVC', dSol]
#          ]
#    
#    res = self.rsDB.setSiteStatus( *tuple( initArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    
#    #Test first parameter ( siteName )
#    modArgs = initArgs[:]
#    modArgs[ 0 ] = 'LCG.CERN.ch'
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 0 ] = [ 'LCG.CERN.ch' ]
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 0 ] = 'eGGs'
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 0 ] = [ 'eGGs' ]
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'eGGs' ]
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 0 ] = [ None ]
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'eGGs', None ]
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    
#    #Test second parameter ( statusType )
#    modArgs[ 1 ] = ''
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 1 ] = [ '' ]
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 1 ] = 'eGGs'
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 1 ] = [ 'eGGs' ]
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 1 ] = [ '', 'eGGs' ]
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 1 ] = [ None ]
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 1 ] = [ '', 'eGGs', None ]
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#
#    #Test third parameter ( status )
#    modArgs[ 2 ] = 'Active'
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 2 ] = [ 'Active' ]
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 2 ] = 'eGGs'
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 2 ] = [ 'eGGs' ]
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 2 ] = [ 'Active', 'eGGs' ]
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 2 ] = [ None ]
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 2 ] = [ 'Active', 'eGGs', None ]
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    
#    #Test forth parameter ( reason )
#    modArgs = initArgs[:]
#    modArgs[ 3 ] = 'Init'
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 3 ] = [ 'Init' ]    
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True ) 
#    modArgs[ 3 ] = 'eGGs'
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 3 ] = [ 'eGGs' ]
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 3 ] = [ 'Init', 'eGGs' ]
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 3 ] = [ None ]
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )         
#    modArgs[ 3 ] = [ 'Init', 'eGGs', None ]
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )  
#
#    #Test fifth parameter ( dateCreated )
#    modArgs = initArgs[:]
#    modArgs[ 4 ] = dSol
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True ) 
#    modArgs[ 4 ] = [ dSol ]    
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )     
#    modArgs[ 4 ] = dNow
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True ) 
#    modArgs[ 4 ] = [ dNow ]
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True ) 
#    modArgs[ 4 ] = [ dSol, dNow ]
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True ) 
#    modArgs[ 4 ] = [ None ]
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 4 ] = [ dSol, dNow, None ]
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True ) 
#            
#    #Test sixth parameter ( dateEffective ) 
#    modArgs = initArgs[:]
#    modArgs[ 5 ] = dSol
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )    
#    modArgs[ 5 ] = [ dSol ]    
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )         
#    modArgs[ 5 ] = dNow
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )  
#    modArgs[ 5 ] = [ dNow ]
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True ) 
#    modArgs[ 5 ] = [ dSol, dNow ]
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )     
#    modArgs[ 5 ] = [ None ]
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 5 ] = [ dSol, dNow, None ]
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )     
#
#    #Test seventh parameter ( dateEnd )
#    modArgs = initArgs[:]
#    modArgs[ 6 ] = dSol
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )   
#    modArgs[ 6 ] = [ dSol ]    
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )        
#    modArgs[ 6 ] = dNow
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True ) 
#    modArgs[ 6 ] = [ dNow ]
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 6 ] = [ dSol, dNow ]
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True ) 
#    modArgs[ 6 ] = [ None ]
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 6 ] = [ dSol, dNow, None ]
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True ) 
#            
#    #Test eighth parameter ( lastCheckTime )
#    modArgs = initArgs[:]
#    modArgs[ 7 ] = dSol
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )   
#    modArgs[ 7 ] = [ dSol ]    
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )        
#    modArgs[ 7 ] = dNow
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True ) 
#    modArgs[ 7 ] = [ dNow ]
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 7 ] = [ dSol, dNow ]
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 7 ] = [ None ]
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 7 ] = [ dSol, dNow, None ]
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    
#    #Test ninth parameter ( tokenOwner )
#    modArgs = initArgs[:]
#    modArgs[ 8 ] = 'RS_SVC'
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 8 ] = [ 'RS_SVC' ]   
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )   
#    modArgs[ 8 ] = 'eGGs'   
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 8 ] = [ 'eGGs' ]   
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 8 ] = [ 'RS_SVC','eGGs' ]   
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 8 ] = [ None ]   
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 8 ] = [ 'RS_SVC','eGGs', None ]   
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    
#    #Test tenth parameter ( tokenExpiration )
#    modArgs = initArgs[:]
#    modArgs[ 9 ] = dSol
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )   
#    modArgs[ 9 ] = [ dSol ]    
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )        
#    modArgs[ 9 ] = dNow
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True ) 
#    modArgs[ 9 ] = [ dNow ]
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 9 ] = [ dSol, dNow ]
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 9 ] = [ None ]
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )    
#    modArgs[ 9 ] = [ dSol, dNow, None ]
#    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    
#    ##############
#    
#    '''
#      Mixed tests
#    '''
#    
#    '''
#      Kwargs tests...
#    '''
#    
#    res = self.rsDB.setSiteStatus( 'LCG.CERN.ch', '', 'Active', None, None,
#                                   None,None,None,None,None )
#    self.assertEqual( res, S_OK() )
#    res = self.rsDB.setSiteStatus( 'LCG.CERN.ch', '', 'Active', None, None,
#                                   None, None, None, None, None )
#    self.assertEqual( res, S_OK() )
#    
#    res = self.rsDB.setSiteStatus( ['LCG.CERN.ch'], '', 'Active', None, None,
#                                   None, None, None, None, None )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.setSiteStatus( 'LCG.CERN.ch', [''], 'Active', None, None,
#                                   None, None, None, None, None )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.setSiteStatus( 'LCG.CERN.ch', '', ['Active'], None, None,
#                                   None, None, None, None, None )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.setSiteStatus( 'LCG.CERN.ch', '', 'Active', None, 'eGGs',
#                                   None, None, None, None, None )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.setSiteStatus( 'LCG.CERN.ch', '', 'Active', None, None,
#                                   'eGGs', None, None, None, None )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.setSiteStatus( 'LCG.CERN.ch', '', 'Active', None, None,
#                                   None, 'eGGs', None, None, None )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.setSiteStatus( 'LCG.CERN.ch', '', 'Active', None, None,
#                                   None, None, 'eGGs', None, None )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.setSiteStatus( 'LCG.CERN.ch', '', 'Active', None, None,
#                                   None, None, None, None, 'eGGs' )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    
#    now = datetime.now()
#    
#    res = self.rsDB.setSiteStatus( 'LCG.CERN.ch', '', 'Active', None, now,
#                                   now, now, now, None, now )
#    self.assertEqual( res, S_OK() )
#    res = self.rsDB.setSiteStatus( 'eGGs', '', 'Active', None, now,
#                                   now, now, now, None, now )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.setSiteStatus( 'LCG.CERN.ch', 'eGGs', 'Active', None, now,
#                                   now, now, now, None, now )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.setSiteStatus( 'LCG.CERN.ch', '', 'eGGs', None, now,
#                                   now, now, now, None, now )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#
#  def test_03_SitesInput_setSiteScheduledStatus( self ):
#    '''
#    setSiteScheduledStatus( self, siteName, statusType, status, reason, dateCreated, 
#                            dateEffective, dateEnd, lastCheckTime, tokenOwner, 
#                            tokenExpiration )
#    '''   
#    dNow = datetime.now()
#    
#    res = self.rsDB.setSiteScheduledStatus()
#    self.assertNotEqual( res.has_key( 'Value' ), True )   
#    res = self.rsDB.setSiteScheduledStatus( 'LCG.CERN.ch' )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.setSiteScheduledStatus( 'LCG.CERN.ch', '' )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.setSiteScheduledStatus( 'LCG.CERN.ch', '', 'eGGs reason' )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.setSiteScheduledStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.setSiteScheduledStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.setSiteScheduledStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow, dNow )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.setSiteScheduledStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow, dNow, dNow )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.setSiteScheduledStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow, dNow, dNow, 'token' )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.setSiteScheduledStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow, dNow, dNow, 'token', dNow, {}, dNow )
#    
#    initArgs = [ None, None, None, None, None, None, None, None, None, None ]
#    
#    dSol= datetime( 9999, 12, 11, 10, 9, 8 )
#    sol = [
#           ['LCG.CERN.ch', '', 'Active', 'Init', dSol, dSol, dSol, dSol, 'RS_SVC', dSol], 
#           ['LCG.CNAF.it', '', 'Banned', 'Init', dSol, dSol, dSol, dSol, 'RS_SVC', dSol]
#          ]
#    
#    res = self.rsDB.setSiteScheduledStatus( *tuple( initArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    
#    #Test first parameter ( siteName )
#    modArgs = initArgs[:]
#    modArgs[ 0 ] = 'LCG.CERN.ch'
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 0 ] = [ 'LCG.CERN.ch' ]
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 0 ] = 'eGGs'
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 0 ] = [ 'eGGs' ]
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'eGGs' ]
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 0 ] = [ None ]
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'eGGs', None ]
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    
#    #Test second parameter ( statusType )
#    modArgs[ 1 ] = ''
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 1 ] = [ '' ]
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 1 ] = 'eGGs'
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 1 ] = [ 'eGGs' ]
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 1 ] = [ '', 'eGGs' ]
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 1 ] = [ None ]
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 1 ] = [ '', 'eGGs', None ]
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#
#    #Test third parameter ( status )
#    modArgs[ 2 ] = 'Active'
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 2 ] = [ 'Active' ]
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 2 ] = 'eGGs'
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 2 ] = [ 'eGGs' ]
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 2 ] = [ 'Active', 'eGGs' ]
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 2 ] = [ None ]
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 2 ] = [ 'Active', 'eGGs', None ]
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    
#    #Test forth parameter ( reason )
#    modArgs = initArgs[:]
#    modArgs[ 3 ] = 'Init'
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 3 ] = [ 'Init' ]    
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True ) 
#    modArgs[ 3 ] = 'eGGs'
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 3 ] = [ 'eGGs' ]
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 3 ] = [ 'Init', 'eGGs' ]
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 3 ] = [ None ]
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )         
#    modArgs[ 3 ] = [ 'Init', 'eGGs', None ]
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )  
#
#    #Test fifth parameter ( dateCreated )
#    modArgs = initArgs[:]
#    modArgs[ 4 ] = dSol
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True ) 
#    modArgs[ 4 ] = [ dSol ]    
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )     
#    modArgs[ 4 ] = dNow
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True ) 
#    modArgs[ 4 ] = [ dNow ]
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True ) 
#    modArgs[ 4 ] = [ dSol, dNow ]
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True ) 
#    modArgs[ 4 ] = [ None ]
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 4 ] = [ dSol, dNow, None ]
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True ) 
#            
#    #Test sixth parameter ( dateEffective ) 
#    modArgs = initArgs[:]
#    modArgs[ 5 ] = dSol
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )    
#    modArgs[ 5 ] = [ dSol ]    
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )         
#    modArgs[ 5 ] = dNow
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )  
#    modArgs[ 5 ] = [ dNow ]
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True ) 
#    modArgs[ 5 ] = [ dSol, dNow ]
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )     
#    modArgs[ 5 ] = [ None ]
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 5 ] = [ dSol, dNow, None ]
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )     
#
#    #Test seventh parameter ( dateEnd )
#    modArgs = initArgs[:]
#    modArgs[ 6 ] = dSol
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )   
#    modArgs[ 6 ] = [ dSol ]    
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )        
#    modArgs[ 6 ] = dNow
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True ) 
#    modArgs[ 6 ] = [ dNow ]
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 6 ] = [ dSol, dNow ]
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True ) 
#    modArgs[ 6 ] = [ None ]
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 6 ] = [ dSol, dNow, None ]
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True ) 
#            
#    #Test eighth parameter ( lastCheckTime )
#    modArgs = initArgs[:]
#    modArgs[ 7 ] = dSol
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )   
#    modArgs[ 7 ] = [ dSol ]    
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )        
#    modArgs[ 7 ] = dNow
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True ) 
#    modArgs[ 7 ] = [ dNow ]
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 7 ] = [ dSol, dNow ]
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 7 ] = [ None ]
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 7 ] = [ dSol, dNow, None ]
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    
#    #Test ninth parameter ( tokenOwner )
#    modArgs = initArgs[:]
#    modArgs[ 8 ] = 'RS_SVC'
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 8 ] = [ 'RS_SVC' ]   
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )   
#    modArgs[ 8 ] = 'eGGs'   
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 8 ] = [ 'eGGs' ]   
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 8 ] = [ 'RS_SVC','eGGs' ]   
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 8 ] = [ None ]   
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 8 ] = [ 'RS_SVC','eGGs', None ]   
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    
#    #Test tenth parameter ( tokenExpiration )
#    modArgs = initArgs[:]
#    modArgs[ 9 ] = dSol
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )   
#    modArgs[ 9 ] = [ dSol ]    
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )        
#    modArgs[ 9 ] = dNow
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True ) 
#    modArgs[ 9 ] = [ dNow ]
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 9 ] = [ dSol, dNow ]
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 9 ] = [ None ]
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )    
#    modArgs[ 9 ] = [ dSol, dNow, None ]
#    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    
#    '''
#      Mixed tests
#    '''
#    
#    '''
#      Kwargs tests...
#    '''
#    
#    now = datetime.now()
#    
##    res = self.rsDB.setSiteScheduledStatus( 'LCG.CERN.ch', '', 'Active', None, now,
##                                            now, now, now, None, now )
##    self.assertEqual( res, S_OK() )
#    res = self.rsDB.setSiteScheduledStatus( 'eGGs', '', 'Active', None, now,
#                                            now, now, now, None, now )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.setSiteScheduledStatus( 'LCG.CERN.ch', 'eGGs', 'Active', None, now,
#                                            now, now, now, None, now )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.setSiteScheduledStatus( 'LCG.CERN.ch', '', 'eGGs', None, now,
#                                            now, now, now, None, now )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    
#  def test_04_SitesInput_updateSiteStatus( self ):
#    '''
#    updateSiteStatus( self, siteName, statusType, status, reason, dateCreated, 
#                      dateEffective, dateEnd, lastCheckTime, tokenOwner, 
#                      tokenExpiration )
#    '''
#    dNow = datetime.now()
#    
#    res = self.rsDB.updateSiteStatus()
#    self.assertNotEqual( res.has_key( 'Value' ), True )   
#    res = self.rsDB.updateSiteStatus( 'LCG.CERN.ch' )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.updateSiteStatus( 'LCG.CERN.ch', '' )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.updateSiteStatus( 'LCG.CERN.ch', '', 'eGGs reason' )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.updateSiteStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.updateSiteStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.updateSiteStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow, dNow )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.updateSiteStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow, dNow, dNow )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.updateSiteStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow, dNow, dNow, 'token' )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.updateSiteStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow, dNow, dNow, 'token', dNow, {}, dNow )
#    
#    initArgs = [ None, None, None, None, None, None, None, None, None, None ]
#    
#    dSol= datetime( 9999, 12, 11, 10, 9, 8 )
#    sol = [
#           [ 1L, 'LCG.CERN.ch', '', 'Active', 'Init', dSol, dSol, dSol, dSol, 'RS_SVC', dSol], 
#           [ 2L, 'LCG.CNAF.it', '', 'Banned', 'Init', dSol, dSol, dSol, dSol, 'RS_SVC', dSol]
#          ]
#    
#    res = self.rsDB.updateSiteStatus( *tuple( initArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    
#    '''
#      Param tests
#    '''    
#    #Test first parameter ( siteName )
#    modArgs = initArgs[:]
#    modArgs[ 0 ] = 'LCG.CERN.ch'
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 0 ] = [ 'LCG.CERN.ch' ]
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 0 ] = 'eGGs'
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 0 ] = [ 'eGGs' ]
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'eGGs' ]
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 0 ] = [ None ]
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'eGGs', None ]
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    
#    #Test second parameter ( statusType )
#    modArgs[ 1 ] = ''
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 1 ] = [ '' ]
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 1 ] = 'eGGs'
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 1 ] = [ 'eGGs' ]
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 1 ] = [ '', 'eGGs' ]
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 1 ] = [ None ]
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 1 ] = [ '', 'eGGs', None ]
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#
#    #Test third parameter ( status )
#    modArgs[ 2 ] = 'Active'
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 2 ] = [ 'Active' ]
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 2 ] = 'eGGs'
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 2 ] = [ 'eGGs' ]
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 2 ] = [ 'Active', 'eGGs' ]
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 2 ] = [ None ]
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 2 ] = [ 'Active', 'eGGs', None ]
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    
#    #Test forth parameter ( reason )
#    modArgs = initArgs[:]
#    modArgs[ 3 ] = 'Init'
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 3 ] = [ 'Init' ]    
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True ) 
#    modArgs[ 3 ] = 'eGGs'
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 3 ] = [ 'eGGs' ]
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 3 ] = [ 'Init', 'eGGs' ]
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 3 ] = [ None ]
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )         
#    modArgs[ 3 ] = [ 'Init', 'eGGs', None ]
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )  
#
#    #Test fifth parameter ( dateCreated )
#    modArgs = initArgs[:]
#    modArgs[ 4 ] = dSol
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True ) 
#    modArgs[ 4 ] = [ dSol ]    
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )     
#    modArgs[ 4 ] = dNow
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True ) 
#    modArgs[ 4 ] = [ dNow ]
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True ) 
#    modArgs[ 4 ] = [ dSol, dNow ]
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True ) 
#    modArgs[ 4 ] = [ None ]
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 4 ] = [ dSol, dNow, None ]
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True ) 
#            
#    #Test sixth parameter ( dateEffective ) 
#    modArgs = initArgs[:]
#    modArgs[ 5 ] = dSol
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )    
#    modArgs[ 5 ] = [ dSol ]    
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )         
#    modArgs[ 5 ] = dNow
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )  
#    modArgs[ 5 ] = [ dNow ]
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True ) 
#    modArgs[ 5 ] = [ dSol, dNow ]
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )     
#    modArgs[ 5 ] = [ None ]
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 5 ] = [ dSol, dNow, None ]
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )     
#
#    #Test seventh parameter ( dateEnd )
#    modArgs = initArgs[:]
#    modArgs[ 6 ] = dSol
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )   
#    modArgs[ 6 ] = [ dSol ]    
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )        
#    modArgs[ 6 ] = dNow
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True ) 
#    modArgs[ 6 ] = [ dNow ]
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 6 ] = [ dSol, dNow ]
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True ) 
#    modArgs[ 6 ] = [ None ]
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 6 ] = [ dSol, dNow, None ]
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True ) 
#            
#    #Test eighth parameter ( lastCheckTime )
#    modArgs = initArgs[:]
#    modArgs[ 7 ] = dSol
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )   
#    modArgs[ 7 ] = [ dSol ]    
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )        
#    modArgs[ 7 ] = dNow
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True ) 
#    modArgs[ 7 ] = [ dNow ]
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 7 ] = [ dSol, dNow ]
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 7 ] = [ None ]
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 7 ] = [ dSol, dNow, None ]
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    
#    #Test ninth parameter ( tokenOwner )
#    modArgs = initArgs[:]
#    modArgs[ 8 ] = 'RS_SVC'
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 8 ] = [ 'RS_SVC' ]   
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )   
#    modArgs[ 8 ] = 'eGGs'   
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 8 ] = [ 'eGGs' ]   
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 8 ] = [ 'RS_SVC','eGGs' ]   
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 8 ] = [ None ]   
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 8 ] = [ 'RS_SVC','eGGs', None ]   
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    
#    #Test tenth parameter ( tokenExpiration )
#    modArgs = initArgs[:]
#    modArgs[ 9 ] = dSol
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )   
#    modArgs[ 9 ] = [ dSol ]    
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )        
#    modArgs[ 9 ] = dNow
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True ) 
#    modArgs[ 9 ] = [ dNow ]
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 9 ] = [ dSol, dNow ]
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    modArgs[ 9 ] = [ None ]
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )    
#    modArgs[ 9 ] = [ dSol, dNow, None ]
#    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    
#    '''
#      Mixed tests
#    '''
#    
#    '''
#      Kwargs tests... TO BE IMPROVED
#    '''
#    
#    
#    res = self.rsDB.updateSiteStatus( 'LCG.CERN.ch', '', 'Active', None, None,
#                                       None,None,None,None,None )
#    self.assertEqual( res, S_OK() )
#    
#    res = self.rsDB.updateSiteStatus( ['LCG.CERN.ch'], '', 'Active', None, None,
#                                            None, None, None, None, None )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.updateSiteStatus( 'LCG.CERN.ch', [''], 'Active', None, None,
#                                            None, None, None, None, None )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.updateSiteStatus( 'LCG.CERN.ch', '', ['Active'], None, None,
#                                            None, None, None, None, None )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.updateSiteStatus( 'LCG.CERN.ch', '', 'Active', None, 'eGGs',
#                                            None, None, None, None, None )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.updateSiteStatus( 'LCG.CERN.ch', '', 'Active', None, None,
#                                            'eGGs', None, None, None, None )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.updateSiteStatus( 'LCG.CERN.ch', '', 'Active', None, None,
#                                            None, 'eGGs', None, None, None )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.updateSiteStatus( 'LCG.CERN.ch', '', 'Active', None, None,
#                                            None, None, 'eGGs', None, None )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.updateSiteStatus( 'LCG.CERN.ch', '', 'Active', None, None,
#                                            None, None, None, None, 'eGGs' )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    
#    now = datetime.now()
#    
#    res = self.rsDB.updateSiteStatus( 'LCG.CERN.ch', '', 'Active', None, now,
#                                            now, now, now, None, now )
#    self.assertEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.updateSiteStatus( 'eGGs', '', 'Active', None, now,
#                                            now, now, now, None, now )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.updateSiteStatus( 'LCG.CERN.ch', 'eGGs', 'Active', None, now,
#                                            now, now, now, None, now )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.updateSiteStatus( 'LCG.CERN.ch', '', 'eGGs', None, now,
#                                       now, now, now, None, now )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    
#    
#    # Used for next rounds
#    
#    res = self.rsDB.setSiteStatus( *tuple( sol[ 0 ][1:] ) )
#    self.assertEqual( res, S_OK() ) 
#    res = self.rsDB.setSiteStatus( *tuple( sol[ 1 ][1:] ) )
#    self.assertEqual( res, S_OK() )    
#        
#  def test_05_SitesInput_getSites( self ):
#    '''
#    getSites( self, siteName, siteType, gridSiteName, **kwargs )
#    '''
#    res = self.rsDB.getSites()
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.getSites( 'LCG.CERN.ch' )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.getSites( 'LCG.CERN.ch', 'T0' )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.getSites( 'LCG.CERN.ch', 'T0', 'eGGs', 'eGGs' )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    
#    initArgs = [ None, None, None ]
#    sol      = [
#                ['LCG.CERN.ch', 'T0', 'CERN-PROD'],
#                ['LCG.CNAF.it', 'T1', 'INFN-T1'] 
#               ]
#    
#    res = self.rsDB.getSites( *tuple( initArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )  
#    
#    '''
#      Param tests
#    '''    
#    #Test first param ( siteName )
#    modArgs = initArgs[:]
#    modArgs[ 0 ] = 'LCG.CERN.ch'
#    res = self.rsDB.getSites( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [ sol[ 0 ] ] )  
#    modArgs[ 0 ] = [ 'LCG.CNAF.it' ]
#    res = self.rsDB.getSites( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [ sol[ 1 ] ] )  
#    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'LCG.CNAF.it' ]
#    res = self.rsDB.getSites( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )  
#    modArgs[ 0 ] = 'eGGs'
#    res = self.rsDB.getSites( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] )
#    modArgs[ 0 ] = [ 'eGGs' ]
#    res = self.rsDB.getSites( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] )
#    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'LCG.CNAF.it', 'eGGs' ]
#    res = self.rsDB.getSites( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )  
#    modArgs[ 0 ] = [ None ]
#    res = self.rsDB.getSites( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] )
#    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'LCG.CNAF.it', 'eGGs', None ]
#    res = self.rsDB.getSites( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )  
#
#    #Test second param ( siteType )
#    modArgs = initArgs[:]
#    modArgs[ 1 ] = 'T0'
#    res = self.rsDB.getSites( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [ sol[ 0 ] ] )
#    modArgs[ 1 ] = [ 'T1' ]
#    res = self.rsDB.getSites( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [ sol[ 1 ] ] )
#    modArgs[ 1 ] = [ 'T0', 'T1' ]
#    res = self.rsDB.getSites( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )
#    modArgs[ 1 ] = 'eGGs'
#    res = self.rsDB.getSites( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] )
#    modArgs[ 1 ] = [ 'eGGs' ]
#    res = self.rsDB.getSites( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] )
#    modArgs[ 1 ] = [ 'T0', 'T1', 'eGGs' ]
#    res = self.rsDB.getSites( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )
#    modArgs[ 1 ] = [ None ]
#    res = self.rsDB.getSites( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] )
#    modArgs[ 1 ] = [ 'T0', 'T1', 'eGGs', None ]
#    res = self.rsDB.getSites( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )
#
#    #Test third param ( gridSiteName )
#    modArgs = initArgs[:]
#    modArgs[ 2 ] = 'CERN-PROD'
#    res = self.rsDB.getSites( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [ sol[ 0 ] ] )
#    modArgs[ 2 ] = [ 'INFN-T1' ]
#    res = self.rsDB.getSites( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [ sol[ 1 ] ] )
#    modArgs[ 2 ] = [ 'CERN-PROD', 'INFN-T1' ]
#    res = self.rsDB.getSites( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )
#    modArgs[ 2 ] = 'eGGs'
#    res = self.rsDB.getSites( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] )
#    modArgs[ 2 ] = [ 'eGGs' ]
#    res = self.rsDB.getSites( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] )
#    modArgs[ 2 ] = [ 'CERN-PROD', 'INFN-T1', 'eGGs' ]
#    res = self.rsDB.getSites( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )
#    modArgs[ 2 ] = [ None ]
#    res = self.rsDB.getSites( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] )
#    modArgs[ 2 ] = [ 'CERN-PROD', 'INFN-T1', 'eGGs', None ]
#    res = self.rsDB.getSites( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )
#    
#    '''
#      Mixed tests
#    '''
#    
#    '''
#      Kwargs tests... TO BE IMPROVED
#    '''
#    
#    kwargs = {}
#    res = self.rsDB.getSites( None, None, None, **kwargs )
#    self.assertEqual( res[ 'Value' ], [['LCG.CERN.ch', 'T0', 'CERN-PROD'], ['LCG.CNAF.it', 'T1', 'INFN-T1']] )
#    kwargs = { 'columns' : 'eGGs' }
#    res = self.rsDB.getSites( None, None, None, **kwargs )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    kwargs = { 'columns' : 'SiteName' }
#    res = self.rsDB.getSites( None, None, None, **kwargs )
#    self.assertEqual( res[ 'Value' ], [['LCG.CERN.ch'], ['LCG.CNAF.it']] )
#    res = self.rsDB.getSites( 'LCG.CERN.ch', None, None, **kwargs )
#    self.assertEqual( res[ 'Value' ], [['LCG.CERN.ch']] )
#    res = self.rsDB.getSites( [ 'LCG.CERN.ch' ], None, None, **kwargs )
#    self.assertEqual( res[ 'Value' ], [['LCG.CERN.ch']] )
#    res = self.rsDB.getSites( [ 'LCG.CERN.ch', 'LCG.CNAF.it' ], None, None, **kwargs )
#    self.assertEqual( res[ 'Value' ], [['LCG.CERN.ch'], ['LCG.CNAF.it']] )
#    res = self.rsDB.getSites( None, 'T0', None, **kwargs )
#    self.assertEqual( res[ 'Value' ], [['LCG.CERN.ch']] )
#    res = self.rsDB.getSites( None, None, 'CERN-PROD', **kwargs )
#    self.assertEqual( res[ 'Value' ], [['LCG.CERN.ch']] )
#    kwargs = { 'columns' : 'SiteType' }
#    res = self.rsDB.getSites( None, None, None, **kwargs )
#    self.assertEqual( res[ 'Value' ], [['T0'], ['T1']] )
#    kwargs = { 'columns' : [ 'SiteName', 'SiteType' ] }
#    res = self.rsDB.getSites( None, None, None, **kwargs )
#    self.assertEqual( res[ 'Value' ], [['LCG.CERN.ch', 'T0'], ['LCG.CNAF.it', 'T1']] )
#    kwargs = { 'columns' : [ 'SiteType', 'SiteName' ] }
#    res = self.rsDB.getSites( None, None, None, **kwargs )
#    self.assertEqual( res[ 'Value' ], [['T0','LCG.CERN.ch'], ['T1','LCG.CNAF.it']] )
#    kwargs = { 'columns' : [ 'SiteName', 'SiteType', 'GridSiteName',  ] }
#    res = self.rsDB.getSites( None, None, None, **kwargs )
#    self.assertEqual( res[ 'Value' ], [['LCG.CERN.ch', 'T0', 'CERN-PROD'], ['LCG.CNAF.it', 'T1', 'INFN-T1']] )
#
#  def test_06_SitesInput_getSitesStatus( self ):
#    '''
#    getSitesStatus( self, siteName, statusType, status, reason, dateCreated, 
#                    dateEffective, dateEnd, lastCheckTime, tokenOwner, 
#                    tokenExpiration, **kwargs )
#    '''
#    dNow = datetime.now()
#    
#    res = self.rsDB.getSitesStatus()
#    self.assertNotEqual( res.has_key( 'Value' ), True )   
#    res = self.rsDB.getSitesStatus( 'LCG.CERN.ch' )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.getSitesStatus( 'LCG.CERN.ch', '' )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.getSitesStatus( 'LCG.CERN.ch', '', 'eGGs reason' )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.getSitesStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.getSitesStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.getSitesStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow, dNow )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.getSitesStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow, dNow, dNow )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.getSitesStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow, dNow, dNow, 'token' )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.getSitesStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow, dNow, dNow, 'token', dNow, {}, dNow )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    
#    initArgs = [ None,None,None,None,None,None,None,None,None,None ]
#    dSol     = datetime(9999, 12, 11, 10, 9, 8)
#    sol      = [
#                [1L, 'LCG.CERN.ch', '', 'Active', 'Init', dSol, dSol, dSol, dSol, 'RS_SVC', dSol], 
#                [2L, 'LCG.CNAF.it', '', 'Banned', 'Init', dSol, dSol, dSol, dSol, 'RS_SVC', dSol]
#               ]
#        
#    res = self.rsDB.getSitesStatus( *tuple( initArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )       
#    
#    '''
#      Param tests
#    '''    
#    #Test first param ( siteName )
#    modArgs = initArgs[:]
#    modArgs[ 0 ] = 'LCG.CERN.ch'
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [ sol[ 0 ] ] )
#    modArgs[ 0 ] = [ 'LCG.CERN.ch' ]    
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [ sol[ 0 ] ] )        
#    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'LCG.CNAF.it' ]
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )
#    modArgs[ 0 ] = 'LCG.eGGs.xy'
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] )
#    modArgs[ 0 ] = [ 'LCG.eGGs.xy' ]
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] )
#    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'LCG.eGGs.xy' ]
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [ sol[0] ] )
#    modArgs[ 0 ] = [ None ]
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] )
#    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'LCG.eGGs.xy', None ]
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [ sol[0] ] )
#
#    #Test second param ( statusType ) 
#    modArgs = initArgs[:]
#    modArgs[ 1 ] = ''
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )
#    modArgs[ 1 ] = [ '' ]    
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )        
#    modArgs[ 1 ] = 'eGGs'
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] )
#    modArgs[ 1 ] = [ 'eGGs' ]
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] )
#    modArgs[ 1 ] = [ '', 'eGGs' ]
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )
#    modArgs[ 1 ] = [ None ]
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] )
#    modArgs[ 1 ] = [ '', 'eGGs', None ]
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )
#
#    #Test third parameter ( status )
#    modArgs = initArgs[:]
#    modArgs[ 2 ] = 'Active'
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [ sol[0] ] )
#    modArgs[ 2 ] = [ 'Active' ]    
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [ sol[0] ] )        
#    modArgs[ 2 ] = 'Banned'
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [ sol[ 1 ] ] ) 
#    modArgs[ 2 ] = [ 'Banned' ]
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [ sol[ 1 ] ] )
#    modArgs[ 2 ] = [ 'Active', 'Banned' ]
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )
#    modArgs[ 2 ] = 'eGGs'
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [])
#    modArgs[ 2 ] = [ 'eGGs' ]
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] )      
#    modArgs[ 2 ] = [ 'eGGs', 'Banned' ]
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [ sol[ 1 ] ] )        
#    modArgs[ 2 ] = [ None ]
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] )      
#    modArgs[ 2 ] = [ 'eGGs', 'Banned', None ]
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [ sol[ 1 ] ] )
#
#    #Test forth parameter ( reason )
#    modArgs = initArgs[:]
#    modArgs[ 3 ] = 'Init'
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )
#    modArgs[ 3 ] = [ 'Init' ]    
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )        
#    modArgs[ 3 ] = 'eGGs'
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] ) 
#    modArgs[ 3 ] = [ 'eGGs' ]
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] )
#    modArgs[ 3 ] = [ 'Init', 'eGGs' ]
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )           
#    modArgs[ 3 ] = [ None ]
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] )
#    modArgs[ 3 ] = [ 'Init', 'eGGs', None ]
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )
#
#    #Test fifth parameter ( dateCreated )
#    modArgs = initArgs[:]
#    modArgs[ 4 ] = dSol
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )   
#    modArgs[ 4 ] = [ dSol ]    
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )        
#    modArgs[ 4 ] = dNow
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] ) 
#    modArgs[ 4 ] = [ dNow ]
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] )
#    modArgs[ 4 ] = [ dSol, dNow ]
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )     
#    modArgs[ 4 ] = [ None ]
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'OK' ], False )
#    modArgs[ 4 ] = [ dSol, dNow, None ]
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'OK' ], False )
#            
#    #Test sixth parameter ( dateEffective ) 
#    modArgs = initArgs[:]
#    modArgs[ 5 ] = dSol
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )   
#    modArgs[ 5 ] = [ dSol ]    
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )        
#    modArgs[ 5 ] = dNow
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] ) 
#    modArgs[ 5 ] = [ dNow ]
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] )
#    modArgs[ 5 ] = [ dSol, dNow ]
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )     
#    modArgs[ 5 ] = [ None ]
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'OK' ], False )
#    modArgs[ 5 ] = [ dSol, dNow, None ]
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'OK' ], False )
#
#    #Test seventh parameter ( dateEnd )
#    modArgs = initArgs[:]
#    modArgs[ 6 ] = dSol
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )   
#    modArgs[ 6 ] = [ dSol ]    
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )        
#    modArgs[ 6 ] = dNow
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] ) 
#    modArgs[ 6 ] = [ dNow ]
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] )
#    modArgs[ 6 ] = [ dSol, dNow ]
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )     
#    modArgs[ 6 ] = [ None ]
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'OK' ], False )
#    modArgs[ 6 ] = [ dSol, dNow, None ]
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'OK' ], False )
#                
#    #Test eighth parameter ( lastCheckTime )
#    modArgs = initArgs[:]
#    modArgs[ 7 ] = dSol
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )   
#    modArgs[ 7 ] = [ dSol ]    
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )        
#    modArgs[ 7 ] = dNow
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] ) 
#    modArgs[ 7 ] = [ dNow ]
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] )
#    modArgs[ 7 ] = [ dSol, dNow ]
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )     
#    modArgs[ 7 ] = [ None ]
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'OK' ], False )
#    modArgs[ 7 ] = [ dSol, dNow, None ]
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'OK' ], False )
#        
#    #Test ninth parameter ( tokenOwner )
#    modArgs = initArgs[:]
#    modArgs[ 8 ] = 'RS_SVC'
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )
#    modArgs[ 8 ] = [ 'RS_SVC' ]   
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )   
#    modArgs[ 8 ] = 'eGGs'   
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] )
#    modArgs[ 8 ] = [ 'eGGs' ]   
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] )
#    modArgs[ 8 ] = [ 'RS_SVC','eGGs' ]   
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )
#    modArgs[ 8 ] = [ None ]   
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] )
#    modArgs[ 8 ] = [ 'RS_SVC','eGGs', None ]   
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )
#    
#    #Test tenth parameter ( tokenExpiration )
#    modArgs = initArgs[:]
#    modArgs[ 9 ] = dSol
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )   
#    modArgs[ 9 ] = [ dSol ]    
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )        
#    modArgs[ 9 ] = dNow
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] ) 
#    modArgs[ 9 ] = [ dNow ]
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] )
#    modArgs[ 9 ] = [ dSol, dNow ]
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )     
#    modArgs[ 9 ] = [ None ]
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'OK' ], False )
#    modArgs[ 9 ] = [ dSol, dNow, None ]
#    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'OK' ], False )
#         
#    '''
#      Mixed tests
#    '''
#    
#    '''
#      Kwargs tests...
#    '''         
#             
#  def test_07_SitesInput_getSitesHistory( self ):
#    '''
#    getSitesHistory( self, siteName, statusType, status, reason, dateCreated, 
#                       dateEffective, dateEnd, lastCheckTime, tokenOwner, 
#                       tokenExpiration, **kwargs )
#    '''
#    dNow = datetime.now()
#    
#    res = self.rsDB.getSitesHistory()
#    self.assertNotEqual( res.has_key( 'Value' ), True )   
#    res = self.rsDB.getSitesHistory( 'LCG.CERN.ch' )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.getSitesHistory( 'LCG.CERN.ch', '' )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.getSitesHistory( 'LCG.CERN.ch', '', 'eGGs reason' )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.getSitesHistory( 'LCG.CERN.ch', '', 'eGGs reason', dNow )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.getSitesHistory( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.getSitesHistory( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow, dNow )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.getSitesHistory( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow, dNow, dNow )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.getSitesHistory( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow, dNow, dNow, 'token' )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.getSitesHistory( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow, dNow, dNow, 'token', dNow, {}, dNow )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    
#    initArgs = [ None,None,None,None,None,None,None,None,None,None ]
#    dSol     = datetime(9999, 12, 11, 10, 9, 8)
#    sol      = [
#                [10L, 'LCG.CERN.ch', '', 'Active', 'Init', dSol, dSol, dSol, dSol, 'RS_SVC', dSol], 
#                [11L, 'LCG.CNAF.it', '', 'Banned', 'Init', dSol, dSol, dSol, dSol, 'RS_SVC', dSol]
#               ]
#        
#    self.rsDB.deleteSitesHistory( 'LCG.CERN.ch', None, None, None, None, None, None, None, None, None )    
#    self.rsDB.deleteSitesHistory( 'LCG.CNAF.it', None, None, None, None, None, None, None, None, None )
#    
#    # This is risky.. if we do not hit the same second, tachan !!
#    now = datetime.utcnow().replace( microsecond = 0 )
#    
#    import copy
#    
#    
#    sol2 = copy.deepcopy( sol )
#    sol2[ 0 ][ 6 ] = now   
#    sol2[ 1 ][ 6 ] = now
#    self.rsDB.setSiteStatus( *tuple( sol2[ 0 ][1:] ) )
#    self.rsDB.setSiteStatus( *tuple( sol2[ 1 ][1:] ) )
#
#    sol[ 0 ][ 7 ] = now   
#    sol[ 1 ][ 7 ] = now
#    
#    res = self.rsDB.getSitesHistory( *tuple( initArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )       
#    
#    '''
#      Param tests
#    '''    
#    #Test first param ( siteName )
#    modArgs = initArgs[:]
#    modArgs[ 0 ] = 'LCG.CERN.ch'
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [ sol[ 0 ] ] )
#    modArgs[ 0 ] = [ 'LCG.CERN.ch' ]    
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [ sol[ 0 ] ] )        
#    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'LCG.CNAF.it' ]
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )
#    modArgs[ 0 ] = 'LCG.eGGs.xy'
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] )
#    modArgs[ 0 ] = [ 'LCG.eGGs.xy' ]
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] )
#    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'LCG.eGGs.xy' ]
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [ sol[0] ] )
#    modArgs[ 0 ] = [ None ]
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] )
#    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'LCG.eGGs.xy', None ]
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [ sol[0] ] )
#
#    #Test second param ( statusType ) 
#    modArgs = initArgs[:]
#    modArgs[ 1 ] = ''
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )
#    modArgs[ 1 ] = [ '' ]    
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )        
#    modArgs[ 1 ] = 'eGGs'
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] )
#    modArgs[ 1 ] = [ 'eGGs' ]
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] )
#    modArgs[ 1 ] = [ '', 'eGGs' ]
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )
#    modArgs[ 1 ] = [ None ]
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] )
#    modArgs[ 1 ] = [ '', 'eGGs', None ]
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )
#
#    #Test third parameter ( status )
#    modArgs = initArgs[:]
#    modArgs[ 2 ] = 'Active'
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [ sol[0] ] )
#    modArgs[ 2 ] = [ 'Active' ]    
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [ sol[0] ] )        
#    modArgs[ 2 ] = 'Banned'
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [ sol[ 1 ] ] ) 
#    modArgs[ 2 ] = [ 'Banned' ]
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [ sol[ 1 ] ] )
#    modArgs[ 2 ] = [ 'Active', 'Banned' ]
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )
#    modArgs[ 2 ] = 'eGGs'
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [])
#    modArgs[ 2 ] = [ 'eGGs' ]
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] )      
#    modArgs[ 2 ] = [ 'eGGs', 'Banned' ]
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [ sol[ 1 ] ] )        
#    modArgs[ 2 ] = [ None ]
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] )      
#    modArgs[ 2 ] = [ 'eGGs', 'Banned', None ]
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [ sol[ 1 ] ] )
#
#    #Test forth parameter ( reason )
#    modArgs = initArgs[:]
#    modArgs[ 3 ] = 'Init'
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )
#    modArgs[ 3 ] = [ 'Init' ]    
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )        
#    modArgs[ 3 ] = 'eGGs'
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] ) 
#    modArgs[ 3 ] = [ 'eGGs' ]
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] )
#    modArgs[ 3 ] = [ 'Init', 'eGGs' ]
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )           
#    modArgs[ 3 ] = [ None ]
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] )
#    modArgs[ 3 ] = [ 'Init', 'eGGs', None ]
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )
#
#    #Test fifth parameter ( dateCreated )
#    modArgs = initArgs[:]
#    modArgs[ 4 ] = dSol
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )   
#    modArgs[ 4 ] = [ dSol ]    
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )        
#    modArgs[ 4 ] = dNow
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] ) 
#    modArgs[ 4 ] = [ dNow ]
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] )
#    modArgs[ 4 ] = [ dSol, dNow ]
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )     
#    modArgs[ 4 ] = [ None ]
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'OK' ], False )
#    modArgs[ 4 ] = [ dSol, dNow, None ]
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'OK' ], False )
#            
#    #Test sixth parameter ( dateEffective ) 
#    modArgs = initArgs[:]
#    modArgs[ 5 ] = dSol
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )   
#    modArgs[ 5 ] = [ dSol ]    
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )        
#    modArgs[ 5 ] = dNow
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] ) 
#    modArgs[ 5 ] = [ dNow ]
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] )
#    modArgs[ 5 ] = [ dSol, dNow ]
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )     
#    modArgs[ 5 ] = [ None ]
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'OK' ], False )
#    modArgs[ 5 ] = [ dSol, dNow, None ]
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'OK' ], False )
#
#    #Test seventh parameter ( dateEnd )
#    modArgs = initArgs[:]
#    modArgs[ 6 ] = now
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )   
#    modArgs[ 6 ] = [ now ]    
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )        
#    modArgs[ 6 ] = dNow
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] ) 
#    modArgs[ 6 ] = [ dNow ]
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] )
#    modArgs[ 6 ] = [ now, dNow ]
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )     
#    modArgs[ 6 ] = [ None ]
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'OK' ], False )
#    modArgs[ 6 ] = [ now, dNow, None ]
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'OK' ], False )
#                
#    #Test eighth parameter ( lastCheckTime )
#    modArgs = initArgs[:]
#    modArgs[ 7 ] = dSol
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )   
#    modArgs[ 7 ] = [ dSol ]    
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )        
#    modArgs[ 7 ] = dNow
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] ) 
#    modArgs[ 7 ] = [ dNow ]
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] )
#    modArgs[ 7 ] = [ dSol, dNow ]
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )     
#    modArgs[ 7 ] = [ None ]
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'OK' ], False )
#    modArgs[ 7 ] = [ dSol, dNow, None ]
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'OK' ], False )
#        
#    #Test ninth parameter ( tokenOwner )
#    modArgs = initArgs[:]
#    modArgs[ 8 ] = 'RS_SVC'
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )
#    modArgs[ 8 ] = [ 'RS_SVC' ]   
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )   
#    modArgs[ 8 ] = 'eGGs'   
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] )
#    modArgs[ 8 ] = [ 'eGGs' ]   
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] )
#    modArgs[ 8 ] = [ 'RS_SVC','eGGs' ]   
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )
#    modArgs[ 8 ] = [ None ]   
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] )
#    modArgs[ 8 ] = [ 'RS_SVC','eGGs', None ]   
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )
#    
#    #Test tenth parameter ( tokenExpiration )
#    modArgs = initArgs[:]
#    modArgs[ 9 ] = dSol
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )   
#    modArgs[ 9 ] = [ dSol ]    
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )        
#    modArgs[ 9 ] = dNow
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] ) 
#    modArgs[ 9 ] = [ dNow ]
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] )
#    modArgs[ 9 ] = [ dSol, dNow ]
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )     
#    modArgs[ 9 ] = [ None ]
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'OK' ], False )
#    modArgs[ 9 ] = [ dSol, dNow, None ]
#    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res[ 'OK' ], False )
#         
#    '''
#      Mixed tests
#    '''
#    
#    '''
#      Kwargs tests...
#    '''         
#
#  def test_08_SitesInput_getSitesScheduledStatus( self ):
#    '''
#    getSitesScheduledStatus( self, siteName, statusType, status, reason, dateCreated, 
#                             dateEffective, dateEnd, lastCheckTime, tokenOwner, 
#                             tokenExpiration, **kwargs )
#    '''
#    dNow = datetime.now()
#    
#    res = self.rsDB.getSitesScheduledStatus()
#    self.assertNotEqual( res.has_key( 'Value' ), True )   
#    res = self.rsDB.getSitesScheduledStatus( 'LCG.CERN.ch' )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.getSitesScheduledStatus( 'LCG.CERN.ch', '' )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.getSitesScheduledStatus( 'LCG.CERN.ch', '', 'eGGs reason' )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.getSitesScheduledStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.getSitesScheduledStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.getSitesScheduledStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow, dNow )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.getSitesScheduledStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow, dNow, dNow )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.getSitesScheduledStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow, dNow, dNow, 'token' )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.getSitesScheduledStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow, dNow, dNow, 'token', dNow, {}, dNow )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    
#    initArgs = [ None,None,None,None,None,None,None,None,None,None ]
#    dSol     = datetime(9999, 12, 11, 10, 9, 8)
#    sol      = [
#                [1L, 'LCG.CERN.ch', '', 'Active', 'Init', dSol, dSol, dSol, dSol, 'RS_SVC', dSol], 
#                [2L, 'LCG.CNAF.it', '', 'Banned', 'Init', dSol, dSol, dSol, dSol, 'RS_SVC', dSol]
#               ]
#        
#    self.rsDB.setSiteScheduledStatus( *tuple( sol[ 0 ][1:] ) )
#    self.rsDB.setSiteScheduledStatus( *tuple( sol[ 1 ][1:] ) )
#    
#    # This is risky.. if we do not hit the same second, tachan !!
#    #now = datetime.utcnow().replace( microsecond = 0 )
#    #sol[ 0 ][ 7 ] = now   
#    #sol[ 1 ][ 7 ] = now  
#        
#    res = self.rsDB.getSitesScheduledStatus( *tuple( initArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )       
#    
#    '''
#      Param tests
#    '''    
#    #Test first param ( siteName )
#    modArgs = initArgs[:]
#    modArgs[ 0 ] = 'LCG.CERN.ch'
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [ sol[ 0 ] ] )
#    modArgs[ 0 ] = [ 'LCG.CERN.ch' ]    
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [ sol[ 0 ] ] )        
#    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'LCG.CNAF.it' ]
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )
#    modArgs[ 0 ] = 'LCG.eGGs.xy'
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] )
#    modArgs[ 0 ] = [ 'LCG.eGGs.xy' ]
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] )
#    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'LCG.eGGs.xy' ]
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [ sol[0] ] )
#    modArgs[ 0 ] = [ None ]
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] )
#    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'LCG.eGGs.xy', None ]
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [ sol[0] ] )
#
#    #Test second param ( statusType ) 
#    modArgs = initArgs[:]
#    modArgs[ 1 ] = ''
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )
#    modArgs[ 1 ] = [ '' ]    
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )        
#    modArgs[ 1 ] = 'eGGs'
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] )
#    modArgs[ 1 ] = [ 'eGGs' ]
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] )
#    modArgs[ 1 ] = [ '', 'eGGs' ]
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )
#    modArgs[ 1 ] = [ None ]
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] )
#    modArgs[ 1 ] = [ '', 'eGGs', None ]
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )
#
#    #Test third parameter ( status )
#    modArgs = initArgs[:]
#    modArgs[ 2 ] = 'Active'
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [ sol[0] ] )
#    modArgs[ 2 ] = [ 'Active' ]    
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [ sol[0] ] )        
#    modArgs[ 2 ] = 'Banned'
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [ sol[ 1 ] ] ) 
#    modArgs[ 2 ] = [ 'Banned' ]
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [ sol[ 1 ] ] )
#    modArgs[ 2 ] = [ 'Active', 'Banned' ]
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )
#    modArgs[ 2 ] = 'eGGs'
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [])
#    modArgs[ 2 ] = [ 'eGGs' ]
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] )      
#    modArgs[ 2 ] = [ 'eGGs', 'Banned' ]
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [ sol[ 1 ] ] )        
#    modArgs[ 2 ] = [ None ]
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] )      
#    modArgs[ 2 ] = [ 'eGGs', 'Banned', None ]
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [ sol[ 1 ] ] )
#
#    #Test forth parameter ( reason )
#    modArgs = initArgs[:]
#    modArgs[ 3 ] = 'Init'
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )
#    modArgs[ 3 ] = [ 'Init' ]    
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )        
#    modArgs[ 3 ] = 'eGGs'
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] ) 
#    modArgs[ 3 ] = [ 'eGGs' ]
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] )
#    modArgs[ 3 ] = [ 'Init', 'eGGs' ]
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )           
#    modArgs[ 3 ] = [ None ]
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] )
#    modArgs[ 3 ] = [ 'Init', 'eGGs', None ]
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )
#
#    #Test fifth parameter ( dateCreated )
#    modArgs = initArgs[:]
#    modArgs[ 4 ] = dSol
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )   
#    modArgs[ 4 ] = [ dSol ]    
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )        
#    modArgs[ 4 ] = dNow
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] ) 
#    modArgs[ 4 ] = [ dNow ]
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] )
#    modArgs[ 4 ] = [ dSol, dNow ]
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )     
#    modArgs[ 4 ] = [ None ]
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'OK' ], False )
#    modArgs[ 4 ] = [ dSol, dNow, None ]
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'OK' ], False )
#            
#    #Test sixth parameter ( dateEffective ) 
#    modArgs = initArgs[:]
#    modArgs[ 5 ] = dSol
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )   
#    modArgs[ 5 ] = [ dSol ]    
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )        
#    modArgs[ 5 ] = dNow
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] ) 
#    modArgs[ 5 ] = [ dNow ]
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] )
#    modArgs[ 5 ] = [ dSol, dNow ]
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )     
#    modArgs[ 5 ] = [ None ]
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'OK' ], False )
#    modArgs[ 5 ] = [ dSol, dNow, None ]
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'OK' ], False )
#
#    #Test seventh parameter ( dateEnd )
#    modArgs = initArgs[:]
#    modArgs[ 6 ] = dSol
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )   
#    modArgs[ 6 ] = [ dSol ]    
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )        
#    modArgs[ 6 ] = dNow
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] ) 
#    modArgs[ 6 ] = [ dNow ]
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] )
#    modArgs[ 6 ] = [ dSol, dNow ]
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )     
#    modArgs[ 6 ] = [ None ]
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'OK' ], False )
#    modArgs[ 6 ] = [ dSol, dNow, None ]
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'OK' ], False )
#                
#    #Test eighth parameter ( lastCheckTime )
#    modArgs = initArgs[:]
#    modArgs[ 7 ] = dSol
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )   
#    modArgs[ 7 ] = [ dSol ]    
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )        
#    modArgs[ 7 ] = dNow
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] ) 
#    modArgs[ 7 ] = [ dNow ]
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] )
#    modArgs[ 7 ] = [ dSol, dNow ]
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )     
#    modArgs[ 7 ] = [ None ]
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'OK' ], False )
#    modArgs[ 7 ] = [ dSol, dNow, None ]
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'OK' ], False )
#        
#    #Test ninth parameter ( tokenOwner )
#    modArgs = initArgs[:]
#    modArgs[ 8 ] = 'RS_SVC'
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )
#    modArgs[ 8 ] = [ 'RS_SVC' ]   
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )   
#    modArgs[ 8 ] = 'eGGs'   
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] )
#    modArgs[ 8 ] = [ 'eGGs' ]   
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] )
#    modArgs[ 8 ] = [ 'RS_SVC','eGGs' ]   
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )
#    modArgs[ 8 ] = [ None ]   
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] )
#    modArgs[ 8 ] = [ 'RS_SVC','eGGs', None ]   
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )
#    
#    #Test tenth parameter ( tokenExpiration )
#    modArgs = initArgs[:]
#    modArgs[ 9 ] = dSol
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )   
#    modArgs[ 9 ] = [ dSol ]    
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )        
#    modArgs[ 9 ] = dNow
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] ) 
#    modArgs[ 9 ] = [ dNow ]
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], [] )
#    modArgs[ 9 ] = [ dSol, dNow ]
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'Value' ], sol )     
#    modArgs[ 9 ] = [ None ]
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'OK' ], False )
#    modArgs[ 9 ] = [ dSol, dNow, None ]
#    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res[ 'OK' ], False )
#         
#    '''
#      Mixed tests
#    '''
#    
#    '''
#      Kwargs tests...
#    '''         
#
#  def test_09_SitesInput_getSitesPresent( self ):
#    '''
#    getSitesPresent( self, siteName, siteType, gridSiteName, gridTier, 
#                     statusType, status, dateEffective, reason, lastCheckTime, 
#                     tokenOwner, tokenExpiration, formerStatus, **kwargs )
#    '''
#    dNow = datetime.now()
#    
#    res = self.rsDB.getSitesPresent()
#    self.assertNotEqual( res.has_key( 'Value' ), True )   
#    res = self.rsDB.getSitesPresent( 'LCG.CERN.ch' )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.getSitesPresent( 'LCG.CERN.ch', 'T0' )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.getSitesPresent( 'LCG.CERN.ch', 'T0', 'CERN-PROD' )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.getSitesPresent( 'LCG.CERN.ch', 'T0', 'CERN-PROD', 'T0' )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.getSitesPresent( 'LCG.CERN.ch', 'T0', 'CERN-PROD', 'T0', '' )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.getSitesPresent( 'LCG.CERN.ch', 'T0', 'CERN-PROD', 'T0', '', 'Active' )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.getSitesPresent( 'LCG.CERN.ch', 'T0', 'CERN-PROD', 'T0', '', 
#                                             'Active', dNow )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.getSitesPresent( 'LCG.CERN.ch', 'T0', 'CERN-PROD', 'T0', '', 
#                                             'Active', dNow, 'eGGs reason' )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.getSitesPresent( 'LCG.CERN.ch', 'T0', 'CERN-PROD', 'T0', '', 
#                                             'Active', dNow, 'eGGs reason', dNow )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.getSitesPresent( 'LCG.CERN.ch', 'T0', 'CERN-PROD', 'T0', '', 
#                                             'Active', dNow, 'eGGs reason', dNow, 'token' )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.getSitesPresent( 'LCG.CERN.ch', 'T0', 'CERN-PROD', 'T0', '', 
#                                             'Active', dNow, 'eGGs reason', dNow, 'token', dNow )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.getSitesPresent( 'LCG.CERN.ch', 'T0', 'CERN-PROD', 'T0', '', 
#                                             'Active', dNow, 'eGGs reason', dNow, 'token', dNow, 'Banned', {} )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.getSitesPresent( 'LCG.CERN.ch', 'T0', 'CERN-PROD', 'T0', '', 
#                                             'Active', dNow, 'eGGs reason', dNow, 'token', dNow, 'Banned', {}, dNow )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#
#    
#    initArgs = [ None,None,None,None,None,None,None,None,None,None, None, None ]
#    dSol     = datetime(9999, 12, 11, 10, 9, 8)
#    sol      = [
#                [1L, 'LCG.CERN.ch', '', 'Active', 'Init', dSol, dSol, dSol, dSol, 'RS_SVC', dSol], 
#                [2L, 'LCG.CNAF.it', '', 'Banned', 'Init', dSol, dSol, dSol, dSol, 'RS_SVC', dSol]
#               ]
#
#    '''
#    [
#     ['LCG.CERN.ch', 'T0', 'CERN-PROD', 'T0', '', 'Active', datetime.datetime(2011, 10, 5, 15, 14, 49), 'Init', 
#         datetime.datetime(9999, 12, 11, 10, 9, 8), 'RS_SVC', datetime.datetime(9999, 12, 11, 10, 9, 8), 'Active'], 
#     ['LCG.CNAF.it', 'T1', 'INFN-T1', 'T1', '', 'Banned', datetime.datetime(2011, 10, 5, 15, 14, 49), 'Init', 
#        datetime.datetime(9999, 12, 11, 10, 9, 8), 'RS_SVC', datetime.datetime(9999, 12, 11, 10, 9, 8), 'Banned']]
#    '''
#
#    '''
#      WTF !! This is bizarre !!
#    '''
#
#    #print self.rsDB.getSites( *tuple( initArgs[9:]) )
#    #print self.rsDB.getGridSites( None, None )
#    #print self.rsDB.getSitesStatus( *tuple( initArgs[2:]))
#    #print self.rsDB.getSitesHistory( *tuple( initArgs[2:]))
#
#    #res = self.rsDB.getSitesPresent( None,None,None,None,None,None,None,None,None,None, None, None )
#    #print res[ 'Value' ]
#    #res = self.rsDB.getSitesPresent( 'LCG.CERN.ch',None,None,None,None,None,None,None,None,None, None, None )
#    #print res[ 'Value' ]
#
#  def test_10_SitesInput_deleteSitesScheduledStatus( self ):
#    '''
#    deleteSitesScheduledStatus( self, siteName, statusType, status, reason, 
#                                dateCreated, dateEffective, dateEnd, 
#                                lastCheckTime, tokenOwner, tokenExpiration
#    '''
#    dNow = datetime.now()
#    
#    res = self.rsDB.deleteSitesScheduledStatus()
#    self.assertNotEqual( res.has_key( 'Value' ), True )   
#    res = self.rsDB.deleteSitesScheduledStatus( 'LCG.CERN.ch' )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.deleteSitesScheduledStatus( 'LCG.CERN.ch', '' )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.deleteSitesScheduledStatus( 'LCG.CERN.ch', '', 'eGGs reason' )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.deleteSitesScheduledStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.deleteSitesScheduledStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.deleteSitesScheduledStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow, dNow )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.deleteSitesScheduledStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow, dNow, dNow )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.deleteSitesScheduledStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow, dNow, dNow, 'token' )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.deleteSitesScheduledStatus( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow, dNow, dNow, 'token', dNow, {}, dNow )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    
#    initArgs = [ None,None,None,None,None,None,None,None,None,None ]
#    dSol     = datetime(9999, 12, 11, 10, 9, 8)
#    sol      = [
#                [1L, 'LCG.CERN.ch', '', 'Active', 'Init', dSol, dSol, dSol, dSol, 'RS_SVC', dSol], 
#                [2L, 'LCG.CNAF.it', '', 'Banned', 'Init', dSol, dSol, dSol, dSol, 'RS_SVC', dSol]
#               ]
#        
#       
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( initArgs ) )
#    self.assertNotEquals( res[ 'OK' ], True )       
#    
#    '''
#      Param tests
#    '''    
#    #Test first param ( siteName )
#    modArgs = initArgs[:]
#    modArgs[ 0 ] = 'LCG.CERN.ch'
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    modArgs[ 0 ] = [ 'LCG.CERN.ch' ]    
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() )        
#    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'LCG.CNAF.it' ]
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    modArgs[ 0 ] = 'LCG.eGGs.xy'
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    modArgs[ 0 ] = [ 'LCG.eGGs.xy' ]
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'LCG.eGGs.xy' ]
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    modArgs[ 0 ] = [ None ]
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'LCG.eGGs.xy', None ]
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#
#    #Test second param ( statusType ) 
#    modArgs = initArgs[:]
#    modArgs[ 1 ] = ''
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    modArgs[ 1 ] = [ '' ]    
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() )         
#    modArgs[ 1 ] = 'eGGs'
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    modArgs[ 1 ] = [ 'eGGs' ]
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    modArgs[ 1 ] = [ '', 'eGGs' ]
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    modArgs[ 1 ] = [ None ]
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    modArgs[ 1 ] = [ '', 'eGGs', None ]
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#
#    #Test third parameter ( status )
#    modArgs = initArgs[:]
#    modArgs[ 2 ] = 'Active'
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    modArgs[ 2 ] = [ 'Active' ]    
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() )         
#    modArgs[ 2 ] = 'Banned'
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    modArgs[ 2 ] = [ 'Banned' ]
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    modArgs[ 2 ] = [ 'Active', 'Banned' ]
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    modArgs[ 2 ] = 'eGGs'
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    modArgs[ 2 ] = [ 'eGGs' ]
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() )      
#    modArgs[ 2 ] = [ 'eGGs', 'Banned' ]
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() )         
#    modArgs[ 2 ] = [ None ]
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() )      
#    modArgs[ 2 ] = [ 'eGGs', 'Banned', None ]
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#
#    #Test forth parameter ( reason )
#    modArgs = initArgs[:]
#    modArgs[ 3 ] = 'Init'
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    modArgs[ 3 ] = [ 'Init' ]    
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() )        
#    modArgs[ 3 ] = 'eGGs'
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    modArgs[ 3 ] = [ 'eGGs' ]
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    modArgs[ 3 ] = [ 'Init', 'eGGs' ]
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() )           
#    modArgs[ 3 ] = [ None ]
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    modArgs[ 3 ] = [ 'Init', 'eGGs', None ]
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#
#    #Test fifth parameter ( dateCreated )
#    modArgs = initArgs[:]
#    modArgs[ 4 ] = dSol
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() )   
#    modArgs[ 4 ] = [ dSol ]    
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() )         
#    modArgs[ 4 ] = dNow
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() )  
#    modArgs[ 4 ] = [ dNow ]
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    modArgs[ 4 ] = [ dSol, dNow ]
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() )      
#    modArgs[ 4 ] = [ None ]
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEquals( res[ 'OK' ], True ) 
#    modArgs[ 4 ] = [ dSol, dNow, None ]
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEquals( res[ 'OK' ], True ) 
#            
#    #Test sixth parameter ( dateEffective ) 
#    modArgs = initArgs[:]
#    modArgs[ 5 ] = dSol
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() )   
#    modArgs[ 5 ] = [ dSol ]    
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() )         
#    modArgs[ 5 ] = dNow
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() )  
#    modArgs[ 5 ] = [ dNow ]
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    modArgs[ 5 ] = [ dSol, dNow ]
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() )    
#    modArgs[ 5 ] = [ None ]
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEquals( res[ 'OK' ], True ) 
#    modArgs[ 5 ] = [ dSol, dNow, None ]
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEquals( res[ 'OK' ], True ) 
#
#    #Test seventh parameter ( dateEnd )
#    modArgs = initArgs[:]
#    modArgs[ 6 ] = dSol
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() )   
#    modArgs[ 6 ] = [ dSol ]    
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() )        
#    modArgs[ 6 ] = dNow
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    modArgs[ 6 ] = [ dNow ]
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    modArgs[ 6 ] = [ dSol, dNow ]
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() )      
#    modArgs[ 6 ] = [ None ]
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEquals( res[ 'OK' ], True ) 
#    modArgs[ 6 ] = [ dSol, dNow, None ]
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEquals( res[ 'OK' ], True ) 
#                
#    #Test eighth parameter ( lastCheckTime )
#    modArgs = initArgs[:]
#    modArgs[ 7 ] = dSol
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() )   
#    modArgs[ 7 ] = [ dSol ]    
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() )         
#    modArgs[ 7 ] = dNow
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() )  
#    modArgs[ 7 ] = [ dNow ]
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    modArgs[ 7 ] = [ dSol, dNow ]
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() )      
#    modArgs[ 7 ] = [ None ]
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEquals( res[ 'OK' ], True ) 
#    modArgs[ 7 ] = [ dSol, dNow, None ]
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEquals( res[ 'OK' ], True ) 
#        
#    #Test ninth parameter ( tokenOwner )
#    modArgs = initArgs[:]
#    modArgs[ 8 ] = 'RS_SVC'
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    modArgs[ 8 ] = [ 'RS_SVC' ]   
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() )    
#    modArgs[ 8 ] = 'eGGs'   
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    modArgs[ 8 ] = [ 'eGGs' ]   
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    modArgs[ 8 ] = [ 'RS_SVC','eGGs' ]   
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    modArgs[ 8 ] = [ None ]   
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    modArgs[ 8 ] = [ 'RS_SVC','eGGs', None ]   
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    
#    #Test tenth parameter ( tokenExpiration )
#    modArgs = initArgs[:]
#    modArgs[ 9 ] = dSol
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() )    
#    modArgs[ 9 ] = [ dSol ]    
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() )         
#    modArgs[ 9 ] = dNow
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    modArgs[ 9 ] = [ dNow ]
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    modArgs[ 9 ] = [ dSol, dNow ]
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() )     
#    modArgs[ 9 ] = [ None ]
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEquals( res[ 'OK' ], True ) 
#    modArgs[ 9 ] = [ dSol, dNow, None ]
#    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
#    self.assertNotEquals( res[ 'OK' ], True ) 
#
#  def test_11_SitesInput_deleteSitesHistory( self ):
#    '''
#    deleteSitesHistory( self, siteName, statusType, status, reason, 
#                        dateCreated, dateEffective, dateEnd, 
#                        lastCheckTime, tokenOwner, tokenExpiration, kwargs )
#    '''
#    dNow = datetime.now()
#    
#    res = self.rsDB.deleteSitesHistory()
#    self.assertNotEqual( res.has_key( 'Value' ), True )   
#    res = self.rsDB.deleteSitesHistory( 'LCG.CERN.ch' )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.deleteSitesHistory( 'LCG.CERN.ch', '' )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.deleteSitesHistory( 'LCG.CERN.ch', '', 'eGGs reason' )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.deleteSitesHistory( 'LCG.CERN.ch', '', 'eGGs reason', dNow )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.deleteSitesHistory( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.deleteSitesHistory( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow, dNow )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.deleteSitesHistory( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow, dNow, dNow )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.deleteSitesHistory( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow, dNow, dNow, 'token' )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    res = self.rsDB.deleteSitesHistory( 'LCG.CERN.ch', '', 'eGGs reason', dNow, dNow, dNow, dNow, 'token', dNow, {}, dNow )
#    self.assertNotEqual( res.has_key( 'Value' ), True )
#    
#    initArgs = [ None,None,None,None,None,None,None,None,None,None ]
#    dSol     = datetime(9999, 12, 11, 10, 9, 8)
#    sol      = [
#                [1L, 'LCG.CERN.ch', '', 'Active', 'Init', dSol, dSol, dSol, dSol, 'RS_SVC', dSol], 
#                [2L, 'LCG.CNAF.it', '', 'Banned', 'Init', dSol, dSol, dSol, dSol, 'RS_SVC', dSol]
#               ]
#        
#       
#    res = self.rsDB.deleteSitesHistory( *tuple( initArgs ) )
#    self.assertNotEquals( res[ 'OK' ], True )       
#    
#    '''
#      Param tests
#    '''    
#    #Test first param ( siteName )
#    modArgs = initArgs[:]
#    modArgs[ 0 ] = 'LCG.CERN.ch'
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    modArgs[ 0 ] = [ 'LCG.CERN.ch' ]    
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() )        
#    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'LCG.CNAF.it' ]
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    modArgs[ 0 ] = 'LCG.eGGs.xy'
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    modArgs[ 0 ] = [ 'LCG.eGGs.xy' ]
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'LCG.eGGs.xy' ]
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    modArgs[ 0 ] = [ None ]
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'LCG.eGGs.xy', None ]
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#
#    #Test second param ( statusType ) 
#    modArgs = initArgs[:]
#    modArgs[ 1 ] = ''
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    modArgs[ 1 ] = [ '' ]    
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() )         
#    modArgs[ 1 ] = 'eGGs'
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    modArgs[ 1 ] = [ 'eGGs' ]
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    modArgs[ 1 ] = [ '', 'eGGs' ]
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    modArgs[ 1 ] = [ None ]
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    modArgs[ 1 ] = [ '', 'eGGs', None ]
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#
#    #Test third parameter ( status )
#    modArgs = initArgs[:]
#    modArgs[ 2 ] = 'Active'
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    modArgs[ 2 ] = [ 'Active' ]    
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() )         
#    modArgs[ 2 ] = 'Banned'
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    modArgs[ 2 ] = [ 'Banned' ]
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    modArgs[ 2 ] = [ 'Active', 'Banned' ]
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    modArgs[ 2 ] = 'eGGs'
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    modArgs[ 2 ] = [ 'eGGs' ]
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() )      
#    modArgs[ 2 ] = [ 'eGGs', 'Banned' ]
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() )         
#    modArgs[ 2 ] = [ None ]
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() )      
#    modArgs[ 2 ] = [ 'eGGs', 'Banned', None ]
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#
#    #Test forth parameter ( reason )
#    modArgs = initArgs[:]
#    modArgs[ 3 ] = 'Init'
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    modArgs[ 3 ] = [ 'Init' ]    
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() )        
#    modArgs[ 3 ] = 'eGGs'
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    modArgs[ 3 ] = [ 'eGGs' ]
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    modArgs[ 3 ] = [ 'Init', 'eGGs' ]
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() )           
#    modArgs[ 3 ] = [ None ]
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    modArgs[ 3 ] = [ 'Init', 'eGGs', None ]
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#
#    #Test fifth parameter ( dateCreated )
#    modArgs = initArgs[:]
#    modArgs[ 4 ] = dSol
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() )   
#    modArgs[ 4 ] = [ dSol ]    
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() )         
#    modArgs[ 4 ] = dNow
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() )  
#    modArgs[ 4 ] = [ dNow ]
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    modArgs[ 4 ] = [ dSol, dNow ]
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() )      
#    modArgs[ 4 ] = [ None ]
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertNotEquals( res[ 'OK' ], True ) 
#    modArgs[ 4 ] = [ dSol, dNow, None ]
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertNotEquals( res[ 'OK' ], True ) 
#            
#    #Test sixth parameter ( dateEffective ) 
#    modArgs = initArgs[:]
#    modArgs[ 5 ] = dSol
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() )   
#    modArgs[ 5 ] = [ dSol ]    
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() )         
#    modArgs[ 5 ] = dNow
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() )  
#    modArgs[ 5 ] = [ dNow ]
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    modArgs[ 5 ] = [ dSol, dNow ]
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() )    
#    modArgs[ 5 ] = [ None ]
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertNotEquals( res[ 'OK' ], True ) 
#    modArgs[ 5 ] = [ dSol, dNow, None ]
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertNotEquals( res[ 'OK' ], True ) 
#
#    #Test seventh parameter ( dateEnd )
#    modArgs = initArgs[:]
#    modArgs[ 6 ] = dSol
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() )   
#    modArgs[ 6 ] = [ dSol ]    
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() )        
#    modArgs[ 6 ] = dNow
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    modArgs[ 6 ] = [ dNow ]
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    modArgs[ 6 ] = [ dSol, dNow ]
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() )      
#    modArgs[ 6 ] = [ None ]
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertNotEquals( res[ 'OK' ], True ) 
#    modArgs[ 6 ] = [ dSol, dNow, None ]
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertNotEquals( res[ 'OK' ], True ) 
#                
#    #Test eighth parameter ( lastCheckTime )
#    modArgs = initArgs[:]
#    modArgs[ 7 ] = dSol
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() )   
#    modArgs[ 7 ] = [ dSol ]    
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() )         
#    modArgs[ 7 ] = dNow
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() )  
#    modArgs[ 7 ] = [ dNow ]
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    modArgs[ 7 ] = [ dSol, dNow ]
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() )      
#    modArgs[ 7 ] = [ None ]
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertNotEquals( res[ 'OK' ], True ) 
#    modArgs[ 7 ] = [ dSol, dNow, None ]
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertNotEquals( res[ 'OK' ], True ) 
#        
#    #Test ninth parameter ( tokenOwner )
#    modArgs = initArgs[:]
#    modArgs[ 8 ] = 'RS_SVC'
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    modArgs[ 8 ] = [ 'RS_SVC' ]   
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() )    
#    modArgs[ 8 ] = 'eGGs'   
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    modArgs[ 8 ] = [ 'eGGs' ]   
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    modArgs[ 8 ] = [ 'RS_SVC','eGGs' ]   
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    modArgs[ 8 ] = [ None ]   
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    modArgs[ 8 ] = [ 'RS_SVC','eGGs', None ]   
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    
#    #Test tenth parameter ( tokenExpiration )
#    modArgs = initArgs[:]
#    modArgs[ 9 ] = dSol
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() )    
#    modArgs[ 9 ] = [ dSol ]    
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() )         
#    modArgs[ 9 ] = dNow
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    modArgs[ 9 ] = [ dNow ]
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() ) 
#    modArgs[ 9 ] = [ dSol, dNow ]
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertEquals( res, S_OK() )     
#    modArgs[ 9 ] = [ None ]
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertNotEquals( res[ 'OK' ], True ) 
#    modArgs[ 9 ] = [ dSol, dNow, None ]
#    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
#    self.assertNotEquals( res[ 'OK' ], True )       
#
#  def test_12_SitesInput_deleteSites( self ):
    '''
    deleteSites( self, siteName )
    '''

    res = self.rsDB.deleteSites()
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.deleteSites( 'eGGs', None )
    self.assertNotEqual( res.has_key( 'Value' ), True )

    #Test first param
    res = self.rsDB.deleteSites( 'LCG.CERN.ch' )
    self.assertEqual( res, S_OK() )
    res = self.rsDB.deleteSites( [ 'LCG.CERN.ch' ] )
    self.assertEqual( res, S_OK() )
    res = self.rsDB.deleteSites( 'eGGs' )
    self.assertEqual( res, S_OK() )
    res = self.rsDB.deleteSites( [ 'eGGs' ] )
    self.assertEqual( res, S_OK() )
    res = self.rsDB.deleteSites( None )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.deleteSites( [ None ] )
    self.assertEqual( res, S_OK() )
    res = self.rsDB.deleteSites( [ 'LCG.CERN.ch', None ] )
    self.assertEqual( res, S_OK() )


################################################################################    

################################################################################

def cleanDB():

  ## CLEAN UP DB FOR NEXT TEST
  print '\n----------------------------------------------------------------------'
  print '  Cleaning db for next tests'
  
  import MySQLdb
  db = MySQLdb.connect( host = 'localhost', user = 'test', 
                        passwd = 'test', db = 'ResourceStatusTestDB' )
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
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(Test_GridSitesInput))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(Test_SitesInput))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(Test_ServicesInput))
  
  unittest.TextTestRunner(verbosity=2).run(suite)    

################################################################################    
    
if __name__ == '__main__':
  
  cleanDB()
  runTests() 
     
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF    