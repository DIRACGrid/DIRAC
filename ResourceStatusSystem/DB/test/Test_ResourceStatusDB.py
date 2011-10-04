import unittest

from datetime import datetime

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
    res = self.rsDB.addOrModifyGridSite( None, None )
    self.assertNotEqual( res.has_key( 'Value' ), True )       
    res = self.rsDB.addOrModifyGridSite( None, 'T2' )
    self.assertNotEqual( res.has_key( 'Value' ), True )
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
    
    res = self.rsDB.getGridSites( None, None )  
    self.assertEqual( res[ 'Value' ], [['AUVERGRID', 'T2'], ['INFN-T1', 'T1']] )
    res = self.rsDB.getGridSites( 'AUVERGRID', None )
    self.assertEqual( res[ 'Value' ], [['AUVERGRID', 'T2']] )
    res = self.rsDB.getGridSites( ['AUVERGRID','INFN-T1'], None )
    self.assertEqual( res[ 'Value' ], [['AUVERGRID', 'T2'],['INFN-T1', 'T1']] )
    res = self.rsDB.getGridSites( None, 'T2' )  
    self.assertEqual( res[ 'Value' ], [['AUVERGRID', 'T2']] )
    res = self.rsDB.getGridSites( None, ['T1', 'T2'] )
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
    res = self.rsDB.addOrModifySite( 'LCG.CERN.ch', 'T0', None )
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
    setSiteStatus( self, siteName, statusType, status, reason, tokenOwner, 
                   tokenExpiration, dateCreated, dateEffective, dateEnd, 
                   lastCheckTime ) 
    '''
    res = self.rsDB.setSiteStatus()
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.setSiteStatus( 'LCG.CERN.ch' )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.setSiteStatus( 'LCG.CERN.ch', '')
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.setSiteStatus( 'LCG.CERN.ch', '', 'Active')
    self.assertNotEqual( res.has_key( 'Value' ), True )
    
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
                                   None, None, None, 'eGGs', None )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.setSiteStatus( 'LCG.CERN.ch', '', 'Active', None, None,
                                   None, None, None, None, 'eGGs' )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    
    now = datetime.now()
    
    res = self.rsDB.setSiteStatus( 'LCG.CERN.ch', '', 'Active', None, None,
                                   now, now, now, now, now )
    self.assertEqual( res, S_OK() )
    res = self.rsDB.setSiteStatus( 'eGGs', '', 'Active', None, None,
                                   now, now, now, now, now )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.setSiteStatus( 'LCG.CERN.ch', 'eGGs', 'Active', None, None,
                                   now, now, now, now, now )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.setSiteStatus( 'LCG.CERN.ch', '', 'eGGs', None, None,
                                   now, now, now, now, now )
    self.assertNotEqual( res.has_key( 'Value' ), True )

  def test_Sites03_setSiteScheduledStatus( self ):
    '''
    setSiteScheduledStatus( self, siteName, statusType, status, reason, tokenOwner, 
                            tokenExpiration, dateCreated, dateEffective, 
                            dateEnd, lastCheckTime )
    '''   
    res = self.rsDB.setSiteScheduledStatus()
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.setSiteScheduledStatus( 'LCG.CERN.ch' )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.setSiteScheduledStatus( 'LCG.CERN.ch', '')
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.setSiteScheduledStatus( 'LCG.CERN.ch', '', 'Active')
    self.assertNotEqual( res.has_key( 'Value' ), True )
    
    res = self.rsDB.setSiteScheduledStatus( 'LCG.CERN.ch', '', 'Active', None, None,
                                            None,None,None,None,None )
    self.assertEqual( res, S_OK() )
    
    res = self.rsDB.setSiteScheduledStatus( ['LCG.CERN.ch'], '', 'Active', None, None,
                                            None, None, None, None, None )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.setSiteScheduledStatus( 'LCG.CERN.ch', [''], 'Active', None, None,
                                            None, None, None, None, None )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.setSiteScheduledStatus( 'LCG.CERN.ch', '', ['Active'], None, None,
                                            None, None, None, None, None )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.setSiteScheduledStatus( 'LCG.CERN.ch', '', 'Active', None, None,
                                            'eGGs', None, None, None, None )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.setSiteScheduledStatus( 'LCG.CERN.ch', '', 'Active', None, None,
                                            None, 'eGGs', None, None, None )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.setSiteScheduledStatus( 'LCG.CERN.ch', '', 'Active', None, None,
                                            None, None, 'eGGs', None, None )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.setSiteScheduledStatus( 'LCG.CERN.ch', '', 'Active', None, None,
                                            None, None, None, 'eGGs', None )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.setSiteScheduledStatus( 'LCG.CERN.ch', '', 'Active', None, None,
                                            None, None, None, None, 'eGGs' )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    
    now = datetime.now()
    
    res = self.rsDB.setSiteScheduledStatus( 'LCG.CERN.ch', '', 'Active', None, None,
                                            now, now, now, now, now )
    self.assertEqual( res, S_OK() )
    res = self.rsDB.setSiteScheduledStatus( 'eGGs', '', 'Active', None, None,
                                            now, now, now, now, now )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.setSiteScheduledStatus( 'LCG.CERN.ch', 'eGGs', 'Active', None, None,
                                            now, now, now, now, now )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.setSiteScheduledStatus( 'LCG.CERN.ch', '', 'eGGs', None, None,
                                            now, now, now, now, now )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    
  def test_Sites04_updateSiteStatus( self ):
    '''
    updateSiteStatus( self, siteName, statusType, status, reason, tokenOwner, 
                      tokenExpiration, dateCreated, dateEffective, dateEnd, 
                      lastCheckTime )
    '''
    res = self.rsDB.updateSiteStatus()
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.updateSiteStatus( 'LCG.CERN.ch' )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.updateSiteStatus( 'LCG.CERN.ch', '')
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.updateSiteStatus( 'LCG.CERN.ch', '', 'Active')
    self.assertNotEqual( res.has_key( 'Value' ), True )
    
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
                                            None, None, None, 'eGGs', None )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.updateSiteStatus( 'LCG.CERN.ch', '', 'Active', None, None,
                                            None, None, None, None, 'eGGs' )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    
    now = datetime.now()
    
    res = self.rsDB.updateSiteStatus( 'LCG.CERN.ch', '', 'Active', None, None,
                                            now, now, now, now, now )
    self.assertEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.updateSiteStatus( 'eGGs', '', 'Active', None, None,
                                            now, now, now, now, now )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.updateSiteStatus( 'LCG.CERN.ch', 'eGGs', 'Active', None, None,
                                            now, now, now, now, now )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.updateSiteStatus( 'LCG.CERN.ch', '', 'eGGs', None, None,
                                       now, now, now, now, now )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    
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
    
    res = self.rsDB.getSites( None, None, None )
    self.assertEqual( res[ 'Value' ], [['LCG.CERN.ch', 'T0', 'CERN-PROD'], ['LCG.CNAF.it', 'T1', 'INFN-T1']] )
    res = self.rsDB.getSites( None, 'T1', None )
    self.assertEqual( res[ 'Value' ], [['LCG.CNAF.it', 'T1', 'INFN-T1']] )
    res = self.rsDB.getSites( None, None, 'INFN-T1' )
    self.assertEqual( res[ 'Value' ], [['LCG.CNAF.it', 'T1', 'INFN-T1']] )
    res = self.rsDB.getSites( 'eGGs', None, None )
    self.assertEqual( res[ 'Value' ], [] )
    res = self.rsDB.getSites( None, 'eGGs', None )
    self.assertEqual( res[ 'Value' ], [] )    
    res = self.rsDB.getSites( None, None, 'eGGs' )
    self.assertEqual( res[ 'Value' ], [] )
    
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

#  def test_Sites06_getSitesStatus( self ):
#    pass
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
    
if __name__ == '__main__':

  cleanDB()
  
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(Test_ResourceStatusDB)
  
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(Test_GridSites))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(Test_Sites))
  
  unittest.TextTestRunner(verbosity=2).run(suite)    
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF    