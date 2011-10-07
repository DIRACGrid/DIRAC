from DIRAC import S_OK
from DIRAC.ResourceStatusSystem.DB.test.TestSuite_ResourceStatusDB.TestCase_EmptyDB import TestCase_EmptyDB

from datetime import datetime
import inspect

class Test_EmptyDB_Sites( TestCase_EmptyDB ):

  def test_01_addOrModifySite( self ):
    '''
    addOrModifySite( self, siteName, siteType, gridSiteName )
    '''
    
    ins = inspect.getargspec( self.rsDB.addOrModifySite.f )   
    self.assertEqual( ins.args, [ 'self', 'siteName', 'siteType', 'gridSiteName' ] )
    self.assertEqual( ins.varargs,  None)
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )
    
    initArgs = [ None, None, None ]
    
    #Fails because second parameter does not validate
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'LCG.CERN.ch'
    res = self.rsDB.addOrModifySite( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'LCG.CERN.ch' ]
    res = self.rsDB.addOrModifySite( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = 'eGGs'
    res = self.rsDB.addOrModifySite( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'eGGs' ]
    res = self.rsDB.addOrModifySite( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'eGGs' ]
    res = self.rsDB.addOrModifySite( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ None ]
    res = self.rsDB.addOrModifySite( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'eGGs', None ]
    res = self.rsDB.addOrModifySite( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )

    #Fail because first parameter does not validate
    modArgs = initArgs[:]
    modArgs[ 1 ] = 'T0'
    res = self.rsDB.addOrModifySite( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ 'T0' ]
    res = self.rsDB.addOrModifySite( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.addOrModifySite( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ 'eGGs' ]
    res = self.rsDB.addOrModifySite( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ 'T0', 'eGGs' ]
    res = self.rsDB.addOrModifySite( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ None ]
    res = self.rsDB.addOrModifySite( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ 'T0', 'eGGs', None ]
    res = self.rsDB.addOrModifySite( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )    

    #Fail because first parameter does not validate
    modArgs = initArgs[:]
    modArgs[ 2 ] = 'CERN-PROD'
    res = self.rsDB.addOrModifySite( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ 'CERN-PROD' ]
    res = self.rsDB.addOrModifySite( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = 'eGGs'
    res = self.rsDB.addOrModifySite( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ 'eGGs' ]
    res = self.rsDB.addOrModifySite( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ 'CERN-PROD', 'eGGs' ]
    res = self.rsDB.addOrModifySite( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ None ]
    res = self.rsDB.addOrModifySite( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ 'CERN-PROD', 'eGGs', None ]
    res = self.rsDB.addOrModifySite( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )     
           
    res = self.rsDB.addOrModifySite( None, None, None )
    self.assertEqual( res[ 'OK' ], False )   
    res = self.rsDB.addOrModifySite( 'LCG.CERN.ch', None, None )
    self.assertEqual( res[ 'OK' ], False )   
    res = self.rsDB.addOrModifySite( 'LCG.CERN.ch', None, 'CERN-PROD' )
    self.assertEqual( res[ 'OK' ], False )   
    res = self.rsDB.addOrModifySite( 'LCG.CERN.ch', 'eGGs', 'CERN-PROD' )
    self.assertEqual( res[ 'OK' ], False )   
    # This one looks correct, it is correct, but GridSites table is empty !!
    res = self.rsDB.addOrModifySite( 'LCG.CERN.ch', 'T0', 'CERN-PROD' )
    self.assertEqual( res[ 'OK' ], False )   
            
  def test_02_setSiteStatus( self ):
    '''
    setSiteStatus( self, siteName, statusType, status, reason, dateCreated, 
                   dateEffective, dateEnd, lastCheckTime, tokenOwner, 
                   tokenExpiration ) 
    '''

    ins = inspect.getargspec( self.rsDB.setSiteStatus.f )   
    self.assertEqual( ins.args, [ 'self', 'siteName', 'statusType', 'status', 'reason', 'dateCreated', 
                                  'dateEffective', 'dateEnd', 'lastCheckTime', 'tokenOwner', 
                                  'tokenExpiration' ] )
    self.assertEqual( ins.varargs,  None)
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )
    
    dNow = datetime.now()
        
    initArgs = [ None, None, None, None, None, None, None, None, None, None ]
    
    dSol = datetime( 9999, 12, 11, 10, 9, 8 )
    
    res = self.rsDB.setSiteStatus( *tuple( initArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    #Test first parameter ( siteName )
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'LCG.CERN.ch'
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'LCG.CERN.ch' ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = 'eGGs'
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'eGGs' ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'eGGs' ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ None ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'eGGs', None ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    #Test second parameter ( statusType )
    modArgs[ 1 ] = ''
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ '' ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ 'eGGs' ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ '', 'eGGs' ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ None ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ '', 'eGGs', None ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )

    #Test third parameter ( status )
    modArgs[ 2 ] = 'Active'
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ 'Active' ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = 'eGGs'
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ 'eGGs' ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ 'Active', 'eGGs' ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ None ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ 'Active', 'eGGs', None ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    #Test forth parameter ( reason )
    modArgs = initArgs[:]
    modArgs[ 3 ] = 'Init'
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 3 ] = [ 'Init' ]    
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 3 ] = 'eGGs'
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 3 ] = [ 'eGGs' ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 3 ] = [ 'Init', 'eGGs' ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 3 ] = [ None ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )         
    modArgs[ 3 ] = [ 'Init', 'eGGs', None ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )  

    #Test fifth parameter ( dateCreated )
    modArgs = initArgs[:]
    modArgs[ 4 ] = dSol
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 4 ] = [ dSol ]    
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )     
    modArgs[ 4 ] = dNow
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 4 ] = [ dNow ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 4 ] = [ dSol, dNow ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 4 ] = [ None ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 4 ] = [ dSol, dNow, None ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
            
    #Test sixth parameter ( dateEffective ) 
    modArgs = initArgs[:]
    modArgs[ 5 ] = dSol
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )    
    modArgs[ 5 ] = [ dSol ]    
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )         
    modArgs[ 5 ] = dNow
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )  
    modArgs[ 5 ] = [ dNow ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 5 ] = [ dSol, dNow ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )     
    modArgs[ 5 ] = [ None ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 5 ] = [ dSol, dNow, None ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )     

    #Test seventh parameter ( dateEnd )
    modArgs = initArgs[:]
    modArgs[ 6 ] = dSol
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )   
    modArgs[ 6 ] = [ dSol ]    
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )        
    modArgs[ 6 ] = dNow
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 6 ] = [ dNow ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 6 ] = [ dSol, dNow ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 6 ] = [ None ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 6 ] = [ dSol, dNow, None ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
            
    #Test eighth parameter ( lastCheckTime )
    modArgs = initArgs[:]
    modArgs[ 7 ] = dSol
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )   
    modArgs[ 7 ] = [ dSol ]    
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )        
    modArgs[ 7 ] = dNow
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 7 ] = [ dNow ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 7 ] = [ dSol, dNow ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 7 ] = [ None ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 7 ] = [ dSol, dNow, None ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    #Test ninth parameter ( tokenOwner )
    modArgs = initArgs[:]
    modArgs[ 8 ] = 'RS_SVC'
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 8 ] = [ 'RS_SVC' ]   
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )   
    modArgs[ 8 ] = 'eGGs'   
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 8 ] = [ 'eGGs' ]   
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs' ]   
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 8 ] = [ None ]   
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs', None ]   
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    #Test tenth parameter ( tokenExpiration )
    modArgs = initArgs[:]
    modArgs[ 9 ] = dSol
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )   
    modArgs[ 9 ] = [ dSol ]    
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )        
    modArgs[ 9 ] = dNow
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 9 ] = [ dNow ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 9 ] = [ dSol, dNow ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 9 ] = [ None ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )    
    modArgs[ 9 ] = [ dSol, dNow, None ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'LCG.CERN.ch'
    modArgs[ 1 ] = ''
    modArgs[ 2 ] = 'Active'    
    modArgs[ 3 ] = 'Reason'
    modArgs[ 8 ] = 'token'
    
    #This should work, but the DB is empty
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    modArgs[ 0 ] =  [ 'LCG.CERN.ch' ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] =  'LCG.CERN.ch'
    modArgs[ 1 ] =  ''
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = ''
    modArgs[ 2 ] = [ 'Active' ]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = 'Active'
    modArgs[ 4 ] = 'eGGs'
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 4 ] = None
    modArgs[ 5 ] = 'eGGs'
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 5 ] = None
    modArgs[ 6 ] = 'eGGs'
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 6 ] = None
    modArgs[ 7 ] = 'eGGs'
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 7 ] = None
    modArgs[ 9 ] = 'eGGs'
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    now = datetime.now()
    modArgs = [ 'eGGs', '', 'Active', None, now, now, now, now, None, now]
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = 'LCG.CERN.ch'
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = ''
    modArgs[ 2 ] = 'eGGs'
    res = self.rsDB.setSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
  def test_03_setSiteScheduledStatus( self ):
    '''
    setSiteScheduledStatus( self, siteName, statusType, status, reason, dateCreated, 
                            dateEffective, dateEnd, lastCheckTime, tokenOwner, 
                            tokenExpiration )
    '''   
        
    ins = inspect.getargspec( self.rsDB.setSiteScheduledStatus.f )   
    self.assertEqual( ins.args, [ 'self', 'siteName', 'statusType', 'status', 'reason', 'dateCreated', 
                                  'dateEffective', 'dateEnd', 'lastCheckTime', 'tokenOwner', 
                                  'tokenExpiration' ] )
    self.assertEqual( ins.varargs,  None)
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )
    
    dNow = datetime.now()
        
    initArgs = [ None, None, None, None, None, None, None, None, None, None ]
    
    dSol = datetime( 9999, 12, 11, 10, 9, 8 )
    
    res = self.rsDB.setSiteScheduledStatus( *tuple( initArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    #Test first parameter ( siteName )
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'LCG.CERN.ch'
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'LCG.CERN.ch' ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = 'eGGs'
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'eGGs' ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'eGGs' ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ None ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'eGGs', None ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    #Test second parameter ( statusType )
    modArgs[ 1 ] = ''
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ '' ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ 'eGGs' ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ '', 'eGGs' ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ None ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ '', 'eGGs', None ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )

    #Test third parameter ( status )
    modArgs[ 2 ] = 'Active'
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ 'Active' ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = 'eGGs'
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ 'eGGs' ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ 'Active', 'eGGs' ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ None ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ 'Active', 'eGGs', None ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    #Test forth parameter ( reason )
    modArgs = initArgs[:]
    modArgs[ 3 ] = 'Init'
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 3 ] = [ 'Init' ]    
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 3 ] = 'eGGs'
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 3 ] = [ 'eGGs' ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 3 ] = [ 'Init', 'eGGs' ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 3 ] = [ None ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )         
    modArgs[ 3 ] = [ 'Init', 'eGGs', None ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )  

    #Test fifth parameter ( dateCreated )
    modArgs = initArgs[:]
    modArgs[ 4 ] = dSol
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 4 ] = [ dSol ]    
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )     
    modArgs[ 4 ] = dNow
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 4 ] = [ dNow ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 4 ] = [ dSol, dNow ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 4 ] = [ None ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 4 ] = [ dSol, dNow, None ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
            
    #Test sixth parameter ( dateEffective ) 
    modArgs = initArgs[:]
    modArgs[ 5 ] = dSol
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )    
    modArgs[ 5 ] = [ dSol ]    
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )         
    modArgs[ 5 ] = dNow
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )  
    modArgs[ 5 ] = [ dNow ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 5 ] = [ dSol, dNow ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )     
    modArgs[ 5 ] = [ None ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 5 ] = [ dSol, dNow, None ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )     

    #Test seventh parameter ( dateEnd )
    modArgs = initArgs[:]
    modArgs[ 6 ] = dSol
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )   
    modArgs[ 6 ] = [ dSol ]    
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )        
    modArgs[ 6 ] = dNow
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 6 ] = [ dNow ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 6 ] = [ dSol, dNow ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 6 ] = [ None ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 6 ] = [ dSol, dNow, None ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
            
    #Test eighth parameter ( lastCheckTime )
    modArgs = initArgs[:]
    modArgs[ 7 ] = dSol
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )   
    modArgs[ 7 ] = [ dSol ]    
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )        
    modArgs[ 7 ] = dNow
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 7 ] = [ dNow ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 7 ] = [ dSol, dNow ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 7 ] = [ None ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 7 ] = [ dSol, dNow, None ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    #Test ninth parameter ( tokenOwner )
    modArgs = initArgs[:]
    modArgs[ 8 ] = 'RS_SVC'
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 8 ] = [ 'RS_SVC' ]   
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )   
    modArgs[ 8 ] = 'eGGs'   
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 8 ] = [ 'eGGs' ]   
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs' ]   
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 8 ] = [ None ]   
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs', None ]   
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    #Test tenth parameter ( tokenExpiration )
    modArgs = initArgs[:]
    modArgs[ 9 ] = dSol
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )   
    modArgs[ 9 ] = [ dSol ]    
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )        
    modArgs[ 9 ] = dNow
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 9 ] = [ dNow ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 9 ] = [ dSol, dNow ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 9 ] = [ None ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )    
    modArgs[ 9 ] = [ dSol, dNow, None ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'LCG.CERN.ch'
    modArgs[ 1 ] = ''
    modArgs[ 2 ] = 'Active'    
    modArgs[ 3 ] = 'Reason'
    modArgs[ 8 ] = 'token'
    
    #This should work, but the DB is empty
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    modArgs[ 0 ] =  [ 'LCG.CERN.ch' ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] =  'LCG.CERN.ch'
    modArgs[ 1 ] =  ''
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = ''
    modArgs[ 2 ] = [ 'Active' ]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = 'Active'
    modArgs[ 4 ] = 'eGGs'
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 4 ] = None
    modArgs[ 5 ] = 'eGGs'
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 5 ] = None
    modArgs[ 6 ] = 'eGGs'
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 6 ] = None
    modArgs[ 7 ] = 'eGGs'
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 7 ] = None
    modArgs[ 9 ] = 'eGGs'
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    now = datetime.now()
    modArgs = [ 'eGGs', '', 'Active', None, now, now, now, now, None, now]
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = 'LCG.CERN.ch'
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = ''
    modArgs[ 2 ] = 'eGGs'
    res = self.rsDB.setSiteScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
  def test_04_updateSiteStatus( self ):
    '''
    updateSiteStatus( self, siteName, statusType, status, reason, dateCreated, 
                      dateEffective, dateEnd, lastCheckTime, tokenOwner, 
                      tokenExpiration )
    '''
    
    ins = inspect.getargspec( self.rsDB.updateSiteStatus.f )   
    self.assertEqual( ins.args, [ 'self', 'siteName', 'statusType', 'status', 'reason', 'dateCreated', 
                                  'dateEffective', 'dateEnd', 'lastCheckTime', 'tokenOwner', 
                                  'tokenExpiration' ] )
    self.assertEqual( ins.varargs,  None)
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )
    
    dNow = datetime.now()
    
    initArgs = [ None, None, None, None, None, None, None, None, None, None ]
    
    dSol= datetime( 9999, 12, 11, 10, 9, 8 )
    
    res = self.rsDB.updateSiteStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    
    '''
      Param tests
    '''    
    #Test first parameter ( siteName )
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'LCG.CERN.ch'
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'LCG.CERN.ch' ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = 'eGGs'
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'eGGs' ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'eGGs' ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ None ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'eGGs', None ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    #Test second parameter ( statusType )
    modArgs[ 1 ] = ''
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ '' ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ 'eGGs' ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ '', 'eGGs' ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ None ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ '', 'eGGs', None ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )

    #Test third parameter ( status )
    modArgs[ 2 ] = 'Active'
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ 'Active' ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = 'eGGs'
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ 'eGGs' ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ 'Active', 'eGGs' ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ None ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ 'Active', 'eGGs', None ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    #Test forth parameter ( reason )
    modArgs = initArgs[:]
    modArgs[ 3 ] = 'Init'
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 3 ] = [ 'Init' ]    
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 3 ] = 'eGGs'
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 3 ] = [ 'eGGs' ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 3 ] = [ 'Init', 'eGGs' ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 3 ] = [ None ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )         
    modArgs[ 3 ] = [ 'Init', 'eGGs', None ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )  

    #Test fifth parameter ( dateCreated )
    modArgs = initArgs[:]
    modArgs[ 4 ] = dSol
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 4 ] = [ dSol ]    
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )     
    modArgs[ 4 ] = dNow
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 4 ] = [ dNow ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 4 ] = [ dSol, dNow ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 4 ] = [ None ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 4 ] = [ dSol, dNow, None ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
            
    #Test sixth parameter ( dateEffective ) 
    modArgs = initArgs[:]
    modArgs[ 5 ] = dSol
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )    
    modArgs[ 5 ] = [ dSol ]    
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )         
    modArgs[ 5 ] = dNow
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )  
    modArgs[ 5 ] = [ dNow ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 5 ] = [ dSol, dNow ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )     
    modArgs[ 5 ] = [ None ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 5 ] = [ dSol, dNow, None ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )     

    #Test seventh parameter ( dateEnd )
    modArgs = initArgs[:]
    modArgs[ 6 ] = dSol
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )   
    modArgs[ 6 ] = [ dSol ]    
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )        
    modArgs[ 6 ] = dNow
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 6 ] = [ dNow ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 6 ] = [ dSol, dNow ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 6 ] = [ None ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 6 ] = [ dSol, dNow, None ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
            
    #Test eighth parameter ( lastCheckTime )
    modArgs = initArgs[:]
    modArgs[ 7 ] = dSol
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )   
    modArgs[ 7 ] = [ dSol ]    
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )        
    modArgs[ 7 ] = dNow
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 7 ] = [ dNow ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 7 ] = [ dSol, dNow ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 7 ] = [ None ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 7 ] = [ dSol, dNow, None ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    #Test ninth parameter ( tokenOwner )
    modArgs = initArgs[:]
    modArgs[ 8 ] = 'RS_SVC'
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 8 ] = [ 'RS_SVC' ]   
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )  
    modArgs[ 8 ] = 'eGGs'   
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 8 ] = [ 'eGGs' ]   
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs' ]   
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 8 ] = [ None ]   
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs', None ]   
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    #Test tenth parameter ( tokenExpiration )
    modArgs = initArgs[:]
    modArgs[ 9 ] = dSol
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )   
    modArgs[ 9 ] = [ dSol ]    
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )        
    modArgs[ 9 ] = dNow
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 9 ] = [ dNow ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 9 ] = [ dSol, dNow ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 9 ] = [ None ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )    
    modArgs[ 9 ] = [ dSol, dNow, None ]
    res = self.rsDB.updateSiteStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
       
    
    res = self.rsDB.updateSiteStatus( 'LCG.CERN.ch', '', 'Active', None, None,
                                       None,None,None,None,None )
    self.assertEqual( res[ 'OK' ], False )
    
    res = self.rsDB.updateSiteStatus( ['LCG.CERN.ch'], '', 'Active', None, None,
                                            None, None, None, None, None )
    self.assertEqual( res[ 'OK' ], False )
    res = self.rsDB.updateSiteStatus( 'LCG.CERN.ch', [''], 'Active', None, None,
                                            None, None, None, None, None )
    self.assertEqual( res[ 'OK' ], False )
    res = self.rsDB.updateSiteStatus( 'LCG.CERN.ch', '', ['Active'], None, None,
                                            None, None, None, None, None )
    self.assertEqual( res[ 'OK' ], False )
    res = self.rsDB.updateSiteStatus( 'LCG.CERN.ch', '', 'Active', None, 'eGGs',
                                            None, None, None, None, None )
    self.assertEqual( res[ 'OK' ], False )
    res = self.rsDB.updateSiteStatus( 'LCG.CERN.ch', '', 'Active', None, None,
                                            'eGGs', None, None, None, None )
    self.assertEqual( res[ 'OK' ], False )
    res = self.rsDB.updateSiteStatus( 'LCG.CERN.ch', '', 'Active', None, None,
                                            None, 'eGGs', None, None, None )
    self.assertEqual( res[ 'OK' ], False )
    res = self.rsDB.updateSiteStatus( 'LCG.CERN.ch', '', 'Active', None, None,
                                            None, None, 'eGGs', None, None )
    self.assertEqual( res[ 'OK' ], False )
    res = self.rsDB.updateSiteStatus( 'LCG.CERN.ch', '', 'Active', None, None,
                                            None, None, None, None, 'eGGs' )
    self.assertEqual( res[ 'OK' ], False )
    
    now = datetime.now()
    
    #Our DB is empty, some validations must break !
    res = self.rsDB.updateSiteStatus( 'LCG.CERN.ch', '', 'Active', None, now,
                                            now, now, now, None, now )
    self.assertEqual( res[ 'OK' ], False )
    res = self.rsDB.updateSiteStatus( 'eGGs', '', 'Active', None, now,
                                            now, now, now, None, now )
    self.assertEqual( res[ 'OK' ], False )
    res = self.rsDB.updateSiteStatus( 'LCG.CERN.ch', 'eGGs', 'Active', None, now,
                                            now, now, now, None, now )
    self.assertEqual( res[ 'OK' ], False )
    res = self.rsDB.updateSiteStatus( 'LCG.CERN.ch', '', 'eGGs', None, now,
                                       now, now, now, None, now )
    self.assertEqual( res[ 'OK' ], False )
        
  def test_05_getSites( self ):
    '''
    getSites( self, siteName, siteType, gridSiteName, **kwargs )
    '''
    ins = inspect.getargspec( self.rsDB.getSites.f )   
    self.assertEqual( ins.args, [ 'self', 'siteName', 'siteType', 'gridSiteName' ] )
    self.assertEqual( ins.varargs,  None)
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, None )
    
    initArgs = [ None, None, None ]
    
    res = self.rsDB.getSites( *tuple( initArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    
    '''
      Param tests
    '''    
    #Test first param ( siteName )
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'LCG.CERN.ch'
    res = self.rsDB.getSites( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 0 ] = [ 'LCG.CNAF.it' ]
    res = self.rsDB.getSites( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'LCG.CNAF.it' ]
    res = self.rsDB.getSites( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = 'eGGs'
    res = self.rsDB.getSites( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'eGGs' ]
    res = self.rsDB.getSites( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'LCG.CNAF.it', 'eGGs' ]
    res = self.rsDB.getSites( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ None ]
    res = self.rsDB.getSites( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'LCG.CNAF.it', 'eGGs', None ]
    res = self.rsDB.getSites( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 

    #Test second param ( siteType )
    modArgs = initArgs[:]
    modArgs[ 1 ] = 'T0'
    res = self.rsDB.getSites( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ 'T1' ]
    res = self.rsDB.getSites( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ 'T0', 'T1' ]
    res = self.rsDB.getSites( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.getSites( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ 'eGGs' ]
    res = self.rsDB.getSites( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ 'T0', 'T1', 'eGGs' ]
    res = self.rsDB.getSites( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ None ]
    res = self.rsDB.getSites( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ 'T0', 'T1', 'eGGs', None ]
    res = self.rsDB.getSites( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )

    #Test third param ( gridSiteName )
    modArgs = initArgs[:]
    modArgs[ 2 ] = 'CERN-PROD'
    res = self.rsDB.getSites( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = [ 'INFN-T1' ]
    res = self.rsDB.getSites( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = [ 'CERN-PROD', 'INFN-T1' ]
    res = self.rsDB.getSites( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = 'eGGs'
    res = self.rsDB.getSites( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = [ 'eGGs' ]
    res = self.rsDB.getSites( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = [ 'CERN-PROD', 'INFN-T1', 'eGGs' ]
    res = self.rsDB.getSites( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = [ None ]
    res = self.rsDB.getSites( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = [ 'CERN-PROD', 'INFN-T1', 'eGGs', None ]
    res = self.rsDB.getSites( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    
  def test_06_getSitesStatus( self ):
    '''
    getSitesStatus( self, siteName, statusType, status, reason, dateCreated, 
                    dateEffective, dateEnd, lastCheckTime, tokenOwner, 
                    tokenExpiration, **kwargs )
    '''
    dNow = datetime.now()
    
    ins = inspect.getargspec( self.rsDB.getSitesStatus.f )   
    self.assertEqual( ins.args, [ 'self', 'siteName', 'statusType', 'status', 'reason', 'dateCreated', 
                                  'dateEffective', 'dateEnd', 'lastCheckTime', 'tokenOwner', 
                                  'tokenExpiration' ] )
    self.assertEqual( ins.varargs,  None)
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, None )
    
    initArgs = [ None,None,None,None,None,None,None,None,None,None ]
    dSol     = datetime(9999, 12, 11, 10, 9, 8)
        
    res = self.rsDB.getSitesStatus( *tuple( initArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    
    '''
      Param tests
    '''    
    #Test first param ( siteName )
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'LCG.CERN.ch'
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'LCG.CERN.ch' ]    
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'LCG.CNAF.it' ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = 'LCG.eGGs.xy'
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'LCG.eGGs.xy' ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'LCG.eGGs.xy' ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ None ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'LCG.eGGs.xy', None ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )

    #Test second param ( statusType ) 
    modArgs = initArgs[:]
    modArgs[ 1 ] = ''
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ '' ]    
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ 'eGGs' ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ '', 'eGGs' ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ None ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ '', 'eGGs', None ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )

    #Test third parameter ( status )
    modArgs = initArgs[:]
    modArgs[ 2 ] = 'Active'
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = [ 'Active' ]    
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    modArgs[ 2 ] = 'Banned'
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 2 ] = [ 'Banned' ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = [ 'Active', 'Banned' ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = 'eGGs'
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = [ 'eGGs' ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )    
    modArgs[ 2 ] = [ 'eGGs', 'Banned' ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 2 ] = [ None ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )   
    modArgs[ 2 ] = [ 'eGGs', 'Banned', None ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )

    #Test forth parameter ( reason )
    modArgs = initArgs[:]
    modArgs[ 3 ] = 'Init'
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 3 ] = [ 'Init' ]    
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 3 ] = 'eGGs'
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 3 ] = [ 'eGGs' ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 3 ] = [ 'Init', 'eGGs' ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )         
    modArgs[ 3 ] = [ None ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 3 ] = [ 'Init', 'eGGs', None ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )

    #Test fifth parameter ( dateCreated )
    modArgs = initArgs[:]
    modArgs[ 4 ] = dSol
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 4 ] = [ dSol ]    
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 4 ] = dNow
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 4 ] = [ dNow ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 4 ] = [ dSol, dNow ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )    
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
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 5 ] = [ dSol ]    
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    modArgs[ 5 ] = dNow
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 5 ] = [ dNow ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 5 ] = [ dSol, dNow ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )   
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
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 6 ] = [ dSol ]    
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )      
    modArgs[ 6 ] = dNow
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 6 ] = [ dNow ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 6 ] = [ dSol, dNow ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )   
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
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 7 ] = [ dSol ]    
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    modArgs[ 7 ] = dNow
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 7 ] = [ dNow ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 7 ] = [ dSol, dNow ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )    
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
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'RS_SVC' ]   
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 8 ] = 'eGGs'   
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'eGGs' ]   
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs' ]   
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ None ]   
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs', None ]   
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    
    #Test tenth parameter ( tokenExpiration )
    modArgs = initArgs[:]
    modArgs[ 9 ] = dSol
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 9 ] = [ dSol ]    
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )      
    modArgs[ 9 ] = dNow
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 9 ] = [ dNow ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 9 ] = [ dSol, dNow ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 9 ] = [ None ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 9 ] = [ dSol, dNow, None ]
    res = self.rsDB.getSitesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
            
             
  def test_07_getSitesHistory( self ):
    '''
    getSitesHistory( self, siteName, statusType, status, reason, dateCreated, 
                       dateEffective, dateEnd, lastCheckTime, tokenOwner, 
                       tokenExpiration, **kwargs )
    '''
    dNow = datetime.now()
    
    ins = inspect.getargspec( self.rsDB.getSitesHistory.f )   
    self.assertEqual( ins.args, [ 'self', 'siteName', 'statusType', 'status', 'reason', 'dateCreated', 
                                  'dateEffective', 'dateEnd', 'lastCheckTime', 'tokenOwner', 
                                  'tokenExpiration' ] )
    self.assertEqual( ins.varargs,  None)
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, None )
    
    initArgs = [ None,None,None,None,None,None,None,None,None,None ]
    dSol     = datetime(9999, 12, 11, 10, 9, 8)
        
    res = self.rsDB.getSitesHistory( *tuple( initArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    
    '''
      Param tests
    '''    
    #Test first param ( siteName )
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'LCG.CERN.ch'
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = [ 'LCG.CERN.ch' ]    
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )          
    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'LCG.CNAF.it' ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = 'LCG.eGGs.xy'
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = [ 'LCG.eGGs.xy' ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'LCG.eGGs.xy' ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = [ None ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'LCG.eGGs.xy', None ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  

    #Test second param ( statusType ) 
    modArgs = initArgs[:]
    modArgs[ 1 ] = ''
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 1 ] = [ '' ]    
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 1 ] = [ 'eGGs' ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 1 ] = [ '', 'eGGs' ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 1 ] = [ None ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 1 ] = [ '', 'eGGs', None ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  

    #Test third parameter ( status )
    modArgs = initArgs[:]
    modArgs[ 2 ] = 'Active'
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 2 ] = [ 'Active' ]    
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    modArgs[ 2 ] = 'Banned'
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 2 ] = [ 'Banned' ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 2 ] = [ 'Active', 'Banned' ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 2 ] = 'eGGs'
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 2 ] = [ 'eGGs' ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )     
    modArgs[ 2 ] = [ 'eGGs', 'Banned' ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )         
    modArgs[ 2 ] = [ None ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    modArgs[ 2 ] = [ 'eGGs', 'Banned', None ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  

    #Test forth parameter ( reason )
    modArgs = initArgs[:]
    modArgs[ 3 ] = 'Init'
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 3 ] = [ 'Init' ]    
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )         
    modArgs[ 3 ] = 'eGGs'
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 3 ] = [ 'eGGs' ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 3 ] = [ 'Init', 'eGGs' ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )            
    modArgs[ 3 ] = [ None ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 3 ] = [ 'Init', 'eGGs', None ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  

    #Test fifth parameter ( dateCreated )
    modArgs = initArgs[:]
    modArgs[ 4 ] = dSol
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )     
    modArgs[ 4 ] = [ dSol ]    
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 4 ] = dNow
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 4 ] = [ dNow ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 4 ] = [ dSol, dNow ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )    
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
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 5 ] = [ dSol ]    
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )      
    modArgs[ 5 ] = dNow
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 5 ] = [ dNow ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 5 ] = [ dSol, dNow ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )    
    modArgs[ 5 ] = [ None ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 5 ] = [ dSol, dNow, None ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )

    #Test seventh parameter ( dateEnd )
    modArgs = initArgs[:]
    modArgs[ 6 ] = dSol
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 6 ] = [ dSol ]    
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 6 ] = dNow
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 6 ] = [ dNow ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 6 ] = [ dSol, dNow ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )     
    modArgs[ 6 ] = [ None ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False ) 
    modArgs[ 6 ] = [ dSol, dNow, None ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False ) 
                
    #Test eighth parameter ( lastCheckTime )
    modArgs = initArgs[:]
    modArgs[ 7 ] = dSol
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )   
    modArgs[ 7 ] = [ dSol ]    
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    modArgs[ 7 ] = dNow
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 7 ] = [ dNow ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 7 ] = [ dSol, dNow ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
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
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'RS_SVC' ]   
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = 'eGGs'   
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'eGGs' ]   
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs' ]   
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ None ]   
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs', None ]   
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    
    #Test tenth parameter ( tokenExpiration )
    modArgs = initArgs[:]
    modArgs[ 9 ] = dSol
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 9 ] = [ dSol ]    
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )      
    modArgs[ 9 ] = dNow
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 9 ] = [ dNow ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 9 ] = [ dSol, dNow ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )    
    modArgs[ 9 ] = [ None ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 9 ] = [ dSol, dNow, None ]
    res = self.rsDB.getSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )

  def test_08_getSitesScheduledStatus( self ):
    '''
    getSitesScheduledStatus( self, siteName, statusType, status, reason, dateCreated, 
                             dateEffective, dateEnd, lastCheckTime, tokenOwner, 
                             tokenExpiration, **kwargs )
    '''
    dNow = datetime.now()
    
    ins = inspect.getargspec( self.rsDB.getSitesScheduledStatus.f )   
    self.assertEqual( ins.args, [ 'self', 'siteName', 'statusType', 'status', 'reason', 'dateCreated', 
                                  'dateEffective', 'dateEnd', 'lastCheckTime', 'tokenOwner', 
                                  'tokenExpiration' ] )
    self.assertEqual( ins.varargs,  None)
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, None )
    
    initArgs = [ None,None,None,None,None,None,None,None,None,None ]
    dSol     = datetime(9999, 12, 11, 10, 9, 8)
        
    res = self.rsDB.getSitesScheduledStatus( *tuple( initArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    
    '''
      Param tests
    '''    
    #Test first param ( siteName )
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'LCG.CERN.ch'
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = [ 'LCG.CERN.ch' ]    
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )          
    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'LCG.CNAF.it' ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = 'LCG.eGGs.xy'
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = [ 'LCG.eGGs.xy' ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'LCG.eGGs.xy' ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = [ None ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'LCG.eGGs.xy', None ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  

    #Test second param ( statusType ) 
    modArgs = initArgs[:]
    modArgs[ 1 ] = ''
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 1 ] = [ '' ]    
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 1 ] = [ 'eGGs' ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 1 ] = [ '', 'eGGs' ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 1 ] = [ None ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 1 ] = [ '', 'eGGs', None ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  

    #Test third parameter ( status )
    modArgs = initArgs[:]
    modArgs[ 2 ] = 'Active'
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 2 ] = [ 'Active' ]    
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    modArgs[ 2 ] = 'Banned'
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 2 ] = [ 'Banned' ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 2 ] = [ 'Active', 'Banned' ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 2 ] = 'eGGs'
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 2 ] = [ 'eGGs' ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )     
    modArgs[ 2 ] = [ 'eGGs', 'Banned' ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )         
    modArgs[ 2 ] = [ None ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    modArgs[ 2 ] = [ 'eGGs', 'Banned', None ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  

    #Test forth parameter ( reason )
    modArgs = initArgs[:]
    modArgs[ 3 ] = 'Init'
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 3 ] = [ 'Init' ]    
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )         
    modArgs[ 3 ] = 'eGGs'
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 3 ] = [ 'eGGs' ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 3 ] = [ 'Init', 'eGGs' ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )            
    modArgs[ 3 ] = [ None ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 3 ] = [ 'Init', 'eGGs', None ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  

    #Test fifth parameter ( dateCreated )
    modArgs = initArgs[:]
    modArgs[ 4 ] = dSol
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )     
    modArgs[ 4 ] = [ dSol ]    
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 4 ] = dNow
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 4 ] = [ dNow ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 4 ] = [ dSol, dNow ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )    
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
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 5 ] = [ dSol ]    
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )      
    modArgs[ 5 ] = dNow
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 5 ] = [ dNow ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 5 ] = [ dSol, dNow ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )    
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
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 6 ] = [ dSol ]    
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 6 ] = dNow
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 6 ] = [ dNow ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 6 ] = [ dSol, dNow ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )     
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
    self.assertEquals( res[ 'OK' ], True )   
    modArgs[ 7 ] = [ dSol ]    
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    modArgs[ 7 ] = dNow
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 7 ] = [ dNow ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 7 ] = [ dSol, dNow ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
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
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'RS_SVC' ]   
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = 'eGGs'   
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'eGGs' ]   
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs' ]   
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ None ]   
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs', None ]   
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    
    #Test tenth parameter ( tokenExpiration )
    modArgs = initArgs[:]
    modArgs[ 9 ] = dSol
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 9 ] = [ dSol ]    
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )      
    modArgs[ 9 ] = dNow
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 9 ] = [ dNow ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 9 ] = [ dSol, dNow ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )    
    modArgs[ 9 ] = [ None ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 9 ] = [ dSol, dNow, None ]
    res = self.rsDB.getSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )

  def test_09_getSitesPresent( self ):
    '''
    getSitesPresent( self, siteName, siteType, gridSiteName, gridTier, 
                     statusType, status, dateEffective, reason, lastCheckTime, 
                     tokenOwner, tokenExpiration, formerStatus, **kwargs )
    '''
    dNow = datetime.now()
    
    ins = inspect.getargspec( self.rsDB.getSitesPresent.f )   
    self.assertEqual( ins.args, [ 'self', 'siteName', 'siteType', 'gridSiteName', 'gridTier', 
                     'statusType', 'status', 'dateEffective', 'reason', 'lastCheckTime', 
                     'tokenOwner', 'tokenExpiration', 'formerStatus' ] )
    self.assertEqual( ins.varargs,  None)
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, None )

    
    initArgs = [ None,None,None,None,None,None,None,None,None,None, None, None ]
    dSol     = datetime(9999, 12, 11, 10, 9, 8)

    res = self.rsDB.getSitesPresent( *tuple( initArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    
    '''
      Param tests
    '''    
    #Test first param ( siteName )
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'LCG.CERN.ch'
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = [ 'LCG.CERN.ch' ]    
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )          
    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'LCG.CNAF.it' ]
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = 'LCG.eGGs.xy'
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = [ 'LCG.eGGs.xy' ]
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'LCG.eGGs.xy' ]
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = [ None ]
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'LCG.eGGs.xy', None ]
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  

    #Test second param ( siteType )
    modArgs = initArgs[:]
    modArgs[ 1 ] = 'T0'
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 1 ] = [ 'T0' ]    
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )          
    modArgs[ 1 ] = [ 'T0', 'T1' ]
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 1 ] = 'LCG.eGGs.xy'
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 1 ] = [ 'LCG.eGGs.xy' ]
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 1 ] = [ 'T0', 'LCG.eGGs.xy' ]
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 1 ] = [ None ]
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 1 ] = [ 'T0', 'LCG.eGGs.xy', None ]
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  

    #Test third param ( gridSiteName )
    modArgs = initArgs[:]
    modArgs[ 2 ] = 'CERN-PROD'
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 2 ] = [ 'CERN-PROD' ]    
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )          
    modArgs[ 2 ] = [ 'CERN-PROD', 'INFN-T1' ]
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 2 ] = 'LCG.eGGs.xy'
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 2 ] = [ 'LCG.eGGs.xy' ]
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 2 ] = [ 'CERN-PROD', 'LCG.eGGs.xy' ]
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 2 ] = [ None ]
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 2 ] = [ 'CERN-PROD', 'LCG.eGGs.xy', None ]
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  

    #Test forth param ( gridTier )
    modArgs = initArgs[:]
    modArgs[ 3 ] = 'T0'
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 3 ] = [ 'T0' ]    
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )          
    modArgs[ 3 ] = [ 'T0', 'T1' ]
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 3 ] = 'LCG.eGGs.xy'
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 3 ] = [ 'LCG.eGGs.xy' ]
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 3 ] = [ 'T0', 'LCG.eGGs.xy' ]
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 3 ] = [ None ]
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 3 ] = [ 'T0', 'LCG.eGGs.xy', None ]
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  

    #Test fifth param ( statusType ) 
    modArgs = initArgs[:]
    modArgs[ 4 ] = ''
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 4 ] = [ '' ]    
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 4 ] = 'eGGs'
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 4 ] = [ 'eGGs' ]
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 4 ] = [ '', 'eGGs' ]
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 4 ] = [ None ]
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 4 ] = [ '', 'eGGs', None ]
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  

    #Test sixth parameter ( status )
    modArgs = initArgs[:]
    modArgs[ 5 ] = 'Active'
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 5 ] = [ 'Active' ]    
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    modArgs[ 5 ] = 'Banned'
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 5 ] = [ 'Banned' ]
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 5 ] = [ 'Active', 'Banned' ]
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 5 ] = 'eGGs'
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 5 ] = [ 'eGGs' ]
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )     
    modArgs[ 5 ] = [ 'eGGs', 'Banned' ]
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )         
    modArgs[ 5 ] = [ None ]
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    modArgs[ 5 ] = [ 'eGGs', 'Banned', None ]
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  

    #Test seventh parameter ( dateEffective ) 
    modArgs = initArgs[:]
    modArgs[ 6 ] = dSol
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 6 ] = [ dSol ]    
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )      
    modArgs[ 6 ] = dNow
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 6 ] = [ dNow ]
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 6 ] = [ dSol, dNow ]
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )    
    modArgs[ 6 ] = [ None ]
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 6 ] = [ dSol, dNow, None ]
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )

    #Test eighth parameter ( reason )
    modArgs = initArgs[:]
    modArgs[ 7 ] = 'Init'
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 7 ] = [ 'Init' ]    
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )         
    modArgs[ 7 ] = 'eGGs'
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 7 ] = [ 'eGGs' ]
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 7 ] = [ 'Init', 'eGGs' ]
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )            
    modArgs[ 7 ] = [ None ]
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 7 ] = [ 'Init', 'eGGs', None ]
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
                
    #Test ninth parameter ( lastCheckTime )
    modArgs = initArgs[:]
    modArgs[ 8 ] = dSol
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )   
    modArgs[ 8 ] = [ dSol ]    
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    modArgs[ 8 ] = dNow
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 8 ] = [ dNow ]
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ dSol, dNow ]
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ None ]
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 8 ] = [ dSol, dNow, None ]
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
        
    #Test tenth parameter ( tokenOwner )
    modArgs = initArgs[:]
    modArgs[ 9 ] = 'RS_SVC'
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 9 ] = [ 'RS_SVC' ]   
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 9 ] = 'eGGs'   
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 9 ] = [ 'eGGs' ]   
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 9 ] = [ 'RS_SVC','eGGs' ]   
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 9 ] = [ None ]   
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 9 ] = [ 'RS_SVC','eGGs', None ]   
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    
    #Test eleventh parameter ( tokenExpiration )
    modArgs = initArgs[:]
    modArgs[ 10 ] = dSol
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 10 ] = [ dSol ]    
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )      
    modArgs[ 10 ] = dNow
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 10 ] = [ dNow ]
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 10 ] = [ dSol, dNow ]
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )    
    modArgs[ 10 ] = [ None ]
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 10 ] = [ dSol, dNow, None ]
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )

    #Test 12th parameter ( former status )
    modArgs = initArgs[:]
    modArgs[ 11 ] = 'Active'
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 11 ] = [ 'Active' ]    
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    modArgs[ 11 ] = 'Banned'
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 11 ] = [ 'Banned' ]
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 11 ] = [ 'Active', 'Banned' ]
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 11 ] = 'eGGs'
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 11 ] = [ 'eGGs' ]
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )     
    modArgs[ 11 ] = [ 'eGGs', 'Banned' ]
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )         
    modArgs[ 11 ] = [ None ]
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    modArgs[ 11 ] = [ 'eGGs', 'Banned', None ]
    res = self.rsDB.getSitesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  

  def test_10_deleteSitesScheduledStatus( self ):
    '''
    deleteSitesScheduledStatus( self, siteName, statusType, status, reason, 
                                dateCreated, dateEffective, dateEnd, 
                                lastCheckTime, tokenOwner, tokenExpiration, **kwargs)
    '''
    dNow = datetime.now()
    
    ins = inspect.getargspec( self.rsDB.deleteSitesScheduledStatus.f )   
    self.assertEqual( ins.args, [ 'self', 'siteName', 'statusType', 'status', 'reason', 
                                  'dateCreated', 'dateEffective', 'dateEnd', 
                                  'lastCheckTime', 'tokenOwner', 'tokenExpiration' ] )
    self.assertEqual( ins.varargs,  None)
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, None )        
       
    dSol     = datetime(9999, 12, 11, 10, 9, 8)      
    initArgs = [ None,None,None,None,None,None,None,None,None,None ]
   
       
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( initArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    
    '''
      Param tests
    '''    
    #Test first param ( siteName )
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'LCG.CERN.ch'
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'LCG.CERN.ch' ]    
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'LCG.CNAF.it' ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = 'LCG.eGGs.xy'
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'LCG.eGGs.xy' ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'LCG.eGGs.xy' ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ None ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'LCG.eGGs.xy', None ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )

    #Test second param ( statusType ) 
    modArgs = initArgs[:]
    modArgs[ 1 ] = ''
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ '' ]    
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 1 ] = [ 'eGGs' ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ '', 'eGGs' ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 1 ] = [ None ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ '', 'eGGs', None ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )

    #Test third parameter ( status )
    modArgs = initArgs[:]
    modArgs[ 2 ] = 'Active'
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = [ 'Active' ]    
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )         
    modArgs[ 2 ] = 'Banned'
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 2 ] = [ 'Banned' ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = [ 'Active', 'Banned' ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 2 ] = 'eGGs'
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = [ 'eGGs' ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )     
    modArgs[ 2 ] = [ 'eGGs', 'Banned' ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )      
    modArgs[ 2 ] = [ None ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 2 ] = [ 'eGGs', 'Banned', None ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )

    #Test forth parameter ( reason )
    modArgs = initArgs[:]
    modArgs[ 3 ] = 'Init'
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 3 ] = [ 'Init' ]    
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )     
    modArgs[ 3 ] = 'eGGs'
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 3 ] = [ 'eGGs' ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 3 ] = [ 'Init', 'eGGs' ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 3 ] = [ None ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 3 ] = [ 'Init', 'eGGs', None ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )

    #Test fifth parameter ( dateCreated )
    modArgs = initArgs[:]
    modArgs[ 4 ] = dSol
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 4 ] = [ dSol ]    
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    modArgs[ 4 ] = dNow
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 4 ] = [ dNow ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 4 ] = [ dSol, dNow ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )     
    modArgs[ 4 ] = [ None ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 4 ] = [ dSol, dNow, None ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
            
    #Test sixth parameter ( dateEffective ) 
    modArgs = initArgs[:]
    modArgs[ 5 ] = dSol
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 5 ] = [ dSol ]    
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 5 ] = dNow
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 5 ] = [ dNow ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 5 ] = [ dSol, dNow ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )   
    modArgs[ 5 ] = [ None ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 5 ] = [ dSol, dNow, None ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False ) 

    #Test seventh parameter ( dateEnd )
    modArgs = initArgs[:]
    modArgs[ 6 ] = dSol
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )    
    modArgs[ 6 ] = [ dSol ]    
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )      
    modArgs[ 6 ] = dNow
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 6 ] = [ dNow ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 6 ] = [ dSol, dNow ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )     
    modArgs[ 6 ] = [ None ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 6 ] = [ dSol, dNow, None ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
                
    #Test eighth parameter ( lastCheckTime )
    modArgs = initArgs[:]
    modArgs[ 7 ] = dSol
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )   
    modArgs[ 7 ] = [ dSol ]    
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 7 ] = dNow
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 7 ] = [ dNow ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 7 ] = [ dSol, dNow ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )   
    modArgs[ 7 ] = [ None ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 7 ] = [ dSol, dNow, None ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
        
    #Test ninth parameter ( tokenOwner )
    modArgs = initArgs[:]
    modArgs[ 8 ] = 'RS_SVC'
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'RS_SVC' ]   
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 8 ] = 'eGGs'   
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 8 ] = [ 'eGGs' ]   
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs' ]   
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ None ]   
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs', None ]   
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    
    #Test tenth parameter ( tokenExpiration )
    modArgs = initArgs[:]
    modArgs[ 9 ] = dSol
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )   
    modArgs[ 9 ] = [ dSol ]    
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )      
    modArgs[ 9 ] = dNow
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 9 ] = [ dNow ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 9 ] = [ dSol, dNow ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )   
    modArgs[ 9 ] = [ None ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 9 ] = [ dSol, dNow, None ]
    res = self.rsDB.deleteSitesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )

  def test_11_deleteSitesHistory( self ):
    '''
    deleteSitesHistory( self, siteName, statusType, status, reason, 
                        dateCreated, dateEffective, dateEnd, 
                        lastCheckTime, tokenOwner, tokenExpiration, kwargs )
    '''
    dNow = datetime.now()
    
    ins = inspect.getargspec( self.rsDB.deleteSitesHistory.f )   
    self.assertEqual( ins.args, [ 'self', 'siteName', 'statusType', 'status', 'reason', 
                                  'dateCreated', 'dateEffective', 'dateEnd', 
                                  'lastCheckTime', 'tokenOwner', 'tokenExpiration' ] )
    self.assertEqual( ins.varargs,  None)
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, None )        
       
    dSol     = datetime(9999, 12, 11, 10, 9, 8)      
    initArgs = [ None,None,None,None,None,None,None,None,None,None ]
   
       
    res = self.rsDB.deleteSitesHistory( *tuple( initArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    
    '''
      Param tests
    '''    
    #Test first param ( siteName )
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'LCG.CERN.ch'
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'LCG.CERN.ch' ]    
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'LCG.CNAF.it' ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = 'LCG.eGGs.xy'
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'LCG.eGGs.xy' ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'LCG.eGGs.xy' ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ None ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'LCG.CERN.ch', 'LCG.eGGs.xy', None ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )

    #Test second param ( statusType ) 
    modArgs = initArgs[:]
    modArgs[ 1 ] = ''
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ '' ]    
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 1 ] = [ 'eGGs' ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ '', 'eGGs' ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 1 ] = [ None ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ '', 'eGGs', None ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )

    #Test third parameter ( status )
    modArgs = initArgs[:]
    modArgs[ 2 ] = 'Active'
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = [ 'Active' ]    
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )         
    modArgs[ 2 ] = 'Banned'
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 2 ] = [ 'Banned' ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = [ 'Active', 'Banned' ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 2 ] = 'eGGs'
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = [ 'eGGs' ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )     
    modArgs[ 2 ] = [ 'eGGs', 'Banned' ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )      
    modArgs[ 2 ] = [ None ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 2 ] = [ 'eGGs', 'Banned', None ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )

    #Test forth parameter ( reason )
    modArgs = initArgs[:]
    modArgs[ 3 ] = 'Init'
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 3 ] = [ 'Init' ]    
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )     
    modArgs[ 3 ] = 'eGGs'
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 3 ] = [ 'eGGs' ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 3 ] = [ 'Init', 'eGGs' ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 3 ] = [ None ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 3 ] = [ 'Init', 'eGGs', None ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )

    #Test fifth parameter ( dateCreated )
    modArgs = initArgs[:]
    modArgs[ 4 ] = dSol
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 4 ] = [ dSol ]    
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    modArgs[ 4 ] = dNow
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 4 ] = [ dNow ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 4 ] = [ dSol, dNow ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )     
    modArgs[ 4 ] = [ None ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 4 ] = [ dSol, dNow, None ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
            
    #Test sixth parameter ( dateEffective ) 
    modArgs = initArgs[:]
    modArgs[ 5 ] = dSol
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 5 ] = [ dSol ]    
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 5 ] = dNow
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 5 ] = [ dNow ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 5 ] = [ dSol, dNow ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )   
    modArgs[ 5 ] = [ None ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 5 ] = [ dSol, dNow, None ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False ) 

    #Test seventh parameter ( dateEnd )
    modArgs = initArgs[:]
    modArgs[ 6 ] = dSol
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )    
    modArgs[ 6 ] = [ dSol ]    
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )      
    modArgs[ 6 ] = dNow
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 6 ] = [ dNow ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 6 ] = [ dSol, dNow ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )     
    modArgs[ 6 ] = [ None ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 6 ] = [ dSol, dNow, None ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
                
    #Test eighth parameter ( lastCheckTime )
    modArgs = initArgs[:]
    modArgs[ 7 ] = dSol
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )   
    modArgs[ 7 ] = [ dSol ]    
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 7 ] = dNow
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 7 ] = [ dNow ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 7 ] = [ dSol, dNow ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )   
    modArgs[ 7 ] = [ None ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 7 ] = [ dSol, dNow, None ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
        
    #Test ninth parameter ( tokenOwner )
    modArgs = initArgs[:]
    modArgs[ 8 ] = 'RS_SVC'
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'RS_SVC' ]   
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 8 ] = 'eGGs'   
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 8 ] = [ 'eGGs' ]   
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs' ]   
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ None ]   
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs', None ]   
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    
    #Test tenth parameter ( tokenExpiration )
    modArgs = initArgs[:]
    modArgs[ 9 ] = dSol
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )   
    modArgs[ 9 ] = [ dSol ]    
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )      
    modArgs[ 9 ] = dNow
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 9 ] = [ dNow ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 9 ] = [ dSol, dNow ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )   
    modArgs[ 9 ] = [ None ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 9 ] = [ dSol, dNow, None ]
    res = self.rsDB.deleteSitesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )

  def test_12_deleteSites( self ):
    '''
    deleteSites( self, siteName )
    '''

    ins = inspect.getargspec( self.rsDB.deleteSites.f )   
    self.assertEqual( ins.args, [ 'self', 'siteName' ] )
    self.assertEqual( ins.varargs,  None)
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )       

    #Test first param
    res = self.rsDB.deleteSites( 'LCG.CERN.ch' )
    self.assertEquals( res[ 'OK' ], True )
    res = self.rsDB.deleteSites( [ 'LCG.CERN.ch' ] )
    self.assertEquals( res[ 'OK' ], True )
    res = self.rsDB.deleteSites( 'eGGs' )
    self.assertEquals( res[ 'OK' ], True )
    res = self.rsDB.deleteSites( [ 'eGGs' ] )
    self.assertEquals( res[ 'OK' ], True )
    res = self.rsDB.deleteSites( None )
    self.assertEquals( res[ 'OK' ], False )
    res = self.rsDB.deleteSites( [ None ] )
    self.assertEquals( res[ 'OK' ], False )
    res = self.rsDB.deleteSites( [ 'LCG.CERN.ch', None ] )
    self.assertEquals( res[ 'OK' ], False )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF