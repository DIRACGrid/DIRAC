from DIRAC import S_OK
from DIRAC.ResourceStatusSystem.DB.test.TestSuite_ResourceStatusDB.TestCase_EmptyDB import TestCase_EmptyDB

from datetime import datetime
import inspect

class Test_EmptyDB_Resources( TestCase_EmptyDB ):

  def test_01_addOrModifyResource( self ):
    '''
    addOrModifyResource( self, resourceName, resourceType, serviceType, siteName,
                           gridSiteName )
    '''
    
    ins = inspect.getargspec( self.rsDB.addOrModifyResource.f )   
    self.assertEqual( ins.args, [ 'self', 'resourceName', 'resourceType', 
                                  'serviceType', 'siteName', 'gridSiteName' ] )
    self.assertEqual( ins.varargs,  None)
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )
    
    initArgs = [ None, None, None, None, None ]
    
    #Fails because second parameter does not validate
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'xyz.cern.ch'
    res = self.rsDB.addOrModifyResource( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'xyz.cern.ch' ]
    res = self.rsDB.addOrModifyResource( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = 'eGGs'
    res = self.rsDB.addOrModifyResource( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'eGGs' ]
    res = self.rsDB.addOrModifyResource( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'xyz.cern.ch', 'eGGs' ]
    res = self.rsDB.addOrModifyResource( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ None ]
    res = self.rsDB.addOrModifyResource( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'xyz.cern.ch', 'eGGs', None ]
    res = self.rsDB.addOrModifyResource( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )

    #Fail because first parameter does not validate
    modArgs = initArgs[:]
    modArgs[ 1 ] = 'CE'
    res = self.rsDB.addOrModifyResource( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ 'CE' ]
    res = self.rsDB.addOrModifyResource( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.addOrModifyResource( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ 'eGGs' ]
    res = self.rsDB.addOrModifyResource( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ 'CE', 'eGGs' ]
    res = self.rsDB.addOrModifyResource( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ None ]
    res = self.rsDB.addOrModifyResource( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ 'CE', 'eGGs', None ]
    res = self.rsDB.addOrModifyResource( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )    

    #Fail because first parameter does not validate
    modArgs = initArgs[:]
    modArgs[ 2 ] = 'Computing'
    res = self.rsDB.addOrModifyResource( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ 'Computing' ]
    res = self.rsDB.addOrModifyResource( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = 'eGGs'
    res = self.rsDB.addOrModifyResource( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ 'eGGs' ]
    res = self.rsDB.addOrModifyResource( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ 'Computing', 'eGGs' ]
    res = self.rsDB.addOrModifyResource( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ None ]
    res = self.rsDB.addOrModifyResource( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ 'Computing', 'eGGs', None ]
    res = self.rsDB.addOrModifyResource( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )     

    modArgs = initArgs[:]
    modArgs[ 3 ] = 'LCG.CERN.ch'
    res = self.rsDB.addOrModifyResource( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 3 ] = [ 'LCG.CERN.ch' ]
    res = self.rsDB.addOrModifyResource( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 3 ] = 'eGGs'
    res = self.rsDB.addOrModifyResource( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 3 ] = [ 'eGGs' ]
    res = self.rsDB.addOrModifyResource( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 3 ] = [ 'LCG.CERN.ch', 'eGGs' ]
    res = self.rsDB.addOrModifyResource( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 3 ] = [ None ]
    res = self.rsDB.addOrModifyResource( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 3 ] = [ 'LCG.CERN.ch', 'eGGs', None ]
    res = self.rsDB.addOrModifyResource( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )     

    modArgs = initArgs[:]
    modArgs[ 4 ] = 'CERN-PROD'
    res = self.rsDB.addOrModifyResource( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 4 ] = [ 'CERN-PROD' ]
    res = self.rsDB.addOrModifyResource( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 4 ] = 'eGGs'
    res = self.rsDB.addOrModifyResource( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 4 ] = [ 'eGGs' ]
    res = self.rsDB.addOrModifyResource( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 4 ] = [ 'CERN-PROD', 'eGGs' ]
    res = self.rsDB.addOrModifyResource( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 4 ] = [ None ]
    res = self.rsDB.addOrModifyResource( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 4 ] = [ 'CERN-PROD', 'eGGs', None ]
    res = self.rsDB.addOrModifyResource( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )  
           
    res = self.rsDB.addOrModifyResource( *tuple( initArgs ) )
    self.assertEqual( res[ 'OK' ], False )   
    res = self.rsDB.addOrModifyResource( 'xyz.cern.ch', 'eGGs', 'Computing', 'LCG.CERN.ch', 'CERN-PROD' )
    self.assertEqual( res[ 'OK' ], False )   
    res = self.rsDB.addOrModifyResource( 'xyz.cern.ch', 'CE', 'eGGs', 'LCG.CERN.ch', 'CERN-PROD' )
    self.assertEqual( res[ 'OK' ], False )   
    res = self.rsDB.addOrModifyResource( 'xyz.cern.ch', 'CE', 'Computing', 'eGGs', 'CERN-PROD' )
    self.assertEqual( res[ 'OK' ], False )   
    res = self.rsDB.addOrModifyResource( 'xyz.cern.ch', 'CE', 'Computing', 'LCG.CERN.ch', 'eGGs' )
    self.assertEqual( res[ 'OK' ], False )   
    # This one looks correct, it is correct, but Sites table is empty !!
    res = self.rsDB.addOrModifyResource( 'xyz.cern.ch', 'CE', 'Computing', 'LCG.CERN.ch', 'CERN-PROD' )
    self.assertEqual( res[ 'OK' ], False )   
            
  def test_02_setResourceStatus( self ):
    '''
    setResourceStatus( self, resourceName, statusType, status, reason, dateCreated, 
                       dateEffective, dateEnd, lastCheckTime, tokenOwner,tokenExpiration ) 
    '''

    ins = inspect.getargspec( self.rsDB.setResourceStatus.f )   
    self.assertEqual( ins.args, [ 'self', 'resourceName', 'statusType', 'status', 
                                  'reason', 'dateCreated', 'dateEffective', 'dateEnd', 
                                  'lastCheckTime', 'tokenOwner', 'tokenExpiration' ] )
    self.assertEqual( ins.varargs,  None)
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )
    
    dNow = datetime.now()
        
    initArgs = [ None, None, None, None, None, None, None, None, None, None ]
    
    dSol = datetime( 9999, 12, 11, 10, 9, 8 )
    
    res = self.rsDB.setResourceStatus( *tuple( initArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    #Test first parameter ( siteName )
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'xyz.cern.ch'
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'xyz.cern.ch' ]
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = 'eGGs'
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'eGGs' ]
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'xyz.cern.ch', 'eGGs' ]
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ None ]
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'xyz.cern.ch', 'eGGs', None ]
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    #Test second parameter ( statusType )
    modArgs[ 1 ] = ''
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ '' ]
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ 'eGGs' ]
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ '', 'eGGs' ]
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ None ]
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ '', 'eGGs', None ]
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )

    #Test third parameter ( status )
    modArgs[ 2 ] = 'Active'
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ 'Active' ]
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = 'eGGs'
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ 'eGGs' ]
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ 'Active', 'eGGs' ]
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ None ]
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ 'Active', 'eGGs', None ]
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    #Test forth parameter ( reason )
    modArgs = initArgs[:]
    modArgs[ 3 ] = 'Init'
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 3 ] = [ 'Init' ]    
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 3 ] = 'eGGs'
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 3 ] = [ 'eGGs' ]
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 3 ] = [ 'Init', 'eGGs' ]
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 3 ] = [ None ]
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )         
    modArgs[ 3 ] = [ 'Init', 'eGGs', None ]
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )  

    #Test fifth parameter ( dateCreated )
    modArgs = initArgs[:]
    modArgs[ 4 ] = dSol
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 4 ] = [ dSol ]    
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )     
    modArgs[ 4 ] = dNow
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 4 ] = [ dNow ]
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 4 ] = [ dSol, dNow ]
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 4 ] = [ None ]
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 4 ] = [ dSol, dNow, None ]
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
            
    #Test sixth parameter ( dateEffective ) 
    modArgs = initArgs[:]
    modArgs[ 5 ] = dSol
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )    
    modArgs[ 5 ] = [ dSol ]    
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )         
    modArgs[ 5 ] = dNow
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )  
    modArgs[ 5 ] = [ dNow ]
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 5 ] = [ dSol, dNow ]
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )     
    modArgs[ 5 ] = [ None ]
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 5 ] = [ dSol, dNow, None ]
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )     

    #Test seventh parameter ( dateEnd )
    modArgs = initArgs[:]
    modArgs[ 6 ] = dSol
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )   
    modArgs[ 6 ] = [ dSol ]    
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )        
    modArgs[ 6 ] = dNow
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 6 ] = [ dNow ]
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 6 ] = [ dSol, dNow ]
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 6 ] = [ None ]
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 6 ] = [ dSol, dNow, None ]
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
            
    #Test eighth parameter ( lastCheckTime )
    modArgs = initArgs[:]
    modArgs[ 7 ] = dSol
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )   
    modArgs[ 7 ] = [ dSol ]    
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )        
    modArgs[ 7 ] = dNow
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 7 ] = [ dNow ]
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 7 ] = [ dSol, dNow ]
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 7 ] = [ None ]
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 7 ] = [ dSol, dNow, None ]
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    #Test ninth parameter ( tokenOwner )
    modArgs = initArgs[:]
    modArgs[ 8 ] = 'RS_SVC'
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 8 ] = [ 'RS_SVC' ]   
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )   
    modArgs[ 8 ] = 'eGGs'   
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 8 ] = [ 'eGGs' ]   
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs' ]   
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 8 ] = [ None ]   
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs', None ]   
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    #Test tenth parameter ( tokenExpiration )
    modArgs = initArgs[:]
    modArgs[ 9 ] = dSol
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )   
    modArgs[ 9 ] = [ dSol ]    
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )        
    modArgs[ 9 ] = dNow
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 9 ] = [ dNow ]
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 9 ] = [ dSol, dNow ]
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 9 ] = [ None ]
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )    
    modArgs[ 9 ] = [ dSol, dNow, None ]
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'xyz.cern.ch'
    modArgs[ 1 ] = ''
    modArgs[ 2 ] = 'Active'    
    modArgs[ 3 ] = 'Reason'
    modArgs[ 8 ] = 'token'
    
    #This should work, but the DB is empty
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    modArgs[ 0 ] =  [ 'xyz.cern.ch' ]
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] =  'xyz.cern.ch'
    modArgs[ 1 ] =  ''
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = ''
    modArgs[ 2 ] = [ 'Active' ]
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = 'Active'
    modArgs[ 4 ] = 'eGGs'
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 4 ] = None
    modArgs[ 5 ] = 'eGGs'
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 5 ] = None
    modArgs[ 6 ] = 'eGGs'
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 6 ] = None
    modArgs[ 7 ] = 'eGGs'
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 7 ] = None
    modArgs[ 9 ] = 'eGGs'
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    now = datetime.now()
    modArgs = [ 'eGGs', '', 'Active', None, now, now, now, now, None, now]
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = 'xyz.cern.ch'
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = ''
    modArgs[ 2 ] = 'eGGs'
    res = self.rsDB.setResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
  def test_03_setResourceScheduledStatus( self ):
    '''
    setResourceScheduledStatus( self, resourceName, statusType, status, reason, dateCreated, 
                               dateEffective, dateEnd, lastCheckTime, tokenOwner, 
                               tokenExpiration )
    '''   
        
    ins = inspect.getargspec( self.rsDB.setResourceScheduledStatus.f )   
    self.assertEqual( ins.args, [ 'self', 'resourceName', 'statusType', 'status', 'reason', 'dateCreated', 
                                  'dateEffective', 'dateEnd', 'lastCheckTime', 'tokenOwner', 
                                  'tokenExpiration' ] )
    self.assertEqual( ins.varargs,  None)
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )
    
    dNow = datetime.now()
        
    initArgs = [ None, None, None, None, None, None, None, None, None, None ]
    
    dSol = datetime( 9999, 12, 11, 10, 9, 8 )
    
    res = self.rsDB.setResourceScheduledStatus( *tuple( initArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    #Test first parameter ( siteName )
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'xyz.cern.ch'
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'xyz.cern.ch' ]
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = 'eGGs'
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'eGGs' ]
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'xyz.cern.ch', 'eGGs' ]
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ None ]
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'xyz.cern.ch', 'eGGs', None ]
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    #Test second parameter ( statusType )
    modArgs[ 1 ] = ''
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ '' ]
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ 'eGGs' ]
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ '', 'eGGs' ]
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ None ]
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ '', 'eGGs', None ]
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )

    #Test third parameter ( status )
    modArgs[ 2 ] = 'Active'
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ 'Active' ]
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = 'eGGs'
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ 'eGGs' ]
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ 'Active', 'eGGs' ]
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ None ]
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ 'Active', 'eGGs', None ]
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    #Test forth parameter ( reason )
    modArgs = initArgs[:]
    modArgs[ 3 ] = 'Init'
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 3 ] = [ 'Init' ]    
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 3 ] = 'eGGs'
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 3 ] = [ 'eGGs' ]
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 3 ] = [ 'Init', 'eGGs' ]
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 3 ] = [ None ]
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )         
    modArgs[ 3 ] = [ 'Init', 'eGGs', None ]
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )  

    #Test fifth parameter ( dateCreated )
    modArgs = initArgs[:]
    modArgs[ 4 ] = dSol
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 4 ] = [ dSol ]    
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )     
    modArgs[ 4 ] = dNow
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 4 ] = [ dNow ]
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 4 ] = [ dSol, dNow ]
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 4 ] = [ None ]
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 4 ] = [ dSol, dNow, None ]
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
            
    #Test sixth parameter ( dateEffective ) 
    modArgs = initArgs[:]
    modArgs[ 5 ] = dSol
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )    
    modArgs[ 5 ] = [ dSol ]    
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )         
    modArgs[ 5 ] = dNow
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )  
    modArgs[ 5 ] = [ dNow ]
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 5 ] = [ dSol, dNow ]
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )     
    modArgs[ 5 ] = [ None ]
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 5 ] = [ dSol, dNow, None ]
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )     

    #Test seventh parameter ( dateEnd )
    modArgs = initArgs[:]
    modArgs[ 6 ] = dSol
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )   
    modArgs[ 6 ] = [ dSol ]    
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )        
    modArgs[ 6 ] = dNow
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 6 ] = [ dNow ]
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 6 ] = [ dSol, dNow ]
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 6 ] = [ None ]
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 6 ] = [ dSol, dNow, None ]
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
            
    #Test eighth parameter ( lastCheckTime )
    modArgs = initArgs[:]
    modArgs[ 7 ] = dSol
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )   
    modArgs[ 7 ] = [ dSol ]    
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )        
    modArgs[ 7 ] = dNow
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 7 ] = [ dNow ]
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 7 ] = [ dSol, dNow ]
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 7 ] = [ None ]
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 7 ] = [ dSol, dNow, None ]
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    #Test ninth parameter ( tokenOwner )
    modArgs = initArgs[:]
    modArgs[ 8 ] = 'RS_SVC'
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 8 ] = [ 'RS_SVC' ]   
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )   
    modArgs[ 8 ] = 'eGGs'   
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 8 ] = [ 'eGGs' ]   
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs' ]   
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 8 ] = [ None ]   
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs', None ]   
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    #Test tenth parameter ( tokenExpiration )
    modArgs = initArgs[:]
    modArgs[ 9 ] = dSol
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )   
    modArgs[ 9 ] = [ dSol ]    
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )        
    modArgs[ 9 ] = dNow
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 9 ] = [ dNow ]
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 9 ] = [ dSol, dNow ]
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 9 ] = [ None ]
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )    
    modArgs[ 9 ] = [ dSol, dNow, None ]
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'xyz.cern.ch'
    modArgs[ 1 ] = ''
    modArgs[ 2 ] = 'Active'    
    modArgs[ 3 ] = 'Reason'
    modArgs[ 8 ] = 'token'
    
    #This should work, but the DB is empty
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    modArgs[ 0 ] =  [ 'xyz.cern.ch' ]
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] =  'xyz.cern.ch'
    modArgs[ 1 ] =  ''
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = ''
    modArgs[ 2 ] = [ 'Active' ]
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = 'Active'
    modArgs[ 4 ] = 'eGGs'
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 4 ] = None
    modArgs[ 5 ] = 'eGGs'
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 5 ] = None
    modArgs[ 6 ] = 'eGGs'
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 6 ] = None
    modArgs[ 7 ] = 'eGGs'
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 7 ] = None
    modArgs[ 9 ] = 'eGGs'
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    now = datetime.now()
    modArgs = [ 'eGGs', '', 'Active', None, now, now, now, now, None, now]
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = 'xyz.cern.ch'
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = ''
    modArgs[ 2 ] = 'eGGs'
    res = self.rsDB.setResourceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
  def test_04_updateResourceStatus( self ):
    '''
    updateResourceStatus( self, resourceName, statusType, status, reason, dateCreated, 
                          dateEffective, dateEnd, lastCheckTime, tokenOwner, 
                          tokenExpiration )
    '''
    
    ins = inspect.getargspec( self.rsDB.updateResourceStatus.f )   
    self.assertEqual( ins.args, [ 'self', 'resourceName', 'statusType', 'status', 'reason', 'dateCreated', 
                                  'dateEffective', 'dateEnd', 'lastCheckTime', 'tokenOwner', 
                                  'tokenExpiration' ] )
    self.assertEqual( ins.varargs,  None)
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )
    
    dNow = datetime.now()
    
    initArgs = [ None, None, None, None, None, None, None, None, None, None ]
    
    dSol= datetime( 9999, 12, 11, 10, 9, 8 )
    
    res = self.rsDB.updateResourceStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    
    '''
      Param tests
    '''    
    #Test first parameter ( siteName )
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'xyz.cern.ch'
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'xyz.cern.ch' ]
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = 'eGGs'
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'eGGs' ]
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'xyz.cern.ch', 'eGGs' ]
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ None ]
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'xyz.cern.ch', 'eGGs', None ]
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    #Test second parameter ( statusType )
    modArgs[ 1 ] = ''
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ '' ]
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ 'eGGs' ]
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ '', 'eGGs' ]
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ None ]
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ '', 'eGGs', None ]
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )

    #Test third parameter ( status )
    modArgs[ 2 ] = 'Active'
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ 'Active' ]
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = 'eGGs'
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ 'eGGs' ]
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ 'Active', 'eGGs' ]
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ None ]
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ 'Active', 'eGGs', None ]
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    #Test forth parameter ( reason )
    modArgs = initArgs[:]
    modArgs[ 3 ] = 'Init'
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 3 ] = [ 'Init' ]    
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 3 ] = 'eGGs'
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 3 ] = [ 'eGGs' ]
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 3 ] = [ 'Init', 'eGGs' ]
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 3 ] = [ None ]
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )         
    modArgs[ 3 ] = [ 'Init', 'eGGs', None ]
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )  

    #Test fifth parameter ( dateCreated )
    modArgs = initArgs[:]
    modArgs[ 4 ] = dSol
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 4 ] = [ dSol ]    
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )     
    modArgs[ 4 ] = dNow
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 4 ] = [ dNow ]
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 4 ] = [ dSol, dNow ]
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 4 ] = [ None ]
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 4 ] = [ dSol, dNow, None ]
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
            
    #Test sixth parameter ( dateEffective ) 
    modArgs = initArgs[:]
    modArgs[ 5 ] = dSol
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )    
    modArgs[ 5 ] = [ dSol ]    
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )         
    modArgs[ 5 ] = dNow
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )  
    modArgs[ 5 ] = [ dNow ]
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 5 ] = [ dSol, dNow ]
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )     
    modArgs[ 5 ] = [ None ]
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 5 ] = [ dSol, dNow, None ]
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )     

    #Test seventh parameter ( dateEnd )
    modArgs = initArgs[:]
    modArgs[ 6 ] = dSol
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )   
    modArgs[ 6 ] = [ dSol ]    
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )        
    modArgs[ 6 ] = dNow
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 6 ] = [ dNow ]
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 6 ] = [ dSol, dNow ]
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 6 ] = [ None ]
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 6 ] = [ dSol, dNow, None ]
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
            
    #Test eighth parameter ( lastCheckTime )
    modArgs = initArgs[:]
    modArgs[ 7 ] = dSol
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )   
    modArgs[ 7 ] = [ dSol ]    
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )        
    modArgs[ 7 ] = dNow
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 7 ] = [ dNow ]
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 7 ] = [ dSol, dNow ]
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 7 ] = [ None ]
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 7 ] = [ dSol, dNow, None ]
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    #Test ninth parameter ( tokenOwner )
    modArgs = initArgs[:]
    modArgs[ 8 ] = 'RS_SVC'
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 8 ] = [ 'RS_SVC' ]   
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )  
    modArgs[ 8 ] = 'eGGs'   
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 8 ] = [ 'eGGs' ]   
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs' ]   
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 8 ] = [ None ]   
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs', None ]   
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    #Test tenth parameter ( tokenExpiration )
    modArgs = initArgs[:]
    modArgs[ 9 ] = dSol
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )   
    modArgs[ 9 ] = [ dSol ]    
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )        
    modArgs[ 9 ] = dNow
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 9 ] = [ dNow ]
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 9 ] = [ dSol, dNow ]
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 9 ] = [ None ]
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )    
    modArgs[ 9 ] = [ dSol, dNow, None ]
    res = self.rsDB.updateResourceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
       
    
    res = self.rsDB.updateResourceStatus( 'xyz.cern.ch', '', 'Active', None, None,
                                       None,None,None,None,None )
    self.assertEqual( res[ 'OK' ], False )
    
    res = self.rsDB.updateResourceStatus( ['xyz.cern.ch'], '', 'Active', None, None,
                                            None, None, None, None, None )
    self.assertEqual( res[ 'OK' ], False )
    res = self.rsDB.updateResourceStatus( 'xyz.cern.ch', [''], 'Active', None, None,
                                            None, None, None, None, None )
    self.assertEqual( res[ 'OK' ], False )
    res = self.rsDB.updateResourceStatus( 'xyz.cern.ch', '', ['Active'], None, None,
                                            None, None, None, None, None )
    self.assertEqual( res[ 'OK' ], False )
    res = self.rsDB.updateResourceStatus( 'xyz.cern.ch', '', 'Active', None, 'eGGs',
                                            None, None, None, None, None )
    self.assertEqual( res[ 'OK' ], False )
    res = self.rsDB.updateResourceStatus( 'xyz.cern.ch', '', 'Active', None, None,
                                            'eGGs', None, None, None, None )
    self.assertEqual( res[ 'OK' ], False )
    res = self.rsDB.updateResourceStatus( 'xyz.cern.ch', '', 'Active', None, None,
                                            None, 'eGGs', None, None, None )
    self.assertEqual( res[ 'OK' ], False )
    res = self.rsDB.updateResourceStatus( 'xyz.cern.ch', '', 'Active', None, None,
                                            None, None, 'eGGs', None, None )
    self.assertEqual( res[ 'OK' ], False )
    res = self.rsDB.updateResourceStatus( 'xyz.cern.ch', '', 'Active', None, None,
                                            None, None, None, None, 'eGGs' )
    self.assertEqual( res[ 'OK' ], False )
    
    now = datetime.now()
    
    #Our DB is empty, some validations must break !
    res = self.rsDB.updateResourceStatus( 'xyz.cern.ch', '', 'Active', None, now,
                                            now, now, now, None, now )
    self.assertEqual( res[ 'OK' ], False )
    res = self.rsDB.updateResourceStatus( 'eGGs', '', 'Active', None, now,
                                            now, now, now, None, now )
    self.assertEqual( res[ 'OK' ], False )
    res = self.rsDB.updateResourceStatus( 'xyz.cern.ch', 'eGGs', 'Active', None, now,
                                            now, now, now, None, now )
    self.assertEqual( res[ 'OK' ], False )
    res = self.rsDB.updateResourceStatus( 'xyz.cern.ch', '', 'eGGs', None, now,
                                       now, now, now, None, now )
    self.assertEqual( res[ 'OK' ], False )
        
  def test_05_getResources( self ):
    '''
    getResources( self, resourceName, resourceType, serviceType, siteName, 
                    gridSiteName, **kwargs )
    '''
    ins = inspect.getargspec( self.rsDB.getResources.f )   
    self.assertEqual( ins.args, [ 'self', 'resourceName', 'resourceType', 'serviceType', 
                                  'siteName', 'gridSiteName' ] )
    self.assertEqual( ins.varargs,  None)
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, None )
    
    initArgs = [ None, None, None, None, None ]
    
    res = self.rsDB.getResources( *tuple( initArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    
    '''
      Param tests
    '''    
    #Test first param ( serviceName )
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'Computing@LCG.CERN.ch'
    res = self.rsDB.getResources( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 0 ] = [ 'Computing@LCG.CNAF.it' ]
    res = self.rsDB.getResources( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = [ 'Computing@LCG.CERN.ch', 'Computing@LCG.CNAF.it' ]
    res = self.rsDB.getResources( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = 'eGGs'
    res = self.rsDB.getResources( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'eGGs' ]
    res = self.rsDB.getResources( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'Computing@LCG.CERN.ch', 'Computing@LCG.CNAF.it', 'eGGs' ]
    res = self.rsDB.getResources( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ None ]
    res = self.rsDB.getResources( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'Computing@LCG.CERN.ch', 'Computing@LCG.CNAF.it', 'eGGs', None ]
    res = self.rsDB.getResources( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 

    modArgs = initArgs[:]
    modArgs[ 1 ] = 'CE'
    res = self.rsDB.getResources( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ 'CE' ]
    res = self.rsDB.getResources( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ 'CE', 'CREAMCE' ]
    res = self.rsDB.getResources( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.getResources( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ 'eGGs' ]
    res = self.rsDB.getResources( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ 'CE', 'CREAMCE', 'eGGs' ]
    res = self.rsDB.getResources( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ None ]
    res = self.rsDB.getResources( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ 'CE', 'CREAMCE', 'eGGs', None ]
    res = self.rsDB.getResources( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )

    #Test second param ( serviceType )
    modArgs = initArgs[:]
    modArgs[ 2 ] = 'Computing'
    res = self.rsDB.getResources( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = [ 'Computing' ]
    res = self.rsDB.getResources( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = [ 'Storage', 'Computing' ]
    res = self.rsDB.getResources( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = 'eGGs'
    res = self.rsDB.getResources( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = [ 'eGGs' ]
    res = self.rsDB.getResources( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = [ 'Storage', 'Computing', 'eGGs' ]
    res = self.rsDB.getResources( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = [ None ]
    res = self.rsDB.getResources( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = [ 'Storage', 'Computing', 'eGGs', None ]
    res = self.rsDB.getResources( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )

    #Test third param ( siteName )
    modArgs = initArgs[:]
    modArgs[ 3 ] = 'LCG.CERN.ch'
    res = self.rsDB.getResources( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 3 ] = [ 'LCG.CERN.ch' ]
    res = self.rsDB.getResources( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 3 ] = [ 'LCG.CERN.ch', 'LCG.CNAF.it' ]
    res = self.rsDB.getResources( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 3 ] = 'eGGs'
    res = self.rsDB.getResources( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 3 ] = [ 'eGGs' ]
    res = self.rsDB.getResources( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 3 ] = [ 'LCG.CERN.ch', 'LCG.CNAF.it', 'eGGs' ]
    res = self.rsDB.getResources( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 3 ] = [ None ]
    res = self.rsDB.getResources( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 3 ] = [ 'LCG.CERN.ch', 'LCG.CNAF.it', 'eGGs', None ]
    res = self.rsDB.getResources( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    
    #Test third param ( siteName )
    modArgs = initArgs[:]
    modArgs[ 3 ] = 'CERN-PROD'
    res = self.rsDB.getResources( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 3 ] = [ 'CERN-PROD' ]
    res = self.rsDB.getResources( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 3 ] = [ 'CERN-PROD', 'INFN-T1' ]
    res = self.rsDB.getResources( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 3 ] = 'eGGs'
    res = self.rsDB.getResources( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 3 ] = [ 'eGGs' ]
    res = self.rsDB.getResources( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 3 ] = [ 'CERN-PROD', 'INFN-T1', 'eGGs' ]
    res = self.rsDB.getResources( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 3 ] = [ None ]
    res = self.rsDB.getResources( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 3 ] = [ 'CERN-PROD', 'INFN-T1', 'eGGs', None ]
    res = self.rsDB.getResources( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
  
  def test_06_getResourcesStatus( self ):
    '''
    getResourcesStatus( self, resourceName, statusType, status, reason, dateCreated, 
                        dateEffective, dateEnd, lastCheckTime, tokenOwner, 
                        tokenExpiration, **kwargs )
    '''
    dNow = datetime.now()
    
    ins = inspect.getargspec( self.rsDB.getResourcesStatus.f )   
    self.assertEqual( ins.args, [ 'self', 'resourceName', 'statusType', 'status', 
                                  'reason', 'dateCreated', 'dateEffective', 'dateEnd', 
                                  'lastCheckTime', 'tokenOwner', 'tokenExpiration' ] )
    self.assertEqual( ins.varargs,  None)
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, None )
    
    initArgs = [ None,None,None,None,None,None,None,None,None,None ]
    dSol     = datetime(9999, 12, 11, 10, 9, 8)
        
    res = self.rsDB.getResourcesStatus( *tuple( initArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    
    '''
      Param tests
    '''    
    #Test first param ( siteName )
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'xyz.cern.ch'
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'xyz.cern.ch' ]    
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 0 ] = [ 'xyz.cern.ch', 'abc.cern.ch' ]
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = 'LCG.eGGs.xy'
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'LCG.eGGs.xy' ]
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'xyz.cern.ch', 'LCG.eGGs.xy' ]
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ None ]
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'xyz.cern.ch', 'LCG.eGGs.xy', None ]
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )

    #Test second param ( statusType ) 
    modArgs = initArgs[:]
    modArgs[ 1 ] = ''
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ '' ]    
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ 'eGGs' ]
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ '', 'eGGs' ]
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ None ]
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ '', 'eGGs', None ]
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )

    #Test third parameter ( status )
    modArgs = initArgs[:]
    modArgs[ 2 ] = 'Active'
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = [ 'Active' ]    
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    modArgs[ 2 ] = 'Banned'
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 2 ] = [ 'Banned' ]
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = [ 'Active', 'Banned' ]
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = 'eGGs'
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = [ 'eGGs' ]
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )    
    modArgs[ 2 ] = [ 'eGGs', 'Banned' ]
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 2 ] = [ None ]
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )   
    modArgs[ 2 ] = [ 'eGGs', 'Banned', None ]
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )

    #Test forth parameter ( reason )
    modArgs = initArgs[:]
    modArgs[ 3 ] = 'Init'
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 3 ] = [ 'Init' ]    
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 3 ] = 'eGGs'
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 3 ] = [ 'eGGs' ]
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 3 ] = [ 'Init', 'eGGs' ]
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )         
    modArgs[ 3 ] = [ None ]
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 3 ] = [ 'Init', 'eGGs', None ]
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )

    #Test fifth parameter ( dateCreated )
    modArgs = initArgs[:]
    modArgs[ 4 ] = dSol
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 4 ] = [ dSol ]    
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 4 ] = dNow
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 4 ] = [ dNow ]
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 4 ] = [ dSol, dNow ]
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )    
    modArgs[ 4 ] = [ None ]
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 4 ] = [ dSol, dNow, None ]
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
            
    #Test sixth parameter ( dateEffective ) 
    modArgs = initArgs[:]
    modArgs[ 5 ] = dSol
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 5 ] = [ dSol ]    
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    modArgs[ 5 ] = dNow
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 5 ] = [ dNow ]
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 5 ] = [ dSol, dNow ]
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )   
    modArgs[ 5 ] = [ None ]
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 5 ] = [ dSol, dNow, None ]
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )

    #Test seventh parameter ( dateEnd )
    modArgs = initArgs[:]
    modArgs[ 6 ] = dSol
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 6 ] = [ dSol ]    
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )      
    modArgs[ 6 ] = dNow
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 6 ] = [ dNow ]
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 6 ] = [ dSol, dNow ]
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )   
    modArgs[ 6 ] = [ None ]
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 6 ] = [ dSol, dNow, None ]
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
                
    #Test eighth parameter ( lastCheckTime )
    modArgs = initArgs[:]
    modArgs[ 7 ] = dSol
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 7 ] = [ dSol ]    
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    modArgs[ 7 ] = dNow
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 7 ] = [ dNow ]
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 7 ] = [ dSol, dNow ]
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )    
    modArgs[ 7 ] = [ None ]
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 7 ] = [ dSol, dNow, None ]
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
        
    #Test ninth parameter ( tokenOwner )
    modArgs = initArgs[:]
    modArgs[ 8 ] = 'RS_SVC'
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'RS_SVC' ]   
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 8 ] = 'eGGs'   
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'eGGs' ]   
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs' ]   
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ None ]   
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs', None ]   
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    
    #Test tenth parameter ( tokenExpiration )
    modArgs = initArgs[:]
    modArgs[ 9 ] = dSol
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 9 ] = [ dSol ]    
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )      
    modArgs[ 9 ] = dNow
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 9 ] = [ dNow ]
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 9 ] = [ dSol, dNow ]
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 9 ] = [ None ]
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 9 ] = [ dSol, dNow, None ]
    res = self.rsDB.getResourcesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
            
             
  def test_07_getResourcesHistory( self ):
    '''
    getResourcesHistory( self, resourceName, statusType, status, reason, dateCreated, 
                         dateEffective, dateEnd, lastCheckTime, tokenOwner, 
                         tokenExpiration, **kwargs )
    '''
    dNow = datetime.now()
    
    ins = inspect.getargspec( self.rsDB.getResourcesHistory.f )   
    self.assertEqual( ins.args, [ 'self', 'resourceName', 'statusType', 'status', 
                                  'reason', 'dateCreated', 'dateEffective', 'dateEnd', 
                                  'lastCheckTime', 'tokenOwner', 'tokenExpiration' ] )
    self.assertEqual( ins.varargs,  None)
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, None )
    
    initArgs = [ None,None,None,None,None,None,None,None,None,None ]
    dSol     = datetime(9999, 12, 11, 10, 9, 8)
        
    res = self.rsDB.getResourcesHistory( *tuple( initArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    
    '''
      Param tests
    '''    
    #Test first param ( siteName )
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'xyz.cern.ch'
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = [ 'xyz.cern.ch' ]    
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )          
    modArgs[ 0 ] = [ 'xyz.cern.ch', 'abc.cern.ch' ]
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = 'LCG.eGGs.xy'
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = [ 'LCG.eGGs.xy' ]
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = [ 'xyz.cern.ch', 'LCG.eGGs.xy' ]
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = [ None ]
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = [ 'xyz.cern.ch', 'LCG.eGGs.xy', None ]
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  

    #Test second param ( statusType ) 
    modArgs = initArgs[:]
    modArgs[ 1 ] = ''
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 1 ] = [ '' ]    
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 1 ] = [ 'eGGs' ]
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 1 ] = [ '', 'eGGs' ]
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 1 ] = [ None ]
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 1 ] = [ '', 'eGGs', None ]
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  

    #Test third parameter ( status )
    modArgs = initArgs[:]
    modArgs[ 2 ] = 'Active'
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 2 ] = [ 'Active' ]    
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    modArgs[ 2 ] = 'Banned'
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 2 ] = [ 'Banned' ]
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 2 ] = [ 'Active', 'Banned' ]
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 2 ] = 'eGGs'
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 2 ] = [ 'eGGs' ]
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )     
    modArgs[ 2 ] = [ 'eGGs', 'Banned' ]
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )         
    modArgs[ 2 ] = [ None ]
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    modArgs[ 2 ] = [ 'eGGs', 'Banned', None ]
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  

    #Test forth parameter ( reason )
    modArgs = initArgs[:]
    modArgs[ 3 ] = 'Init'
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 3 ] = [ 'Init' ]    
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )         
    modArgs[ 3 ] = 'eGGs'
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 3 ] = [ 'eGGs' ]
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 3 ] = [ 'Init', 'eGGs' ]
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )            
    modArgs[ 3 ] = [ None ]
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 3 ] = [ 'Init', 'eGGs', None ]
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  

    #Test fifth parameter ( dateCreated )
    modArgs = initArgs[:]
    modArgs[ 4 ] = dSol
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )     
    modArgs[ 4 ] = [ dSol ]    
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 4 ] = dNow
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 4 ] = [ dNow ]
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 4 ] = [ dSol, dNow ]
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )    
    modArgs[ 4 ] = [ None ]
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )  
    modArgs[ 4 ] = [ dSol, dNow, None ]
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )  
            
    #Test sixth parameter ( dateEffective ) 
    modArgs = initArgs[:]
    modArgs[ 5 ] = dSol
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 5 ] = [ dSol ]    
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )      
    modArgs[ 5 ] = dNow
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 5 ] = [ dNow ]
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 5 ] = [ dSol, dNow ]
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )    
    modArgs[ 5 ] = [ None ]
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 5 ] = [ dSol, dNow, None ]
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )

    #Test seventh parameter ( dateEnd )
    modArgs = initArgs[:]
    modArgs[ 6 ] = dSol
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 6 ] = [ dSol ]    
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 6 ] = dNow
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 6 ] = [ dNow ]
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 6 ] = [ dSol, dNow ]
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )     
    modArgs[ 6 ] = [ None ]
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False ) 
    modArgs[ 6 ] = [ dSol, dNow, None ]
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False ) 
                
    #Test eighth parameter ( lastCheckTime )
    modArgs = initArgs[:]
    modArgs[ 7 ] = dSol
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )   
    modArgs[ 7 ] = [ dSol ]    
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    modArgs[ 7 ] = dNow
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 7 ] = [ dNow ]
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 7 ] = [ dSol, dNow ]
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 7 ] = [ None ]
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 7 ] = [ dSol, dNow, None ]
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
        
    #Test ninth parameter ( tokenOwner )
    modArgs = initArgs[:]
    modArgs[ 8 ] = 'RS_SVC'
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'RS_SVC' ]   
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = 'eGGs'   
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'eGGs' ]   
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs' ]   
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ None ]   
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs', None ]   
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    
    #Test tenth parameter ( tokenExpiration )
    modArgs = initArgs[:]
    modArgs[ 9 ] = dSol
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 9 ] = [ dSol ]    
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )      
    modArgs[ 9 ] = dNow
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 9 ] = [ dNow ]
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 9 ] = [ dSol, dNow ]
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )    
    modArgs[ 9 ] = [ None ]
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 9 ] = [ dSol, dNow, None ]
    res = self.rsDB.getResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )

  def test_08_getResourcesScheduledStatus( self ):
    '''
    getResourcesScheduledStatus( self, resourceName, statusType, status, reason, dateCreated, 
                                 dateEffective, dateEnd, lastCheckTime, tokenOwner, 
                                 tokenExpiration, **kwargs )
    '''
    dNow = datetime.now()
    
    ins = inspect.getargspec( self.rsDB.getResourcesScheduledStatus.f )   
    self.assertEqual( ins.args, [ 'self', 'resourceName', 'statusType', 'status', 
                                  'reason', 'dateCreated', 'dateEffective', 'dateEnd', 
                                  'lastCheckTime', 'tokenOwner', 
                                  'tokenExpiration' ] )
    self.assertEqual( ins.varargs,  None)
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, None )
    
    initArgs = [ None,None,None,None,None,None,None,None,None,None ]
    dSol     = datetime(9999, 12, 11, 10, 9, 8)
        
    res = self.rsDB.getResourcesScheduledStatus( *tuple( initArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    
    '''
      Param tests
    '''    
    #Test first param ( siteName )
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'xyz.cern.ch'
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = [ 'xyz.cern.ch' ]    
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )          
    modArgs[ 0 ] = [ 'xyz.cern.ch', 'abc.cern.ch' ]
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = 'LCG.eGGs.xy'
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = [ 'LCG.eGGs.xy' ]
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = [ 'xyz.cern.ch', 'LCG.eGGs.xy' ]
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = [ None ]
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = [ 'xyz.cern.ch', 'LCG.eGGs.xy', None ]
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  

    #Test second param ( statusType ) 
    modArgs = initArgs[:]
    modArgs[ 1 ] = ''
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 1 ] = [ '' ]    
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 1 ] = [ 'eGGs' ]
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 1 ] = [ '', 'eGGs' ]
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 1 ] = [ None ]
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 1 ] = [ '', 'eGGs', None ]
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  

    #Test third parameter ( status )
    modArgs = initArgs[:]
    modArgs[ 2 ] = 'Active'
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 2 ] = [ 'Active' ]    
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    modArgs[ 2 ] = 'Banned'
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 2 ] = [ 'Banned' ]
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 2 ] = [ 'Active', 'Banned' ]
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 2 ] = 'eGGs'
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 2 ] = [ 'eGGs' ]
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )     
    modArgs[ 2 ] = [ 'eGGs', 'Banned' ]
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )         
    modArgs[ 2 ] = [ None ]
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    modArgs[ 2 ] = [ 'eGGs', 'Banned', None ]
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  

    #Test forth parameter ( reason )
    modArgs = initArgs[:]
    modArgs[ 3 ] = 'Init'
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 3 ] = [ 'Init' ]    
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )         
    modArgs[ 3 ] = 'eGGs'
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 3 ] = [ 'eGGs' ]
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 3 ] = [ 'Init', 'eGGs' ]
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )            
    modArgs[ 3 ] = [ None ]
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 3 ] = [ 'Init', 'eGGs', None ]
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  

    #Test fifth parameter ( dateCreated )
    modArgs = initArgs[:]
    modArgs[ 4 ] = dSol
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )     
    modArgs[ 4 ] = [ dSol ]    
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 4 ] = dNow
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 4 ] = [ dNow ]
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 4 ] = [ dSol, dNow ]
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )    
    modArgs[ 4 ] = [ None ]
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )  
    modArgs[ 4 ] = [ dSol, dNow, None ]
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )  
            
    #Test sixth parameter ( dateEffective ) 
    modArgs = initArgs[:]
    modArgs[ 5 ] = dSol
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 5 ] = [ dSol ]    
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )      
    modArgs[ 5 ] = dNow
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 5 ] = [ dNow ]
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 5 ] = [ dSol, dNow ]
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )    
    modArgs[ 5 ] = [ None ]
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 5 ] = [ dSol, dNow, None ]
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )

    #Test seventh parameter ( dateEnd )
    modArgs = initArgs[:]
    modArgs[ 6 ] = dSol
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 6 ] = [ dSol ]    
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 6 ] = dNow
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 6 ] = [ dNow ]
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 6 ] = [ dSol, dNow ]
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )     
    modArgs[ 6 ] = [ None ]
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False ) 
    modArgs[ 6 ] = [ dSol, dNow, None ]
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False ) 
                
    #Test eighth parameter ( lastCheckTime )
    modArgs = initArgs[:]
    modArgs[ 7 ] = dSol
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )   
    modArgs[ 7 ] = [ dSol ]    
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    modArgs[ 7 ] = dNow
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 7 ] = [ dNow ]
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 7 ] = [ dSol, dNow ]
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 7 ] = [ None ]
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 7 ] = [ dSol, dNow, None ]
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
        
    #Test ninth parameter ( tokenOwner )
    modArgs = initArgs[:]
    modArgs[ 8 ] = 'RS_SVC'
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'RS_SVC' ]   
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = 'eGGs'   
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'eGGs' ]   
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs' ]   
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ None ]   
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs', None ]   
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    
    #Test tenth parameter ( tokenExpiration )
    modArgs = initArgs[:]
    modArgs[ 9 ] = dSol
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 9 ] = [ dSol ]    
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )      
    modArgs[ 9 ] = dNow
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 9 ] = [ dNow ]
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 9 ] = [ dSol, dNow ]
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )    
    modArgs[ 9 ] = [ None ]
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 9 ] = [ dSol, dNow, None ]
    res = self.rsDB.getResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )

  def test_09_getResourcesPresent( self ):
    '''
    def getResourcesPresent( self, resourceName, siteName, serviceType, gridSiteName, 
                           siteType, resourceType, statusType, status, dateEffective, 
                           reason, lastCheckTime, tokenOwner, tokenExpiration, 
                           formerStatus, **kwargs )                 
    '''
    dNow = datetime.now()
    
    ins = inspect.getargspec( self.rsDB.getResourcesPresent.f )   
    self.assertEqual( ins.args, [ 'self', 'resourceName', 'siteName', 'serviceType', 'gridSiteName',
                                  'siteType', 'resourceType', 'statusType', 'status', 'dateEffective', 
                                  'reason', 'lastCheckTime', 'tokenOwner', 'tokenExpiration', 
                                  'formerStatus' ] )
    self.assertEqual( ins.varargs,  None)
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, None )

    
    initArgs = [ None,None,None,None,None,None,None,None,None,None, None, None, None, None ]
    dSol     = datetime(9999, 12, 11, 10, 9, 8)

    res = self.rsDB.getResourcesPresent( *tuple( initArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    
    '''
      Param tests
    '''    
    #Test first param ( siteName )
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'xyz.cern.ch'
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = [ 'xyz.cern.ch' ]    
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )          
    modArgs[ 0 ] = [ 'xyz.cern.ch', 'abc.cern.ch' ]
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = 'LCG.eGGs.xy'
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = [ 'LCG.eGGs.xy' ]
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = [ 'xyz.cern.ch', 'LCG.eGGs.xy' ]
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = [ None ]
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = [ 'xyz.cern.ch', 'LCG.eGGs.xy', None ]
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  

    #Test second param ( siteType )
    modArgs = initArgs[:]
    modArgs[ 1 ] = 'LCG.CERN.ch'
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 1 ] = [ 'LCG.CERN.ch' ]    
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )          
    modArgs[ 1 ] = [ 'LCG.CERN.ch', 'LCG.CNAF.it' ]
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 1 ] = 'LCG.eGGs.xy'
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 1 ] = [ 'LCG.eGGs.xy' ]
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 1 ] = [ 'LCG.CERN.ch', 'LCG.eGGs.xy' ]
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 1 ] = [ None ]
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 1 ] = [ 'LCG.CERN.ch', 'LCG.eGGs.xy', None ]
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  

    #Test second param ( siteType )
    modArgs = initArgs[:]
    modArgs[ 2 ] = 'Computing'
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 2 ] = [ 'Computing' ]    
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )          
    modArgs[ 2 ] = [ 'Computing', 'Storage' ]
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 2 ] = 'eGGs'
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 2 ] = [ 'eGGs' ]
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 2 ] = [ 'Computing', 'eGGs' ]
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 2 ] = [ None ]
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 2 ] = [ 'Computing', 'eGGs', None ]
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  

    #Test second param ( siteType )
    modArgs = initArgs[:]
    modArgs[ 3 ] = 'CERN-PROD'
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 3 ] = [ 'CERN-PROD' ]    
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )          
    modArgs[ 3 ] = [ 'CERN-PROD', 'INFN-T1' ]
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 3 ] = 'eGGs'
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 3 ] = [ 'eGGs' ]
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 3 ] = [ 'CERN-PROD', 'eGGs' ]
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 3 ] = [ None ]
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 3 ] = [ 'CERN-PROD', 'eGGs', None ]
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  

    #Test third param ( gridSiteName )
    modArgs = initArgs[:]
    modArgs[ 4 ] = 'T0'
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 4 ] = [ 'T0' ]    
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )          
    modArgs[ 4 ] = [ 'T0', 'T1' ]
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 4 ] = 'eGGs'
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 4 ] = [ 'eGGs' ]
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 4 ] = [ 'T0', 'eGGs' ]
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 4 ] = [ None ]
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 4 ] = [ 'T0', 'eGGs', None ]
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  

    #Test forth param ( gridTier )
    modArgs = initArgs[:]
    modArgs[ 5 ] = 'CE'
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 5 ] = [ 'CE' ]    
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )          
    modArgs[ 5 ] = [ 'CE', 'CREAMCE' ]
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 5 ] = 'eGGs'
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 5 ] = [ 'eGGs' ]
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 5 ] = [ 'CE', 'eGGs' ]
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 5 ] = [ None ]
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 5 ] = [ 'CE', 'eGGs', None ]
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  

    #Test fifth param ( statusType ) 
    modArgs = initArgs[:]
    modArgs[ 6 ] = ''
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 6 ] = [ '' ]    
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 6 ] = 'eGGs'
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 6 ] = [ 'eGGs' ]
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 6 ] = [ '', 'eGGs' ]
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 6 ] = [ None ]
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 6 ] = [ '', 'eGGs', None ]
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  

    #Test sixth parameter ( status )
    modArgs = initArgs[:]
    modArgs[ 7 ] = 'Active'
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 7 ] = [ 'Active' ]    
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    modArgs[ 7 ] = 'Banned'
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 7 ] = [ 'Banned' ]
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 7 ] = [ 'Active', 'Banned' ]
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 7 ] = 'eGGs'
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 7 ] = [ 'eGGs' ]
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )     
    modArgs[ 7 ] = [ 'eGGs', 'Banned' ]
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )         
    modArgs[ 7 ] = [ None ]
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    modArgs[ 7 ] = [ 'eGGs', 'Banned', None ]
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  

    #Test seventh parameter ( dateEffective ) 
    modArgs = initArgs[:]
    modArgs[ 8 ] = dSol
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ dSol ]    
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )      
    modArgs[ 8 ] = dNow
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ dNow ]
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ dSol, dNow ]
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )    
    modArgs[ 8 ] = [ None ]
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 8 ] = [ dSol, dNow, None ]
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )

    #Test eighth parameter ( reason )
    modArgs = initArgs[:]
    modArgs[ 9 ] = 'Init'
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 9 ] = [ 'Init' ]    
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )         
    modArgs[ 9 ] = 'eGGs'
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 9 ] = [ 'eGGs' ]
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 9 ] = [ 'Init', 'eGGs' ]
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )            
    modArgs[ 9 ] = [ None ]
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 9 ] = [ 'Init', 'eGGs', None ]
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
                
    #Test ninth parameter ( lastCheckTime )
    modArgs = initArgs[:]
    modArgs[ 10 ] = dSol
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )   
    modArgs[ 10 ] = [ dSol ]    
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    modArgs[ 10 ] = dNow
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 10 ] = [ dNow ]
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 10 ] = [ dSol, dNow ]
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 10 ] = [ None ]
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 10 ] = [ dSol, dNow, None ]
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
        
    #Test tenth parameter ( tokenOwner )
    modArgs = initArgs[:]
    modArgs[ 11 ] = 'RS_SVC'
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 11 ] = [ 'RS_SVC' ]   
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 11 ] = 'eGGs'   
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 11 ] = [ 'eGGs' ]   
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 11 ] = [ 'RS_SVC','eGGs' ]   
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 11 ] = [ None ]   
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 11 ] = [ 'RS_SVC','eGGs', None ]   
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    
    #Test eleventh parameter ( tokenExpiration )
    modArgs = initArgs[:]
    modArgs[ 12 ] = dSol
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 12 ] = [ dSol ]    
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )      
    modArgs[ 12 ] = dNow
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 12 ] = [ dNow ]
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 12 ] = [ dSol, dNow ]
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )    
    modArgs[ 12 ] = [ None ]
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 12 ] = [ dSol, dNow, None ]
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )

    #Test 12th parameter ( former status )
    modArgs = initArgs[:]
    modArgs[ 13 ] = 'Active'
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 13 ] = [ 'Active' ]    
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    modArgs[ 13 ] = 'Banned'
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 13 ] = [ 'Banned' ]
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 13 ] = [ 'Active', 'Banned' ]
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 13 ] = 'eGGs'
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 13 ] = [ 'eGGs' ]
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )     
    modArgs[ 13 ] = [ 'eGGs', 'Banned' ]
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )         
    modArgs[ 13 ] = [ None ]
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    modArgs[ 13 ] = [ 'eGGs', 'Banned', None ]
    res = self.rsDB.getResourcesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  

  def test_10_deleteResourcesScheduledStatus( self ):
    '''
    deleteResourcesScheduledStatus( self, resourceName, statusType, status, reason, 
                                    dateCreated, dateEffective, dateEnd, 
                                    lastCheckTime, tokenOwner, tokenExpiration, **kwargs)
    '''
    dNow = datetime.now()
    
    ins = inspect.getargspec( self.rsDB.deleteResourcesScheduledStatus.f )   
    self.assertEqual( ins.args, [ 'self', 'resourceName', 'statusType', 'status', 'reason', 
                                  'dateCreated', 'dateEffective', 'dateEnd', 
                                  'lastCheckTime', 'tokenOwner', 'tokenExpiration' ] )
    self.assertEqual( ins.varargs,  None)
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, None )        
       
    dSol     = datetime(9999, 12, 11, 10, 9, 8)      
    initArgs = [ None,None,None,None,None,None,None,None,None,None ]
   
       
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( initArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    
    '''
      Param tests
    '''    
    #Test first param ( siteName )
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'xyz.cern.ch'
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'xyz.cern.ch' ]    
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 0 ] = [ 'xyz.cern.ch', 'xyz.cern.ch' ]
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = 'eGGs'
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'eGGs' ]
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'xyz.cern.ch', 'eGGs' ]
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ None ]
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'xyz.cern.ch', 'eGGs', None ]
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )

    #Test second param ( statusType ) 
    modArgs = initArgs[:]
    modArgs[ 1 ] = ''
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ '' ]    
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 1 ] = [ 'eGGs' ]
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ '', 'eGGs' ]
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 1 ] = [ None ]
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ '', 'eGGs', None ]
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )

    #Test third parameter ( status )
    modArgs = initArgs[:]
    modArgs[ 2 ] = 'Active'
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = [ 'Active' ]    
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )         
    modArgs[ 2 ] = 'Banned'
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 2 ] = [ 'Banned' ]
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = [ 'Active', 'Banned' ]
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 2 ] = 'eGGs'
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = [ 'eGGs' ]
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )     
    modArgs[ 2 ] = [ 'eGGs', 'Banned' ]
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )      
    modArgs[ 2 ] = [ None ]
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 2 ] = [ 'eGGs', 'Banned', None ]
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )

    #Test forth parameter ( reason )
    modArgs = initArgs[:]
    modArgs[ 3 ] = 'Init'
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 3 ] = [ 'Init' ]    
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )     
    modArgs[ 3 ] = 'eGGs'
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 3 ] = [ 'eGGs' ]
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 3 ] = [ 'Init', 'eGGs' ]
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 3 ] = [ None ]
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 3 ] = [ 'Init', 'eGGs', None ]
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )

    #Test fifth parameter ( dateCreated )
    modArgs = initArgs[:]
    modArgs[ 4 ] = dSol
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 4 ] = [ dSol ]    
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    modArgs[ 4 ] = dNow
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 4 ] = [ dNow ]
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 4 ] = [ dSol, dNow ]
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )     
    modArgs[ 4 ] = [ None ]
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 4 ] = [ dSol, dNow, None ]
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
            
    #Test sixth parameter ( dateEffective ) 
    modArgs = initArgs[:]
    modArgs[ 5 ] = dSol
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 5 ] = [ dSol ]    
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 5 ] = dNow
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 5 ] = [ dNow ]
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 5 ] = [ dSol, dNow ]
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )   
    modArgs[ 5 ] = [ None ]
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 5 ] = [ dSol, dNow, None ]
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False ) 

    #Test seventh parameter ( dateEnd )
    modArgs = initArgs[:]
    modArgs[ 6 ] = dSol
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )    
    modArgs[ 6 ] = [ dSol ]    
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )      
    modArgs[ 6 ] = dNow
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 6 ] = [ dNow ]
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 6 ] = [ dSol, dNow ]
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )     
    modArgs[ 6 ] = [ None ]
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 6 ] = [ dSol, dNow, None ]
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
                
    #Test eighth parameter ( lastCheckTime )
    modArgs = initArgs[:]
    modArgs[ 7 ] = dSol
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )   
    modArgs[ 7 ] = [ dSol ]    
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 7 ] = dNow
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 7 ] = [ dNow ]
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 7 ] = [ dSol, dNow ]
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )   
    modArgs[ 7 ] = [ None ]
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 7 ] = [ dSol, dNow, None ]
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
        
    #Test ninth parameter ( tokenOwner )
    modArgs = initArgs[:]
    modArgs[ 8 ] = 'RS_SVC'
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'RS_SVC' ]   
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 8 ] = 'eGGs'   
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 8 ] = [ 'eGGs' ]   
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs' ]   
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ None ]   
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs', None ]   
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    
    #Test tenth parameter ( tokenExpiration )
    modArgs = initArgs[:]
    modArgs[ 9 ] = dSol
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )   
    modArgs[ 9 ] = [ dSol ]    
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )      
    modArgs[ 9 ] = dNow
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 9 ] = [ dNow ]
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 9 ] = [ dSol, dNow ]
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )   
    modArgs[ 9 ] = [ None ]
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 9 ] = [ dSol, dNow, None ]
    res = self.rsDB.deleteResourcesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )

  def test_11_deleteResourcesHistory( self ):
    '''
    deleteResourcesHistory( self, resourceName, statusType, status, reason, 
                            dateCreated, dateEffective, dateEnd, 
                            lastCheckTime, tokenOwner, tokenExpiration, kwargs )
    '''
    dNow = datetime.now()
    
    ins = inspect.getargspec( self.rsDB.deleteResourcesHistory.f )   
    self.assertEqual( ins.args, [ 'self', 'resourceName', 'statusType', 'status', 'reason', 
                                  'dateCreated', 'dateEffective', 'dateEnd', 
                                  'lastCheckTime', 'tokenOwner', 'tokenExpiration' ] )
    self.assertEqual( ins.varargs,  None)
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, None )        
       
    dSol     = datetime(9999, 12, 11, 10, 9, 8)      
    initArgs = [ None,None,None,None,None,None,None,None,None,None ]
   
       
    res = self.rsDB.deleteResourcesHistory( *tuple( initArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    
    '''
      Param tests
    '''    
    #Test first param ( siteName )
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'xyz.cern.ch'
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'xyz.cern.ch' ]    
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 0 ] = [ 'xyz.cern.ch', 'abc.cern.ch' ]
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = 'eGGs'
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'eGGs' ]
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'xyz.cern.ch', 'eGGs' ]
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ None ]
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'xyz.cern.ch', 'eGGs', None ]
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )

    #Test second param ( statusType ) 
    modArgs = initArgs[:]
    modArgs[ 1 ] = ''
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ '' ]    
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 1 ] = [ 'eGGs' ]
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ '', 'eGGs' ]
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 1 ] = [ None ]
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ '', 'eGGs', None ]
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )

    #Test third parameter ( status )
    modArgs = initArgs[:]
    modArgs[ 2 ] = 'Active'
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = [ 'Active' ]    
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )         
    modArgs[ 2 ] = 'Banned'
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 2 ] = [ 'Banned' ]
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = [ 'Active', 'Banned' ]
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 2 ] = 'eGGs'
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = [ 'eGGs' ]
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )     
    modArgs[ 2 ] = [ 'eGGs', 'Banned' ]
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )      
    modArgs[ 2 ] = [ None ]
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 2 ] = [ 'eGGs', 'Banned', None ]
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )

    #Test forth parameter ( reason )
    modArgs = initArgs[:]
    modArgs[ 3 ] = 'Init'
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 3 ] = [ 'Init' ]    
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )     
    modArgs[ 3 ] = 'eGGs'
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 3 ] = [ 'eGGs' ]
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 3 ] = [ 'Init', 'eGGs' ]
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 3 ] = [ None ]
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 3 ] = [ 'Init', 'eGGs', None ]
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )

    #Test fifth parameter ( dateCreated )
    modArgs = initArgs[:]
    modArgs[ 4 ] = dSol
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 4 ] = [ dSol ]    
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    modArgs[ 4 ] = dNow
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 4 ] = [ dNow ]
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 4 ] = [ dSol, dNow ]
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )     
    modArgs[ 4 ] = [ None ]
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 4 ] = [ dSol, dNow, None ]
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
            
    #Test sixth parameter ( dateEffective ) 
    modArgs = initArgs[:]
    modArgs[ 5 ] = dSol
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 5 ] = [ dSol ]    
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 5 ] = dNow
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 5 ] = [ dNow ]
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 5 ] = [ dSol, dNow ]
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )   
    modArgs[ 5 ] = [ None ]
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 5 ] = [ dSol, dNow, None ]
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False ) 

    #Test seventh parameter ( dateEnd )
    modArgs = initArgs[:]
    modArgs[ 6 ] = dSol
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )    
    modArgs[ 6 ] = [ dSol ]    
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )      
    modArgs[ 6 ] = dNow
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 6 ] = [ dNow ]
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 6 ] = [ dSol, dNow ]
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )     
    modArgs[ 6 ] = [ None ]
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 6 ] = [ dSol, dNow, None ]
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
                
    #Test eighth parameter ( lastCheckTime )
    modArgs = initArgs[:]
    modArgs[ 7 ] = dSol
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )   
    modArgs[ 7 ] = [ dSol ]    
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 7 ] = dNow
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 7 ] = [ dNow ]
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 7 ] = [ dSol, dNow ]
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )   
    modArgs[ 7 ] = [ None ]
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 7 ] = [ dSol, dNow, None ]
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
        
    #Test ninth parameter ( tokenOwner )
    modArgs = initArgs[:]
    modArgs[ 8 ] = 'RS_SVC'
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'RS_SVC' ]   
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 8 ] = 'eGGs'   
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 8 ] = [ 'eGGs' ]   
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs' ]   
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ None ]   
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs', None ]   
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    
    #Test tenth parameter ( tokenExpiration )
    modArgs = initArgs[:]
    modArgs[ 9 ] = dSol
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )   
    modArgs[ 9 ] = [ dSol ]    
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )      
    modArgs[ 9 ] = dNow
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 9 ] = [ dNow ]
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 9 ] = [ dSol, dNow ]
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )   
    modArgs[ 9 ] = [ None ]
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 9 ] = [ dSol, dNow, None ]
    res = self.rsDB.deleteResourcesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )

  def test_12_deleteResources( self ):
    '''
    deleteResources( self, resourceName )
    '''

    ins = inspect.getargspec( self.rsDB.deleteResources.f )   
    self.assertEqual( ins.args, [ 'self', 'resourceName' ] )
    self.assertEqual( ins.varargs,  None)
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )       

    res = self.rsDB.deleteResources()
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.deleteResources( 'eGGs', None )
    self.assertNotEqual( res.has_key( 'Value' ), True )

    #Test first param
    res = self.rsDB.deleteResources( 'xyz.cern.ch' )
    self.assertEquals( res[ 'OK' ], True )
    res = self.rsDB.deleteResources( [ 'xyz.cern.ch' ] )
    self.assertEquals( res[ 'OK' ], True )
    res = self.rsDB.deleteResources( 'eGGs' )
    self.assertEquals( res[ 'OK' ], True )
    res = self.rsDB.deleteResources( [ 'eGGs' ] )
    self.assertEquals( res[ 'OK' ], True )
    res = self.rsDB.deleteResources( None )
    self.assertEquals( res[ 'OK' ], False )
    res = self.rsDB.deleteResources( [ None ] )
    self.assertEquals( res[ 'OK' ], False )
    res = self.rsDB.deleteResources( [ 'xyz.cern.ch', None ] )
    self.assertEquals( res[ 'OK' ], False )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF