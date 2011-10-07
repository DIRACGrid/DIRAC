from DIRAC import S_OK
from DIRAC.ResourceStatusSystem.DB.test.TestSuite_ResourceStatusDB.TestCase_EmptyDB import TestCase_EmptyDB

from datetime import datetime
import inspect

class Test_EmptyDB_StorageElements( TestCase_EmptyDB ):

  def test_01_addOrModifyStorageElement( self ):
    '''
    addOrModifyStorageElement( self, storageElementName, resourceName, gridSiteName )
    '''
    
    ins = inspect.getargspec( self.rsDB.addOrModifyStorageElement.f )   
    self.assertEqual( ins.args, [ 'self', 'storageElementName', 'resourceName', 'gridSiteName' ] )
    self.assertEqual( ins.varargs,  None)
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )
    
    initArgs = [ None, None, None ]
    
    #Fails because second parameter does not validate
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'CERN-test'
    res = self.rsDB.addOrModifyStorageElement( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'CERN-test' ]
    res = self.rsDB.addOrModifyStorageElement( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = 'eGGs'
    res = self.rsDB.addOrModifyStorageElement( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'eGGs' ]
    res = self.rsDB.addOrModifyStorageElement( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'CERN-test', 'eGGs' ]
    res = self.rsDB.addOrModifyStorageElement( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ None ]
    res = self.rsDB.addOrModifyStorageElement( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'CERN-test', 'eGGs', None ]
    res = self.rsDB.addOrModifyStorageElement( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )

    #Fail because first parameter does not validate
    modArgs = initArgs[:]
    modArgs[ 1 ] = 'xyz.cern.ch'
    res = self.rsDB.addOrModifyStorageElement( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ 'xyz.cern.ch' ]
    res = self.rsDB.addOrModifyStorageElement( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.addOrModifyStorageElement( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ 'eGGs' ]
    res = self.rsDB.addOrModifyStorageElement( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ 'xyz.cern.ch', 'eGGs' ]
    res = self.rsDB.addOrModifyStorageElement( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ None ]
    res = self.rsDB.addOrModifyStorageElement( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ 'xyz.cern.ch', 'eGGs', None ]
    res = self.rsDB.addOrModifyStorageElement( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )    

    #Fail because first parameter does not validate
    modArgs = initArgs[:]
    modArgs[ 2 ] = 'CERN-PROD'
    res = self.rsDB.addOrModifyStorageElement( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ 'CERN-PROD' ]
    res = self.rsDB.addOrModifyStorageElement( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = 'eGGs'
    res = self.rsDB.addOrModifyStorageElement( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ 'eGGs' ]
    res = self.rsDB.addOrModifyStorageElement( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ 'CERN-PROD', 'eGGs' ]
    res = self.rsDB.addOrModifyStorageElement( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ None ]
    res = self.rsDB.addOrModifyStorageElement( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ 'CERN-PROD', 'eGGs', None ]
    res = self.rsDB.addOrModifyStorageElement( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )     
  
    res = self.rsDB.addOrModifyStorageElement( *tuple( initArgs ) )
    self.assertEqual( res[ 'OK' ], False )   
    res = self.rsDB.addOrModifyStorageElement( 'eGGs', 'xyz.cern.ch', 'CERN-PROD' )
    self.assertEqual( res[ 'OK' ], False )   
    res = self.rsDB.addOrModifyStorageElement( 'CERN-test', 'eGGs', 'CERN-PROD' )
    self.assertEqual( res[ 'OK' ], False )   
    res = self.rsDB.addOrModifyStorageElement( 'CERN-test', 'xyz.cern.ch', 'eGGs')
    self.assertEqual( res[ 'OK' ], False )   
       
    # This one looks correct, it is correct, but Sites table is empty !!
    res = self.rsDB.addOrModifyStorageElement( 'CERN-test', 'xyz.cern.ch', 'CERN-PROD' )
    self.assertEqual( res[ 'OK' ], False )   
            
  def test_02_setStorageElementStatus( self ):
    '''
    setStorageElementStatus( self, storageElementName, statusType, status, 
                             reason, dateCreated, dateEffective, dateEnd, 
                             lastCheckTime, tokenOwner, tokenExpiration ) 
    '''

    ins = inspect.getargspec( self.rsDB.setStorageElementStatus.f )   
    self.assertEqual( ins.args, [ 'self', 'storageElementName', 'statusType', 'status', 
                                  'reason', 'dateCreated', 'dateEffective', 'dateEnd', 
                                  'lastCheckTime', 'tokenOwner', 'tokenExpiration' ] )
    self.assertEqual( ins.varargs,  None)
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )
    
    dNow = datetime.now()
        
    initArgs = [ None, None, None, None, None, None, None, None, None, None ]
    
    dSol = datetime( 9999, 12, 11, 10, 9, 8 )
    
    res = self.rsDB.setStorageElementStatus( *tuple( initArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    #Test first parameter ( siteName )
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'CERN-test'
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'CERN-test' ]
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = 'eGGs'
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'eGGs' ]
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'CERN-test', 'eGGs' ]
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ None ]
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'CERN-test', 'eGGs', None ]
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    #Test second parameter ( statusType )
    modArgs[ 1 ] = ''
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ '' ]
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ 'eGGs' ]
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ '', 'eGGs' ]
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ None ]
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ '', 'eGGs', None ]
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )

    #Test third parameter ( status )
    modArgs[ 2 ] = 'Active'
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ 'Active' ]
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = 'eGGs'
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ 'eGGs' ]
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ 'Active', 'eGGs' ]
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ None ]
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ 'Active', 'eGGs', None ]
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    #Test forth parameter ( reason )
    modArgs = initArgs[:]
    modArgs[ 3 ] = 'Init'
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 3 ] = [ 'Init' ]    
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 3 ] = 'eGGs'
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 3 ] = [ 'eGGs' ]
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 3 ] = [ 'Init', 'eGGs' ]
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 3 ] = [ None ]
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )         
    modArgs[ 3 ] = [ 'Init', 'eGGs', None ]
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )  

    #Test fifth parameter ( dateCreated )
    modArgs = initArgs[:]
    modArgs[ 4 ] = dSol
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 4 ] = [ dSol ]    
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )     
    modArgs[ 4 ] = dNow
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 4 ] = [ dNow ]
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 4 ] = [ dSol, dNow ]
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 4 ] = [ None ]
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 4 ] = [ dSol, dNow, None ]
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
            
    #Test sixth parameter ( dateEffective ) 
    modArgs = initArgs[:]
    modArgs[ 5 ] = dSol
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )    
    modArgs[ 5 ] = [ dSol ]    
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )         
    modArgs[ 5 ] = dNow
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )  
    modArgs[ 5 ] = [ dNow ]
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 5 ] = [ dSol, dNow ]
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )     
    modArgs[ 5 ] = [ None ]
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 5 ] = [ dSol, dNow, None ]
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )     

    #Test seventh parameter ( dateEnd )
    modArgs = initArgs[:]
    modArgs[ 6 ] = dSol
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )   
    modArgs[ 6 ] = [ dSol ]    
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )        
    modArgs[ 6 ] = dNow
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 6 ] = [ dNow ]
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 6 ] = [ dSol, dNow ]
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 6 ] = [ None ]
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 6 ] = [ dSol, dNow, None ]
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
            
    #Test eighth parameter ( lastCheckTime )
    modArgs = initArgs[:]
    modArgs[ 7 ] = dSol
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )   
    modArgs[ 7 ] = [ dSol ]    
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )        
    modArgs[ 7 ] = dNow
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 7 ] = [ dNow ]
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 7 ] = [ dSol, dNow ]
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 7 ] = [ None ]
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 7 ] = [ dSol, dNow, None ]
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    #Test ninth parameter ( tokenOwner )
    modArgs = initArgs[:]
    modArgs[ 8 ] = 'RS_SVC'
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 8 ] = [ 'RS_SVC' ]   
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )   
    modArgs[ 8 ] = 'eGGs'   
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 8 ] = [ 'eGGs' ]   
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs' ]   
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 8 ] = [ None ]   
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs', None ]   
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    #Test tenth parameter ( tokenExpiration )
    modArgs = initArgs[:]
    modArgs[ 9 ] = dSol
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )   
    modArgs[ 9 ] = [ dSol ]    
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )        
    modArgs[ 9 ] = dNow
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 9 ] = [ dNow ]
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 9 ] = [ dSol, dNow ]
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 9 ] = [ None ]
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )    
    modArgs[ 9 ] = [ dSol, dNow, None ]
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'CERN-test'
    modArgs[ 1 ] = ''
    modArgs[ 2 ] = 'Active'    
    modArgs[ 3 ] = 'Reason'
    modArgs[ 8 ] = 'token'
    
    #This should work, but the DB is empty
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    modArgs[ 0 ] =  [ 'CERN-test' ]
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] =  'CERN-test'
    modArgs[ 1 ] =  ''
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = ''
    modArgs[ 2 ] = [ 'Active' ]
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = 'Active'
    modArgs[ 4 ] = 'eGGs'
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 4 ] = None
    modArgs[ 5 ] = 'eGGs'
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 5 ] = None
    modArgs[ 6 ] = 'eGGs'
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 6 ] = None
    modArgs[ 7 ] = 'eGGs'
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 7 ] = None
    modArgs[ 9 ] = 'eGGs'
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    now = datetime.now()
    modArgs = [ 'eGGs', '', 'Active', None, now, now, now, now, None, now]
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = 'CERN-test'
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = ''
    modArgs[ 2 ] = 'eGGs'
    res = self.rsDB.setStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
  def test_03_setStorageElementScheduledStatus( self ):
    '''
    setStorageElementScheduledStatus( self, storageElementName, statusType, 
                                      status, reason, dateCreated, 
                                      dateEffective, dateEnd, lastCheckTime, 
                                      tokenOwner, tokenExpiration )
    '''   
        
    ins = inspect.getargspec( self.rsDB.setStorageElementScheduledStatus.f )   
    self.assertEqual( ins.args, [ 'self', 'storageElementName', 'statusType', 'status', 
                                  'reason', 'dateCreated', 'dateEffective', 'dateEnd', 
                                  'lastCheckTime', 'tokenOwner', 'tokenExpiration' ] )
    self.assertEqual( ins.varargs,  None)
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )
    
    dNow = datetime.now()
        
    initArgs = [ None, None, None, None, None, None, None, None, None, None ]
    
    dSol = datetime( 9999, 12, 11, 10, 9, 8 )
    
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( initArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    #Test first parameter ( siteName )
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'CERN-test'
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'CERN-test' ]
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = 'eGGs'
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'eGGs' ]
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'CERN-test', 'eGGs' ]
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ None ]
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'CERN-test', 'eGGs', None ]
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    #Test second parameter ( statusType )
    modArgs[ 1 ] = ''
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ '' ]
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ 'eGGs' ]
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ '', 'eGGs' ]
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ None ]
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ '', 'eGGs', None ]
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )

    #Test third parameter ( status )
    modArgs[ 2 ] = 'Active'
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ 'Active' ]
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = 'eGGs'
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ 'eGGs' ]
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ 'Active', 'eGGs' ]
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ None ]
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ 'Active', 'eGGs', None ]
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    #Test forth parameter ( reason )
    modArgs = initArgs[:]
    modArgs[ 3 ] = 'Init'
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 3 ] = [ 'Init' ]    
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 3 ] = 'eGGs'
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 3 ] = [ 'eGGs' ]
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 3 ] = [ 'Init', 'eGGs' ]
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 3 ] = [ None ]
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )         
    modArgs[ 3 ] = [ 'Init', 'eGGs', None ]
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )  

    #Test fifth parameter ( dateCreated )
    modArgs = initArgs[:]
    modArgs[ 4 ] = dSol
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 4 ] = [ dSol ]    
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )     
    modArgs[ 4 ] = dNow
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 4 ] = [ dNow ]
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 4 ] = [ dSol, dNow ]
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 4 ] = [ None ]
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 4 ] = [ dSol, dNow, None ]
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
            
    #Test sixth parameter ( dateEffective ) 
    modArgs = initArgs[:]
    modArgs[ 5 ] = dSol
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )    
    modArgs[ 5 ] = [ dSol ]    
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )         
    modArgs[ 5 ] = dNow
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )  
    modArgs[ 5 ] = [ dNow ]
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 5 ] = [ dSol, dNow ]
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )     
    modArgs[ 5 ] = [ None ]
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 5 ] = [ dSol, dNow, None ]
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )     

    #Test seventh parameter ( dateEnd )
    modArgs = initArgs[:]
    modArgs[ 6 ] = dSol
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )   
    modArgs[ 6 ] = [ dSol ]    
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )        
    modArgs[ 6 ] = dNow
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 6 ] = [ dNow ]
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 6 ] = [ dSol, dNow ]
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 6 ] = [ None ]
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 6 ] = [ dSol, dNow, None ]
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
            
    #Test eighth parameter ( lastCheckTime )
    modArgs = initArgs[:]
    modArgs[ 7 ] = dSol
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )   
    modArgs[ 7 ] = [ dSol ]    
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )        
    modArgs[ 7 ] = dNow
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 7 ] = [ dNow ]
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 7 ] = [ dSol, dNow ]
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 7 ] = [ None ]
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 7 ] = [ dSol, dNow, None ]
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    #Test ninth parameter ( tokenOwner )
    modArgs = initArgs[:]
    modArgs[ 8 ] = 'RS_SVC'
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 8 ] = [ 'RS_SVC' ]   
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )   
    modArgs[ 8 ] = 'eGGs'   
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 8 ] = [ 'eGGs' ]   
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs' ]   
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 8 ] = [ None ]   
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs', None ]   
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    #Test tenth parameter ( tokenExpiration )
    modArgs = initArgs[:]
    modArgs[ 9 ] = dSol
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )   
    modArgs[ 9 ] = [ dSol ]    
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )        
    modArgs[ 9 ] = dNow
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 9 ] = [ dNow ]
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 9 ] = [ dSol, dNow ]
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 9 ] = [ None ]
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )    
    modArgs[ 9 ] = [ dSol, dNow, None ]
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'CERN-test'
    modArgs[ 1 ] = ''
    modArgs[ 2 ] = 'Active'    
    modArgs[ 3 ] = 'Reason'
    modArgs[ 8 ] = 'token'
    
    #This should work, but the DB is empty
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    modArgs[ 0 ] =  [ 'CERN-test' ]
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] =  'CERN-test'
    modArgs[ 1 ] =  ''
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = ''
    modArgs[ 2 ] = [ 'Active' ]
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = 'Active'
    modArgs[ 4 ] = 'eGGs'
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 4 ] = None
    modArgs[ 5 ] = 'eGGs'
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 5 ] = None
    modArgs[ 6 ] = 'eGGs'
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 6 ] = None
    modArgs[ 7 ] = 'eGGs'
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 7 ] = None
    modArgs[ 9 ] = 'eGGs'
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    now = datetime.now()
    modArgs = [ 'eGGs', '', 'Active', None, now, now, now, now, None, now]
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = 'CERN-test'
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = ''
    modArgs[ 2 ] = 'eGGs'
    res = self.rsDB.setStorageElementScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
  def test_04_updateStorageElementStatus( self ):
    '''
    updateStorageElementStatus( self, storageElementName, statusType, status, 
                                reason, dateCreated, dateEffective, dateEnd, 
                                lastCheckTime, tokenOwner, tokenExpiration )
    '''
    
    ins = inspect.getargspec( self.rsDB.updateStorageElementStatus.f )   
    self.assertEqual( ins.args, [ 'self', 'storageElementName', 'statusType', 
                                  'status', 'reason', 'dateCreated', 'dateEffective', 
                                  'dateEnd', 'lastCheckTime', 'tokenOwner', 
                                  'tokenExpiration' ] )
    self.assertEqual( ins.varargs,  None)
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )
    
    dNow = datetime.now()
    
    initArgs = [ None, None, None, None, None, None, None, None, None, None ]
    
    dSol= datetime( 9999, 12, 11, 10, 9, 8 )
    
    res = self.rsDB.updateStorageElementStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    
    '''
      Param tests
    '''    
    #Test first parameter ( siteName )
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'CERN-test'
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'CERN-test' ]
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = 'eGGs'
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'eGGs' ]
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'CERN-test', 'eGGs' ]
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ None ]
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'CERN-test', 'eGGs', None ]
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    #Test second parameter ( statusType )
    modArgs[ 1 ] = ''
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ '' ]
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ 'eGGs' ]
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ '', 'eGGs' ]
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ None ]
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ '', 'eGGs', None ]
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )

    #Test third parameter ( status )
    modArgs[ 2 ] = 'Active'
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ 'Active' ]
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = 'eGGs'
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ 'eGGs' ]
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ 'Active', 'eGGs' ]
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ None ]
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ 'Active', 'eGGs', None ]
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    #Test forth parameter ( reason )
    modArgs = initArgs[:]
    modArgs[ 3 ] = 'Init'
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 3 ] = [ 'Init' ]    
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 3 ] = 'eGGs'
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 3 ] = [ 'eGGs' ]
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 3 ] = [ 'Init', 'eGGs' ]
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 3 ] = [ None ]
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )         
    modArgs[ 3 ] = [ 'Init', 'eGGs', None ]
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )  

    #Test fifth parameter ( dateCreated )
    modArgs = initArgs[:]
    modArgs[ 4 ] = dSol
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 4 ] = [ dSol ]    
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )     
    modArgs[ 4 ] = dNow
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 4 ] = [ dNow ]
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 4 ] = [ dSol, dNow ]
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 4 ] = [ None ]
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 4 ] = [ dSol, dNow, None ]
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
            
    #Test sixth parameter ( dateEffective ) 
    modArgs = initArgs[:]
    modArgs[ 5 ] = dSol
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )    
    modArgs[ 5 ] = [ dSol ]    
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )         
    modArgs[ 5 ] = dNow
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )  
    modArgs[ 5 ] = [ dNow ]
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 5 ] = [ dSol, dNow ]
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )     
    modArgs[ 5 ] = [ None ]
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 5 ] = [ dSol, dNow, None ]
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )     

    #Test seventh parameter ( dateEnd )
    modArgs = initArgs[:]
    modArgs[ 6 ] = dSol
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )   
    modArgs[ 6 ] = [ dSol ]    
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )        
    modArgs[ 6 ] = dNow
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 6 ] = [ dNow ]
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 6 ] = [ dSol, dNow ]
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 6 ] = [ None ]
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 6 ] = [ dSol, dNow, None ]
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
            
    #Test eighth parameter ( lastCheckTime )
    modArgs = initArgs[:]
    modArgs[ 7 ] = dSol
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )   
    modArgs[ 7 ] = [ dSol ]    
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )        
    modArgs[ 7 ] = dNow
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 7 ] = [ dNow ]
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 7 ] = [ dSol, dNow ]
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 7 ] = [ None ]
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 7 ] = [ dSol, dNow, None ]
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    #Test ninth parameter ( tokenOwner )
    modArgs = initArgs[:]
    modArgs[ 8 ] = 'RS_SVC'
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 8 ] = [ 'RS_SVC' ]   
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )  
    modArgs[ 8 ] = 'eGGs'   
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 8 ] = [ 'eGGs' ]   
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs' ]   
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 8 ] = [ None ]   
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs', None ]   
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    #Test tenth parameter ( tokenExpiration )
    modArgs = initArgs[:]
    modArgs[ 9 ] = dSol
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )   
    modArgs[ 9 ] = [ dSol ]    
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )        
    modArgs[ 9 ] = dNow
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 9 ] = [ dNow ]
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 9 ] = [ dSol, dNow ]
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 9 ] = [ None ]
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )    
    modArgs[ 9 ] = [ dSol, dNow, None ]
    res = self.rsDB.updateStorageElementStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
       
    
    res = self.rsDB.updateStorageElementStatus( 'CERN-test', '', 'Active', None, None,
                                       None,None,None,None,None )
    self.assertEqual( res[ 'OK' ], False )
    
    res = self.rsDB.updateStorageElementStatus( ['CERN-test'], '', 'Active', None, None,
                                            None, None, None, None, None )
    self.assertEqual( res[ 'OK' ], False )
    res = self.rsDB.updateStorageElementStatus( 'CERN-test', [''], 'Active', None, None,
                                            None, None, None, None, None )
    self.assertEqual( res[ 'OK' ], False )
    res = self.rsDB.updateStorageElementStatus( 'CERN-test', '', ['Active'], None, None,
                                            None, None, None, None, None )
    self.assertEqual( res[ 'OK' ], False )
    res = self.rsDB.updateStorageElementStatus( 'CERN-test', '', 'Active', None, 'eGGs',
                                            None, None, None, None, None )
    self.assertEqual( res[ 'OK' ], False )
    res = self.rsDB.updateStorageElementStatus( 'CERN-test', '', 'Active', None, None,
                                            'eGGs', None, None, None, None )
    self.assertEqual( res[ 'OK' ], False )
    res = self.rsDB.updateStorageElementStatus( 'CERN-test', '', 'Active', None, None,
                                            None, 'eGGs', None, None, None )
    self.assertEqual( res[ 'OK' ], False )
    res = self.rsDB.updateStorageElementStatus( 'CERN-test', '', 'Active', None, None,
                                            None, None, 'eGGs', None, None )
    self.assertEqual( res[ 'OK' ], False )
    res = self.rsDB.updateStorageElementStatus( 'CERN-test', '', 'Active', None, None,
                                            None, None, None, None, 'eGGs' )
    self.assertEqual( res[ 'OK' ], False )
    
    now = datetime.now()
    
    #Our DB is empty, some validations must break !
    res = self.rsDB.updateStorageElementStatus( 'CERN-test', '', 'Active', None, now,
                                            now, now, now, None, now )
    self.assertEqual( res[ 'OK' ], False )
    res = self.rsDB.updateStorageElementStatus( 'eGGs', '', 'Active', None, now,
                                            now, now, now, None, now )
    self.assertEqual( res[ 'OK' ], False )
    res = self.rsDB.updateStorageElementStatus( 'CERN-test', 'eGGs', 'Active', None, now,
                                            now, now, now, None, now )
    self.assertEqual( res[ 'OK' ], False )
    res = self.rsDB.updateStorageElementStatus( 'CERN-test', '', 'eGGs', None, now,
                                       now, now, now, None, now )
    self.assertEqual( res[ 'OK' ], False )
        
  def test_05_getStorageElements( self ):
    '''
    getStorageElements( self, storageElementName, resourceName, gridSiteName, **kwargs )
    '''
    ins = inspect.getargspec( self.rsDB.getStorageElements.f )   
    self.assertEqual( ins.args, [ 'self', 'storageElementName', 'resourceName', 'gridSiteName' ] )
    self.assertEqual( ins.varargs,  None)
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, None )
    
    initArgs = [ None, None, None ]
    
    res = self.rsDB.getStorageElements( *tuple( initArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    
    '''
      Param tests
    '''    
    #Test first param ( serviceName )
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'CERN-test'
    res = self.rsDB.getStorageElements( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 0 ] = [ 'CERN-test' ]
    res = self.rsDB.getStorageElements( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = [ 'CERN-test', 'CERN-notest' ]
    res = self.rsDB.getStorageElements( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = 'eGGs'
    res = self.rsDB.getStorageElements( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'eGGs' ]
    res = self.rsDB.getStorageElements( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'CERN-test', 'CERN-notest', 'eGGs' ]
    res = self.rsDB.getStorageElements( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ None ]
    res = self.rsDB.getStorageElements( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'CERN-test', 'CERN-notest', 'eGGs', None ]
    res = self.rsDB.getStorageElements( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 

    modArgs = initArgs[:]
    modArgs[ 1 ] = 'CE'
    res = self.rsDB.getStorageElements( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ 'CE' ]
    res = self.rsDB.getStorageElements( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ 'CE', 'CREAMCE' ]
    res = self.rsDB.getStorageElements( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.getStorageElements( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ 'eGGs' ]
    res = self.rsDB.getStorageElements( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ 'CE', 'CREAMCE', 'eGGs' ]
    res = self.rsDB.getStorageElements( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ None ]
    res = self.rsDB.getStorageElements( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ 'CE', 'CREAMCE', 'eGGs', None ]
    res = self.rsDB.getStorageElements( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    
    #Test third param ( siteName )
    modArgs = initArgs[:]
    modArgs[ 2 ] = 'CERN-PROD'
    res = self.rsDB.getStorageElements( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = [ 'CERN-PROD' ]
    res = self.rsDB.getStorageElements( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = [ 'CERN-PROD', 'INFN-T1' ]
    res = self.rsDB.getStorageElements( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = 'eGGs'
    res = self.rsDB.getStorageElements( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = [ 'eGGs' ]
    res = self.rsDB.getStorageElements( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = [ 'CERN-PROD', 'INFN-T1', 'eGGs' ]
    res = self.rsDB.getStorageElements( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = [ None ]
    res = self.rsDB.getStorageElements( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = [ 'CERN-PROD', 'INFN-T1', 'eGGs', None ]
    res = self.rsDB.getStorageElements( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
  
  def test_06_getStorageElementsStatus( self ):
    '''
    getStorageElementsStatus( self, storageElementName, statusType, status, reason, dateCreated, 
                              dateEffective, dateEnd, lastCheckTime, tokenOwner, 
                              tokenExpiration, **kwargs )
    '''
    dNow = datetime.now()
    
    ins = inspect.getargspec( self.rsDB.getStorageElementsStatus.f )   
    self.assertEqual( ins.args, [ 'self', 'storageElementName', 'statusType', 'status', 
                                  'reason', 'dateCreated', 'dateEffective', 'dateEnd', 
                                  'lastCheckTime', 'tokenOwner', 'tokenExpiration' ] )
    self.assertEqual( ins.varargs,  None)
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, None )
    
    initArgs = [ None,None,None,None,None,None,None,None,None,None ]
    dSol     = datetime(9999, 12, 11, 10, 9, 8)
        
    res = self.rsDB.getStorageElementsStatus( *tuple( initArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    
    '''
      Param tests
    '''    
    #Test first param ( siteName )
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'CERN-test'
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'CERN-test' ]    
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 0 ] = [ 'CERN-test', 'CERN-notest' ]
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = 'eGGs'
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'eGGs' ]
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'CERN-test', 'CERN-notest' ]
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ None ]
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'CERN-test', 'eGGs', None ]
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )

    #Test second param ( statusType ) 
    modArgs = initArgs[:]
    modArgs[ 1 ] = ''
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ '' ]    
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ 'eGGs' ]
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ '', 'eGGs' ]
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ None ]
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ '', 'eGGs', None ]
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )

    #Test third parameter ( status )
    modArgs = initArgs[:]
    modArgs[ 2 ] = 'Active'
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = [ 'Active' ]    
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    modArgs[ 2 ] = 'Banned'
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 2 ] = [ 'Banned' ]
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = [ 'Active', 'Banned' ]
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = 'eGGs'
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = [ 'eGGs' ]
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )    
    modArgs[ 2 ] = [ 'eGGs', 'Banned' ]
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 2 ] = [ None ]
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )   
    modArgs[ 2 ] = [ 'eGGs', 'Banned', None ]
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )

    #Test forth parameter ( reason )
    modArgs = initArgs[:]
    modArgs[ 3 ] = 'Init'
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 3 ] = [ 'Init' ]    
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 3 ] = 'eGGs'
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 3 ] = [ 'eGGs' ]
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 3 ] = [ 'Init', 'eGGs' ]
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )         
    modArgs[ 3 ] = [ None ]
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 3 ] = [ 'Init', 'eGGs', None ]
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )

    #Test fifth parameter ( dateCreated )
    modArgs = initArgs[:]
    modArgs[ 4 ] = dSol
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 4 ] = [ dSol ]    
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 4 ] = dNow
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 4 ] = [ dNow ]
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 4 ] = [ dSol, dNow ]
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )    
    modArgs[ 4 ] = [ None ]
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 4 ] = [ dSol, dNow, None ]
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
            
    #Test sixth parameter ( dateEffective ) 
    modArgs = initArgs[:]
    modArgs[ 5 ] = dSol
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 5 ] = [ dSol ]    
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    modArgs[ 5 ] = dNow
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 5 ] = [ dNow ]
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 5 ] = [ dSol, dNow ]
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )   
    modArgs[ 5 ] = [ None ]
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 5 ] = [ dSol, dNow, None ]
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )

    #Test seventh parameter ( dateEnd )
    modArgs = initArgs[:]
    modArgs[ 6 ] = dSol
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 6 ] = [ dSol ]    
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )      
    modArgs[ 6 ] = dNow
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 6 ] = [ dNow ]
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 6 ] = [ dSol, dNow ]
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )   
    modArgs[ 6 ] = [ None ]
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 6 ] = [ dSol, dNow, None ]
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
                
    #Test eighth parameter ( lastCheckTime )
    modArgs = initArgs[:]
    modArgs[ 7 ] = dSol
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 7 ] = [ dSol ]    
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    modArgs[ 7 ] = dNow
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 7 ] = [ dNow ]
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 7 ] = [ dSol, dNow ]
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )    
    modArgs[ 7 ] = [ None ]
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 7 ] = [ dSol, dNow, None ]
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
        
    #Test ninth parameter ( tokenOwner )
    modArgs = initArgs[:]
    modArgs[ 8 ] = 'RS_SVC'
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'RS_SVC' ]   
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 8 ] = 'eGGs'   
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'eGGs' ]   
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs' ]   
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ None ]   
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs', None ]   
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    
    #Test tenth parameter ( tokenExpiration )
    modArgs = initArgs[:]
    modArgs[ 9 ] = dSol
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 9 ] = [ dSol ]    
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )      
    modArgs[ 9 ] = dNow
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 9 ] = [ dNow ]
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 9 ] = [ dSol, dNow ]
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 9 ] = [ None ]
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 9 ] = [ dSol, dNow, None ]
    res = self.rsDB.getStorageElementsStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
            
             
  def test_07_getStorageElementsHistory( self ):
    '''
    getStorageElementsHistory( self, storageElementName, statusType, status, reason, dateCreated, 
                         dateEffective, dateEnd, lastCheckTime, tokenOwner, 
                         tokenExpiration, **kwargs )
    '''
    dNow = datetime.now()
    
    ins = inspect.getargspec( self.rsDB.getStorageElementsHistory.f )   
    self.assertEqual( ins.args, [ 'self', 'storageElementName', 'statusType', 'status', 
                                  'reason', 'dateCreated', 'dateEffective', 'dateEnd', 
                                  'lastCheckTime', 'tokenOwner', 'tokenExpiration' ] )
    self.assertEqual( ins.varargs,  None)
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, None )
    
    initArgs = [ None,None,None,None,None,None,None,None,None,None ]
    dSol     = datetime(9999, 12, 11, 10, 9, 8)
        
    res = self.rsDB.getStorageElementsHistory( *tuple( initArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    
    '''
      Param tests
    '''    
    #Test first param ( siteName )
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'CERN-test'
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = [ 'CERN-test' ]    
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )          
    modArgs[ 0 ] = [ 'CERN-test', 'CERN-notest' ]
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = 'eGGs'
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = [ 'eGGs' ]
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = [ 'CERN-test', 'eGGs' ]
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = [ None ]
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = [ 'CERN-test', 'eGGs', None ]
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  

    #Test second param ( statusType ) 
    modArgs = initArgs[:]
    modArgs[ 1 ] = ''
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 1 ] = [ '' ]    
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 1 ] = [ 'eGGs' ]
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 1 ] = [ '', 'eGGs' ]
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 1 ] = [ None ]
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 1 ] = [ '', 'eGGs', None ]
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  

    #Test third parameter ( status )
    modArgs = initArgs[:]
    modArgs[ 2 ] = 'Active'
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 2 ] = [ 'Active' ]    
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    modArgs[ 2 ] = 'Banned'
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 2 ] = [ 'Banned' ]
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 2 ] = [ 'Active', 'Banned' ]
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 2 ] = 'eGGs'
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 2 ] = [ 'eGGs' ]
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )     
    modArgs[ 2 ] = [ 'eGGs', 'Banned' ]
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )         
    modArgs[ 2 ] = [ None ]
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    modArgs[ 2 ] = [ 'eGGs', 'Banned', None ]
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  

    #Test forth parameter ( reason )
    modArgs = initArgs[:]
    modArgs[ 3 ] = 'Init'
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 3 ] = [ 'Init' ]    
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )         
    modArgs[ 3 ] = 'eGGs'
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 3 ] = [ 'eGGs' ]
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 3 ] = [ 'Init', 'eGGs' ]
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )            
    modArgs[ 3 ] = [ None ]
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 3 ] = [ 'Init', 'eGGs', None ]
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  

    #Test fifth parameter ( dateCreated )
    modArgs = initArgs[:]
    modArgs[ 4 ] = dSol
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )     
    modArgs[ 4 ] = [ dSol ]    
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 4 ] = dNow
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 4 ] = [ dNow ]
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 4 ] = [ dSol, dNow ]
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )    
    modArgs[ 4 ] = [ None ]
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )  
    modArgs[ 4 ] = [ dSol, dNow, None ]
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )  
            
    #Test sixth parameter ( dateEffective ) 
    modArgs = initArgs[:]
    modArgs[ 5 ] = dSol
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 5 ] = [ dSol ]    
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )      
    modArgs[ 5 ] = dNow
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 5 ] = [ dNow ]
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 5 ] = [ dSol, dNow ]
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )    
    modArgs[ 5 ] = [ None ]
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 5 ] = [ dSol, dNow, None ]
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )

    #Test seventh parameter ( dateEnd )
    modArgs = initArgs[:]
    modArgs[ 6 ] = dSol
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 6 ] = [ dSol ]    
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 6 ] = dNow
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 6 ] = [ dNow ]
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 6 ] = [ dSol, dNow ]
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )     
    modArgs[ 6 ] = [ None ]
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False ) 
    modArgs[ 6 ] = [ dSol, dNow, None ]
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False ) 
                
    #Test eighth parameter ( lastCheckTime )
    modArgs = initArgs[:]
    modArgs[ 7 ] = dSol
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )   
    modArgs[ 7 ] = [ dSol ]    
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    modArgs[ 7 ] = dNow
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 7 ] = [ dNow ]
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 7 ] = [ dSol, dNow ]
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 7 ] = [ None ]
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 7 ] = [ dSol, dNow, None ]
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
        
    #Test ninth parameter ( tokenOwner )
    modArgs = initArgs[:]
    modArgs[ 8 ] = 'RS_SVC'
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'RS_SVC' ]   
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = 'eGGs'   
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'eGGs' ]   
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs' ]   
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ None ]   
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs', None ]   
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    
    #Test tenth parameter ( tokenExpiration )
    modArgs = initArgs[:]
    modArgs[ 9 ] = dSol
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 9 ] = [ dSol ]    
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )      
    modArgs[ 9 ] = dNow
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 9 ] = [ dNow ]
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 9 ] = [ dSol, dNow ]
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )    
    modArgs[ 9 ] = [ None ]
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 9 ] = [ dSol, dNow, None ]
    res = self.rsDB.getStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )

  def test_08_getStorageElementsScheduledStatus( self ):
    '''
    getStorageElementsScheduledStatus( self, storageElementName, statusType, status, reason, dateCreated, 
                                       dateEffective, dateEnd, lastCheckTime, tokenOwner, 
                                       tokenExpiration, **kwargs )
    '''
    dNow = datetime.now()
    
    ins = inspect.getargspec( self.rsDB.getStorageElementsScheduledStatus.f )   
    self.assertEqual( ins.args, [ 'self', 'storageElementName', 'statusType', 'status', 
                                  'reason', 'dateCreated', 'dateEffective', 'dateEnd', 
                                  'lastCheckTime', 'tokenOwner', 
                                  'tokenExpiration' ] )
    self.assertEqual( ins.varargs,  None)
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, None )
    
    initArgs = [ None,None,None,None,None,None,None,None,None,None ]
    dSol     = datetime(9999, 12, 11, 10, 9, 8)
        
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( initArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    
    '''
      Param tests
    '''    
    #Test first param ( siteName )
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'CERN-test'
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = [ 'CERN-test' ]    
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )          
    modArgs[ 0 ] = [ 'CERN-test', 'CERN-notest' ]
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = 'eGGs'
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = [ 'eGGs' ]
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = [ 'CERN-test', 'eGGs' ]
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = [ None ]
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = [ 'CERN-test', 'eGGs', None ]
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  

    #Test second param ( statusType ) 
    modArgs = initArgs[:]
    modArgs[ 1 ] = ''
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 1 ] = [ '' ]    
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 1 ] = [ 'eGGs' ]
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 1 ] = [ '', 'eGGs' ]
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 1 ] = [ None ]
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 1 ] = [ '', 'eGGs', None ]
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  

    #Test third parameter ( status )
    modArgs = initArgs[:]
    modArgs[ 2 ] = 'Active'
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 2 ] = [ 'Active' ]    
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    modArgs[ 2 ] = 'Banned'
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 2 ] = [ 'Banned' ]
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 2 ] = [ 'Active', 'Banned' ]
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 2 ] = 'eGGs'
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 2 ] = [ 'eGGs' ]
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )     
    modArgs[ 2 ] = [ 'eGGs', 'Banned' ]
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )         
    modArgs[ 2 ] = [ None ]
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    modArgs[ 2 ] = [ 'eGGs', 'Banned', None ]
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  

    #Test forth parameter ( reason )
    modArgs = initArgs[:]
    modArgs[ 3 ] = 'Init'
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 3 ] = [ 'Init' ]    
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )         
    modArgs[ 3 ] = 'eGGs'
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 3 ] = [ 'eGGs' ]
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 3 ] = [ 'Init', 'eGGs' ]
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )            
    modArgs[ 3 ] = [ None ]
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 3 ] = [ 'Init', 'eGGs', None ]
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  

    #Test fifth parameter ( dateCreated )
    modArgs = initArgs[:]
    modArgs[ 4 ] = dSol
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )     
    modArgs[ 4 ] = [ dSol ]    
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 4 ] = dNow
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 4 ] = [ dNow ]
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 4 ] = [ dSol, dNow ]
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )    
    modArgs[ 4 ] = [ None ]
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )  
    modArgs[ 4 ] = [ dSol, dNow, None ]
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )  
            
    #Test sixth parameter ( dateEffective ) 
    modArgs = initArgs[:]
    modArgs[ 5 ] = dSol
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 5 ] = [ dSol ]    
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )      
    modArgs[ 5 ] = dNow
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 5 ] = [ dNow ]
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 5 ] = [ dSol, dNow ]
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )    
    modArgs[ 5 ] = [ None ]
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 5 ] = [ dSol, dNow, None ]
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )

    #Test seventh parameter ( dateEnd )
    modArgs = initArgs[:]
    modArgs[ 6 ] = dSol
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 6 ] = [ dSol ]    
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 6 ] = dNow
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 6 ] = [ dNow ]
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 6 ] = [ dSol, dNow ]
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )     
    modArgs[ 6 ] = [ None ]
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False ) 
    modArgs[ 6 ] = [ dSol, dNow, None ]
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False ) 
                
    #Test eighth parameter ( lastCheckTime )
    modArgs = initArgs[:]
    modArgs[ 7 ] = dSol
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )   
    modArgs[ 7 ] = [ dSol ]    
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    modArgs[ 7 ] = dNow
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 7 ] = [ dNow ]
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 7 ] = [ dSol, dNow ]
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 7 ] = [ None ]
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 7 ] = [ dSol, dNow, None ]
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
        
    #Test ninth parameter ( tokenOwner )
    modArgs = initArgs[:]
    modArgs[ 8 ] = 'RS_SVC'
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'RS_SVC' ]   
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = 'eGGs'   
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'eGGs' ]   
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs' ]   
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ None ]   
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs', None ]   
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    
    #Test tenth parameter ( tokenExpiration )
    modArgs = initArgs[:]
    modArgs[ 9 ] = dSol
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 9 ] = [ dSol ]    
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )      
    modArgs[ 9 ] = dNow
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 9 ] = [ dNow ]
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 9 ] = [ dSol, dNow ]
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )    
    modArgs[ 9 ] = [ None ]
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 9 ] = [ dSol, dNow, None ]
    res = self.rsDB.getStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )

  def test_09_getStorageElementsPresent( self ):
    '''
    
    def getStorageElementsPresent( self, storageElementName, resourceName, gridSiteName, 
                                   siteType, statusType, status, dateEffective, 
                                   reason, lastCheckTime, tokenOwner, tokenExpiration, 
                                   formerStatus, **kwargs )                 
    '''
    dNow = datetime.now()
    
    ins = inspect.getargspec( self.rsDB.getStorageElementsPresent.f )   
    self.assertEqual( ins.args, [ 'self', 'storageElementName', 'resourceName','gridSiteName',
                                  'siteType', 'statusType', 'status', 'dateEffective', 
                                  'reason', 'lastCheckTime', 'tokenOwner', 'tokenExpiration', 
                                  'formerStatus' ] )
    self.assertEqual( ins.varargs,  None)
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, None )

    
    initArgs = [ None,None,None,None,None,None,None,None,None,None, None, None ]
    dSol     = datetime(9999, 12, 11, 10, 9, 8)

    res = self.rsDB.getStorageElementsPresent( *tuple( initArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    
    '''
      Param tests
    '''    
    #Test first param ( siteName )
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'CERN-test'
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = [ 'CERN-test' ]    
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )          
    modArgs[ 0 ] = [ 'CERN-test', 'CERN-notest' ]
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = 'eGGs'
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = [ 'eGGs' ]
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = [ 'CERN-test', 'eGGs' ]
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = [ None ]
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = [ 'CERN-test', 'eGGs', None ]
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  

    #Test first param ( siteName )
    modArgs = initArgs[:]
    modArgs[ 1 ] = 'xyz.cern.ch'
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 1 ] = [ 'xyz.cern.ch' ]    
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )          
    modArgs[ 1 ] = [ 'xyz.cern.ch', 'abc.cern.ch' ]
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 1 ] = 'LCG.eGGs.xy'
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 1 ] = [ 'LCG.eGGs.xy' ]
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 1 ] = [ 'xyz.cern.ch', 'LCG.eGGs.xy' ]
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 1 ] = [ None ]
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 1 ] = [ 'xyz.cern.ch', 'LCG.eGGs.xy', None ]
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  

    #Test second param ( siteType )
    modArgs = initArgs[:]
    modArgs[ 2 ] = 'CERN-PROD'
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 2 ] = [ 'CERN-PROD' ]    
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )          
    modArgs[ 2 ] = [ 'CERN-PROD', 'INFN-T1' ]
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 2 ] = 'eGGs'
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 2 ] = [ 'eGGs' ]
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 2 ] = [ 'CERN-PROD', 'eGGs' ]
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 2 ] = [ None ]
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 2 ] = [ 'CERN-PROD', 'eGGs', None ]
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  

    #Test third param ( gridSiteName )
    modArgs = initArgs[:]
    modArgs[ 3 ] = 'T0'
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 3 ] = [ 'T0' ]    
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )          
    modArgs[ 3 ] = [ 'T0', 'T1' ]
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 3 ] = 'eGGs'
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 3 ] = [ 'eGGs' ]
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 3 ] = [ 'T0', 'eGGs' ]
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 3 ] = [ None ]
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 3 ] = [ 'T0', 'eGGs', None ]
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  

    #Test fifth param ( statusType ) 
    modArgs = initArgs[:]
    modArgs[ 4 ] = ''
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 4 ] = [ '' ]    
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 4 ] = 'eGGs'
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 4 ] = [ 'eGGs' ]
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 4 ] = [ '', 'eGGs' ]
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 4 ] = [ None ]
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 4 ] = [ '', 'eGGs', None ]
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  

    #Test sixth parameter ( status )
    modArgs = initArgs[:]
    modArgs[ 5 ] = 'Active'
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 5 ] = [ 'Active' ]    
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    modArgs[ 5 ] = 'Banned'
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 5 ] = [ 'Banned' ]
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 5 ] = [ 'Active', 'Banned' ]
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 5 ] = 'eGGs'
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 5 ] = [ 'eGGs' ]
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )     
    modArgs[ 5 ] = [ 'eGGs', 'Banned' ]
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )         
    modArgs[ 5 ] = [ None ]
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    modArgs[ 5 ] = [ 'eGGs', 'Banned', None ]
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  

    #Test seventh parameter ( dateEffective ) 
    modArgs = initArgs[:]
    modArgs[ 6 ] = dSol
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 6 ] = [ dSol ]    
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )      
    modArgs[ 6 ] = dNow
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 6 ] = [ dNow ]
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 6 ] = [ dSol, dNow ]
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )    
    modArgs[ 6 ] = [ None ]
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 6 ] = [ dSol, dNow, None ]
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )

    #Test eighth parameter ( reason )
    modArgs = initArgs[:]
    modArgs[ 7 ] = 'Init'
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 7 ] = [ 'Init' ]    
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )         
    modArgs[ 7 ] = 'eGGs'
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 7 ] = [ 'eGGs' ]
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 7 ] = [ 'Init', 'eGGs' ]
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )            
    modArgs[ 7 ] = [ None ]
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 7 ] = [ 'Init', 'eGGs', None ]
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
                
    #Test ninth parameter ( lastCheckTime )
    modArgs = initArgs[:]
    modArgs[ 8 ] = dSol
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )   
    modArgs[ 8 ] = [ dSol ]    
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    modArgs[ 8 ] = dNow
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 8 ] = [ dNow ]
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ dSol, dNow ]
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ None ]
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 8 ] = [ dSol, dNow, None ]
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
        
    #Test tenth parameter ( tokenOwner )
    modArgs = initArgs[:]
    modArgs[ 9 ] = 'RS_SVC'
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 9 ] = [ 'RS_SVC' ]   
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 9 ] = 'eGGs'   
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 9 ] = [ 'eGGs' ]   
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 9 ] = [ 'RS_SVC','eGGs' ]   
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 9 ] = [ None ]   
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 9 ] = [ 'RS_SVC','eGGs', None ]   
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    
    #Test eleventh parameter ( tokenExpiration )
    modArgs = initArgs[:]
    modArgs[ 10 ] = dSol
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 10 ] = [ dSol ]    
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )      
    modArgs[ 10 ] = dNow
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 10 ] = [ dNow ]
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 10 ] = [ dSol, dNow ]
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )    
    modArgs[ 10 ] = [ None ]
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 10 ] = [ dSol, dNow, None ]
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )

    #Test 12th parameter ( former status )
    modArgs = initArgs[:]
    modArgs[ 11 ] = 'Active'
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 11 ] = [ 'Active' ]    
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    modArgs[ 11 ] = 'Banned'
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 11 ] = [ 'Banned' ]
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 11 ] = [ 'Active', 'Banned' ]
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 11 ] = 'eGGs'
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 11 ] = [ 'eGGs' ]
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )     
    modArgs[ 11 ] = [ 'eGGs', 'Banned' ]
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )         
    modArgs[ 11 ] = [ None ]
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    modArgs[ 11 ] = [ 'eGGs', 'Banned', None ]
    res = self.rsDB.getStorageElementsPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  

  def test_10_deleteStorageElementsScheduledStatus( self ):
    '''
    deleteStorageElementsScheduledStatus( self, storageElementName, statusType, status, reason, 
                                          dateCreated, dateEffective, dateEnd, 
                                          lastCheckTime, tokenOwner, tokenExpiration, **kwargs)
    '''
    dNow = datetime.now()
    
    ins = inspect.getargspec( self.rsDB.deleteStorageElementsScheduledStatus.f )   
    self.assertEqual( ins.args, [ 'self', 'storageElementName', 'statusType', 'status', 'reason', 
                                  'dateCreated', 'dateEffective', 'dateEnd', 
                                  'lastCheckTime', 'tokenOwner', 'tokenExpiration' ] )
    self.assertEqual( ins.varargs,  None)
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, None )        
       
    dSol     = datetime(9999, 12, 11, 10, 9, 8)      
    initArgs = [ None,None,None,None,None,None,None,None,None,None ]
   
       
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( initArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    
    '''
      Param tests
    '''    
    #Test first param ( siteName )
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'CERN-test'
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'CERN-test' ]    
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 0 ] = [ 'CERN-test', 'CERN-test' ]
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = 'eGGs'
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'eGGs' ]
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'CERN-test', 'eGGs' ]
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ None ]
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'CERN-test', 'eGGs', None ]
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )

    #Test second param ( statusType ) 
    modArgs = initArgs[:]
    modArgs[ 1 ] = ''
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ '' ]    
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 1 ] = [ 'eGGs' ]
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ '', 'eGGs' ]
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 1 ] = [ None ]
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ '', 'eGGs', None ]
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )

    #Test third parameter ( status )
    modArgs = initArgs[:]
    modArgs[ 2 ] = 'Active'
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = [ 'Active' ]    
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )         
    modArgs[ 2 ] = 'Banned'
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 2 ] = [ 'Banned' ]
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = [ 'Active', 'Banned' ]
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 2 ] = 'eGGs'
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = [ 'eGGs' ]
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )     
    modArgs[ 2 ] = [ 'eGGs', 'Banned' ]
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )      
    modArgs[ 2 ] = [ None ]
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 2 ] = [ 'eGGs', 'Banned', None ]
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )

    #Test forth parameter ( reason )
    modArgs = initArgs[:]
    modArgs[ 3 ] = 'Init'
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 3 ] = [ 'Init' ]    
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )     
    modArgs[ 3 ] = 'eGGs'
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 3 ] = [ 'eGGs' ]
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 3 ] = [ 'Init', 'eGGs' ]
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 3 ] = [ None ]
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 3 ] = [ 'Init', 'eGGs', None ]
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )

    #Test fifth parameter ( dateCreated )
    modArgs = initArgs[:]
    modArgs[ 4 ] = dSol
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 4 ] = [ dSol ]    
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    modArgs[ 4 ] = dNow
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 4 ] = [ dNow ]
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 4 ] = [ dSol, dNow ]
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )     
    modArgs[ 4 ] = [ None ]
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 4 ] = [ dSol, dNow, None ]
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
            
    #Test sixth parameter ( dateEffective ) 
    modArgs = initArgs[:]
    modArgs[ 5 ] = dSol
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 5 ] = [ dSol ]    
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 5 ] = dNow
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 5 ] = [ dNow ]
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 5 ] = [ dSol, dNow ]
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )   
    modArgs[ 5 ] = [ None ]
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 5 ] = [ dSol, dNow, None ]
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False ) 

    #Test seventh parameter ( dateEnd )
    modArgs = initArgs[:]
    modArgs[ 6 ] = dSol
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )    
    modArgs[ 6 ] = [ dSol ]    
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )      
    modArgs[ 6 ] = dNow
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 6 ] = [ dNow ]
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 6 ] = [ dSol, dNow ]
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )     
    modArgs[ 6 ] = [ None ]
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 6 ] = [ dSol, dNow, None ]
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
                
    #Test eighth parameter ( lastCheckTime )
    modArgs = initArgs[:]
    modArgs[ 7 ] = dSol
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )   
    modArgs[ 7 ] = [ dSol ]    
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 7 ] = dNow
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 7 ] = [ dNow ]
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 7 ] = [ dSol, dNow ]
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )   
    modArgs[ 7 ] = [ None ]
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 7 ] = [ dSol, dNow, None ]
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
        
    #Test ninth parameter ( tokenOwner )
    modArgs = initArgs[:]
    modArgs[ 8 ] = 'RS_SVC'
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'RS_SVC' ]   
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 8 ] = 'eGGs'   
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 8 ] = [ 'eGGs' ]   
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs' ]   
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ None ]   
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs', None ]   
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    
    #Test tenth parameter ( tokenExpiration )
    modArgs = initArgs[:]
    modArgs[ 9 ] = dSol
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )   
    modArgs[ 9 ] = [ dSol ]    
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )      
    modArgs[ 9 ] = dNow
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 9 ] = [ dNow ]
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 9 ] = [ dSol, dNow ]
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )   
    modArgs[ 9 ] = [ None ]
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 9 ] = [ dSol, dNow, None ]
    res = self.rsDB.deleteStorageElementsScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )

  def test_11_deleteStorageElementsHistory( self ):
    '''
    deleteStorageElementsHistory( self, storageElementName, statusType, status, reason, 
                            dateCreated, dateEffective, dateEnd, 
                            lastCheckTime, tokenOwner, tokenExpiration, kwargs )
    '''
    dNow = datetime.now()
    
    ins = inspect.getargspec( self.rsDB.deleteStorageElementsHistory.f )   
    self.assertEqual( ins.args, [ 'self', 'storageElementName', 'statusType', 'status', 'reason', 
                                  'dateCreated', 'dateEffective', 'dateEnd', 
                                  'lastCheckTime', 'tokenOwner', 'tokenExpiration' ] )
    self.assertEqual( ins.varargs,  None)
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, None )        
       
    dSol     = datetime(9999, 12, 11, 10, 9, 8)      
    initArgs = [ None,None,None,None,None,None,None,None,None,None ]
   
       
    res = self.rsDB.deleteStorageElementsHistory( *tuple( initArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    
    '''
      Param tests
    '''    
    #Test first param ( siteName )
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'CERN-test'
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'CERN-test' ]    
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 0 ] = [ 'CERN-test', 'CERN-notest' ]
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = 'eGGs'
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'eGGs' ]
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'CERN-test', 'eGGs' ]
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ None ]
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'CERN-test', 'eGGs', None ]
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )

    #Test second param ( statusType ) 
    modArgs = initArgs[:]
    modArgs[ 1 ] = ''
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ '' ]    
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 1 ] = [ 'eGGs' ]
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ '', 'eGGs' ]
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 1 ] = [ None ]
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ '', 'eGGs', None ]
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )

    #Test third parameter ( status )
    modArgs = initArgs[:]
    modArgs[ 2 ] = 'Active'
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = [ 'Active' ]    
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )         
    modArgs[ 2 ] = 'Banned'
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 2 ] = [ 'Banned' ]
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = [ 'Active', 'Banned' ]
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 2 ] = 'eGGs'
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = [ 'eGGs' ]
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )     
    modArgs[ 2 ] = [ 'eGGs', 'Banned' ]
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )      
    modArgs[ 2 ] = [ None ]
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 2 ] = [ 'eGGs', 'Banned', None ]
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )

    #Test forth parameter ( reason )
    modArgs = initArgs[:]
    modArgs[ 3 ] = 'Init'
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 3 ] = [ 'Init' ]    
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )     
    modArgs[ 3 ] = 'eGGs'
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 3 ] = [ 'eGGs' ]
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 3 ] = [ 'Init', 'eGGs' ]
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 3 ] = [ None ]
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 3 ] = [ 'Init', 'eGGs', None ]
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )

    #Test fifth parameter ( dateCreated )
    modArgs = initArgs[:]
    modArgs[ 4 ] = dSol
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 4 ] = [ dSol ]    
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    modArgs[ 4 ] = dNow
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 4 ] = [ dNow ]
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 4 ] = [ dSol, dNow ]
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )     
    modArgs[ 4 ] = [ None ]
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 4 ] = [ dSol, dNow, None ]
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
            
    #Test sixth parameter ( dateEffective ) 
    modArgs = initArgs[:]
    modArgs[ 5 ] = dSol
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 5 ] = [ dSol ]    
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 5 ] = dNow
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 5 ] = [ dNow ]
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 5 ] = [ dSol, dNow ]
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )   
    modArgs[ 5 ] = [ None ]
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 5 ] = [ dSol, dNow, None ]
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False ) 

    #Test seventh parameter ( dateEnd )
    modArgs = initArgs[:]
    modArgs[ 6 ] = dSol
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )    
    modArgs[ 6 ] = [ dSol ]    
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )      
    modArgs[ 6 ] = dNow
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 6 ] = [ dNow ]
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 6 ] = [ dSol, dNow ]
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )     
    modArgs[ 6 ] = [ None ]
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 6 ] = [ dSol, dNow, None ]
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
                
    #Test eighth parameter ( lastCheckTime )
    modArgs = initArgs[:]
    modArgs[ 7 ] = dSol
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )   
    modArgs[ 7 ] = [ dSol ]    
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 7 ] = dNow
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 7 ] = [ dNow ]
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 7 ] = [ dSol, dNow ]
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )   
    modArgs[ 7 ] = [ None ]
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 7 ] = [ dSol, dNow, None ]
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
        
    #Test ninth parameter ( tokenOwner )
    modArgs = initArgs[:]
    modArgs[ 8 ] = 'RS_SVC'
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'RS_SVC' ]   
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 8 ] = 'eGGs'   
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 8 ] = [ 'eGGs' ]   
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs' ]   
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ None ]   
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs', None ]   
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    
    #Test tenth parameter ( tokenExpiration )
    modArgs = initArgs[:]
    modArgs[ 9 ] = dSol
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )   
    modArgs[ 9 ] = [ dSol ]    
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )      
    modArgs[ 9 ] = dNow
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 9 ] = [ dNow ]
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 9 ] = [ dSol, dNow ]
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )   
    modArgs[ 9 ] = [ None ]
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 9 ] = [ dSol, dNow, None ]
    res = self.rsDB.deleteStorageElementsHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )

  def test_12_deleteStorageElements( self ):
    '''
    deleteStorageElements( self, storageElementName )
    '''

    ins = inspect.getargspec( self.rsDB.deleteStorageElements.f )   
    self.assertEqual( ins.args, [ 'self', 'storageElementName' ] )
    self.assertEqual( ins.varargs,  None)
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )       

    res = self.rsDB.deleteStorageElements()
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.deleteStorageElements( 'eGGs', None )
    self.assertNotEqual( res.has_key( 'Value' ), True )

    #Test first param
    res = self.rsDB.deleteStorageElements( 'CERN-test' )
    self.assertEquals( res[ 'OK' ], True )
    res = self.rsDB.deleteStorageElements( [ 'CERN-test' ] )
    self.assertEquals( res[ 'OK' ], True )
    res = self.rsDB.deleteStorageElements( 'eGGs' )
    self.assertEquals( res[ 'OK' ], True )
    res = self.rsDB.deleteStorageElements( [ 'eGGs' ] )
    self.assertEquals( res[ 'OK' ], True )
    res = self.rsDB.deleteStorageElements( None )
    self.assertEquals( res[ 'OK' ], False )
    res = self.rsDB.deleteStorageElements( [ None ] )
    self.assertEquals( res[ 'OK' ], False )
    res = self.rsDB.deleteStorageElements( [ 'CERN-test', None ] )
    self.assertEquals( res[ 'OK' ], True )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF