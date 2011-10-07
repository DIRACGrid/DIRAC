from DIRAC import S_OK
from DIRAC.ResourceStatusSystem.DB.test.TestSuite_ResourceStatusDB.TestCase_EmptyDB import TestCase_EmptyDB

from datetime import datetime
import inspect

class Test_EmptyDB_Services( TestCase_EmptyDB ):

  def test_01_addOrModifyService( self ):
    '''
    addOrModifyService( self, serviceName, serviceType, siteName )
    '''
    
    ins = inspect.getargspec( self.rsDB.addOrModifyService.f )   
    self.assertEqual( ins.args, [ 'self', 'serviceName', 'serviceType', 'siteName' ] )
    self.assertEqual( ins.varargs,  None)
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )
    
    initArgs = [ None, None, None ]
    
    #Fails because second parameter does not validate
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'Computing@LCG.CERN.ch'
    res = self.rsDB.addOrModifyService( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'Computing@LCG.CERN.ch' ]
    res = self.rsDB.addOrModifyService( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = 'eGGs'
    res = self.rsDB.addOrModifyService( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'eGGs' ]
    res = self.rsDB.addOrModifyService( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'Computing@LCG.CERN.ch', 'eGGs' ]
    res = self.rsDB.addOrModifyService( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ None ]
    res = self.rsDB.addOrModifyService( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'Computing@LCG.CERN.ch', 'eGGs', None ]
    res = self.rsDB.addOrModifyService( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )

    #Fail because first parameter does not validate
    modArgs = initArgs[:]
    modArgs[ 1 ] = 'Computing'
    res = self.rsDB.addOrModifyService( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ 'Computing' ]
    res = self.rsDB.addOrModifyService( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.addOrModifyService( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ 'eGGs' ]
    res = self.rsDB.addOrModifyService( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ 'T0', 'eGGs' ]
    res = self.rsDB.addOrModifyService( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ None ]
    res = self.rsDB.addOrModifyService( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ 'Computing', 'eGGs', None ]
    res = self.rsDB.addOrModifyService( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )    

    #Fail because first parameter does not validate
    modArgs = initArgs[:]
    modArgs[ 2 ] = 'LCG.CERN.ch'
    res = self.rsDB.addOrModifyService( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ 'LCG.CERN.ch' ]
    res = self.rsDB.addOrModifyService( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = 'eGGs'
    res = self.rsDB.addOrModifyService( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ 'eGGs' ]
    res = self.rsDB.addOrModifyService( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ 'LCG.CERN.ch', 'eGGs' ]
    res = self.rsDB.addOrModifyService( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ None ]
    res = self.rsDB.addOrModifyService( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ 'LCG.CERN.ch', 'eGGs', None ]
    res = self.rsDB.addOrModifyService( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )     
           
    res = self.rsDB.addOrModifyService( None, None, None )
    self.assertEqual( res[ 'OK' ], False )   
    res = self.rsDB.addOrModifyService( 'Computing@LCG.CERN.ch', None, None )
    self.assertEqual( res[ 'OK' ], False )   
    res = self.rsDB.addOrModifyService( 'Computing@LCG.CERN.ch', None, 'LCG.CERN.ch' )
    self.assertEqual( res[ 'OK' ], False )   
    res = self.rsDB.addOrModifyService( 'Computing@LCG.CERN.ch', 'eGGs', 'LCG.CERN.ch' )
    self.assertEqual( res[ 'OK' ], False )   
    # This one looks correct, it is correct, but Sites table is empty !!
    res = self.rsDB.addOrModifyService( 'Computing@LCG.CERN.ch', 'Computing', 'LCG.CERN.ch' )
    self.assertEqual( res[ 'OK' ], False )   
            
  def test_02_setServiceStatus( self ):
    '''
    setServiceStatus( self, serviceName, statusType, status, reason, dateCreated, 
                        dateEffective, dateEnd, lastCheckTime,tokenOwner,
                        tokenExpiration ) 
    '''

    ins = inspect.getargspec( self.rsDB.setServiceStatus.f )   
    self.assertEqual( ins.args, [ 'self', 'serviceName', 'statusType', 'status', 'reason', 'dateCreated', 
                                  'dateEffective', 'dateEnd', 'lastCheckTime', 'tokenOwner', 
                                  'tokenExpiration' ] )
    self.assertEqual( ins.varargs,  None)
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )
    
    dNow = datetime.now()
        
    initArgs = [ None, None, None, None, None, None, None, None, None, None ]
    
    dSol = datetime( 9999, 12, 11, 10, 9, 8 )
    
    res = self.rsDB.setServiceStatus( *tuple( initArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    #Test first parameter ( siteName )
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'Computing@LCG.CERN.ch'
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'Computing@LCG.CERN.ch' ]
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = 'eGGs'
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'eGGs' ]
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'Computing@LCG.CERN.ch', 'eGGs' ]
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ None ]
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'Computing@LCG.CERN.ch', 'eGGs', None ]
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    #Test second parameter ( statusType )
    modArgs[ 1 ] = ''
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ '' ]
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ 'eGGs' ]
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ '', 'eGGs' ]
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ None ]
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ '', 'eGGs', None ]
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )

    #Test third parameter ( status )
    modArgs[ 2 ] = 'Active'
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ 'Active' ]
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = 'eGGs'
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ 'eGGs' ]
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ 'Active', 'eGGs' ]
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ None ]
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ 'Active', 'eGGs', None ]
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    #Test forth parameter ( reason )
    modArgs = initArgs[:]
    modArgs[ 3 ] = 'Init'
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 3 ] = [ 'Init' ]    
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 3 ] = 'eGGs'
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 3 ] = [ 'eGGs' ]
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 3 ] = [ 'Init', 'eGGs' ]
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 3 ] = [ None ]
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )         
    modArgs[ 3 ] = [ 'Init', 'eGGs', None ]
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )  

    #Test fifth parameter ( dateCreated )
    modArgs = initArgs[:]
    modArgs[ 4 ] = dSol
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 4 ] = [ dSol ]    
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )     
    modArgs[ 4 ] = dNow
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 4 ] = [ dNow ]
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 4 ] = [ dSol, dNow ]
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 4 ] = [ None ]
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 4 ] = [ dSol, dNow, None ]
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
            
    #Test sixth parameter ( dateEffective ) 
    modArgs = initArgs[:]
    modArgs[ 5 ] = dSol
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )    
    modArgs[ 5 ] = [ dSol ]    
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )         
    modArgs[ 5 ] = dNow
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )  
    modArgs[ 5 ] = [ dNow ]
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 5 ] = [ dSol, dNow ]
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )     
    modArgs[ 5 ] = [ None ]
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 5 ] = [ dSol, dNow, None ]
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )     

    #Test seventh parameter ( dateEnd )
    modArgs = initArgs[:]
    modArgs[ 6 ] = dSol
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )   
    modArgs[ 6 ] = [ dSol ]    
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )        
    modArgs[ 6 ] = dNow
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 6 ] = [ dNow ]
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 6 ] = [ dSol, dNow ]
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 6 ] = [ None ]
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 6 ] = [ dSol, dNow, None ]
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
            
    #Test eighth parameter ( lastCheckTime )
    modArgs = initArgs[:]
    modArgs[ 7 ] = dSol
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )   
    modArgs[ 7 ] = [ dSol ]    
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )        
    modArgs[ 7 ] = dNow
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 7 ] = [ dNow ]
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 7 ] = [ dSol, dNow ]
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 7 ] = [ None ]
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 7 ] = [ dSol, dNow, None ]
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    #Test ninth parameter ( tokenOwner )
    modArgs = initArgs[:]
    modArgs[ 8 ] = 'RS_SVC'
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 8 ] = [ 'RS_SVC' ]   
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )   
    modArgs[ 8 ] = 'eGGs'   
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 8 ] = [ 'eGGs' ]   
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs' ]   
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 8 ] = [ None ]   
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs', None ]   
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    #Test tenth parameter ( tokenExpiration )
    modArgs = initArgs[:]
    modArgs[ 9 ] = dSol
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )   
    modArgs[ 9 ] = [ dSol ]    
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )        
    modArgs[ 9 ] = dNow
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 9 ] = [ dNow ]
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 9 ] = [ dSol, dNow ]
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 9 ] = [ None ]
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )    
    modArgs[ 9 ] = [ dSol, dNow, None ]
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'Computing@LCG.CERN.ch'
    modArgs[ 1 ] = ''
    modArgs[ 2 ] = 'Active'    
    modArgs[ 3 ] = 'Reason'
    modArgs[ 8 ] = 'token'
    
    #This should work, but the DB is empty
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    modArgs[ 0 ] =  [ 'Computing@LCG.CERN.ch' ]
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] =  'Computing@LCG.CERN.ch'
    modArgs[ 1 ] =  ''
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = ''
    modArgs[ 2 ] = [ 'Active' ]
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = 'Active'
    modArgs[ 4 ] = 'eGGs'
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 4 ] = None
    modArgs[ 5 ] = 'eGGs'
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 5 ] = None
    modArgs[ 6 ] = 'eGGs'
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 6 ] = None
    modArgs[ 7 ] = 'eGGs'
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 7 ] = None
    modArgs[ 9 ] = 'eGGs'
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    now = datetime.now()
    modArgs = [ 'eGGs', '', 'Active', None, now, now, now, now, None, now]
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = 'Computing@LCG.CERN.ch'
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = ''
    modArgs[ 2 ] = 'eGGs'
    res = self.rsDB.setServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
  def test_03_setServiceScheduledStatus( self ):
    '''
    setServiceScheduledStatus( self, serviceName, statusType, status, reason, dateCreated, 
                               dateEffective, dateEnd, lastCheckTime, tokenOwner, 
                               tokenExpiration )
    '''   
        
    ins = inspect.getargspec( self.rsDB.setServiceScheduledStatus.f )   
    self.assertEqual( ins.args, [ 'self', 'serviceName', 'statusType', 'status', 'reason', 'dateCreated', 
                                  'dateEffective', 'dateEnd', 'lastCheckTime', 'tokenOwner', 
                                  'tokenExpiration' ] )
    self.assertEqual( ins.varargs,  None)
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )
    
    dNow = datetime.now()
        
    initArgs = [ None, None, None, None, None, None, None, None, None, None ]
    
    dSol = datetime( 9999, 12, 11, 10, 9, 8 )
    
    res = self.rsDB.setServiceScheduledStatus( *tuple( initArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    #Test first parameter ( siteName )
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'Computing@LCG.CERN.ch'
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'Computing@LCG.CERN.ch' ]
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = 'eGGs'
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'eGGs' ]
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'Computing@LCG.CERN.ch', 'eGGs' ]
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ None ]
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'Computing@LCG.CERN.ch', 'eGGs', None ]
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    #Test second parameter ( statusType )
    modArgs[ 1 ] = ''
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ '' ]
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ 'eGGs' ]
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ '', 'eGGs' ]
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ None ]
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ '', 'eGGs', None ]
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )

    #Test third parameter ( status )
    modArgs[ 2 ] = 'Active'
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ 'Active' ]
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = 'eGGs'
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ 'eGGs' ]
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ 'Active', 'eGGs' ]
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ None ]
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ 'Active', 'eGGs', None ]
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    #Test forth parameter ( reason )
    modArgs = initArgs[:]
    modArgs[ 3 ] = 'Init'
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 3 ] = [ 'Init' ]    
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 3 ] = 'eGGs'
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 3 ] = [ 'eGGs' ]
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 3 ] = [ 'Init', 'eGGs' ]
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 3 ] = [ None ]
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )         
    modArgs[ 3 ] = [ 'Init', 'eGGs', None ]
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )  

    #Test fifth parameter ( dateCreated )
    modArgs = initArgs[:]
    modArgs[ 4 ] = dSol
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 4 ] = [ dSol ]    
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )     
    modArgs[ 4 ] = dNow
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 4 ] = [ dNow ]
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 4 ] = [ dSol, dNow ]
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 4 ] = [ None ]
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 4 ] = [ dSol, dNow, None ]
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
            
    #Test sixth parameter ( dateEffective ) 
    modArgs = initArgs[:]
    modArgs[ 5 ] = dSol
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )    
    modArgs[ 5 ] = [ dSol ]    
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )         
    modArgs[ 5 ] = dNow
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )  
    modArgs[ 5 ] = [ dNow ]
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 5 ] = [ dSol, dNow ]
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )     
    modArgs[ 5 ] = [ None ]
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 5 ] = [ dSol, dNow, None ]
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )     

    #Test seventh parameter ( dateEnd )
    modArgs = initArgs[:]
    modArgs[ 6 ] = dSol
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )   
    modArgs[ 6 ] = [ dSol ]    
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )        
    modArgs[ 6 ] = dNow
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 6 ] = [ dNow ]
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 6 ] = [ dSol, dNow ]
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 6 ] = [ None ]
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 6 ] = [ dSol, dNow, None ]
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
            
    #Test eighth parameter ( lastCheckTime )
    modArgs = initArgs[:]
    modArgs[ 7 ] = dSol
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )   
    modArgs[ 7 ] = [ dSol ]    
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )        
    modArgs[ 7 ] = dNow
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 7 ] = [ dNow ]
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 7 ] = [ dSol, dNow ]
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 7 ] = [ None ]
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 7 ] = [ dSol, dNow, None ]
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    #Test ninth parameter ( tokenOwner )
    modArgs = initArgs[:]
    modArgs[ 8 ] = 'RS_SVC'
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 8 ] = [ 'RS_SVC' ]   
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )   
    modArgs[ 8 ] = 'eGGs'   
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 8 ] = [ 'eGGs' ]   
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs' ]   
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 8 ] = [ None ]   
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs', None ]   
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    #Test tenth parameter ( tokenExpiration )
    modArgs = initArgs[:]
    modArgs[ 9 ] = dSol
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )   
    modArgs[ 9 ] = [ dSol ]    
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )        
    modArgs[ 9 ] = dNow
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 9 ] = [ dNow ]
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 9 ] = [ dSol, dNow ]
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 9 ] = [ None ]
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )    
    modArgs[ 9 ] = [ dSol, dNow, None ]
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'Computing@LCG.CERN.ch'
    modArgs[ 1 ] = ''
    modArgs[ 2 ] = 'Active'    
    modArgs[ 3 ] = 'Reason'
    modArgs[ 8 ] = 'token'
    
    #This should work, but the DB is empty
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    modArgs[ 0 ] =  [ 'Computing@LCG.CERN.ch' ]
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] =  'Computing@LCG.CERN.ch'
    modArgs[ 1 ] =  ''
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = ''
    modArgs[ 2 ] = [ 'Active' ]
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = 'Active'
    modArgs[ 4 ] = 'eGGs'
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 4 ] = None
    modArgs[ 5 ] = 'eGGs'
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 5 ] = None
    modArgs[ 6 ] = 'eGGs'
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 6 ] = None
    modArgs[ 7 ] = 'eGGs'
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 7 ] = None
    modArgs[ 9 ] = 'eGGs'
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    now = datetime.now()
    modArgs = [ 'eGGs', '', 'Active', None, now, now, now, now, None, now]
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = 'Computing@LCG.CERN.ch'
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = ''
    modArgs[ 2 ] = 'eGGs'
    res = self.rsDB.setServiceScheduledStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
  def test_04_updateServiceStatus( self ):
    '''
    updateServiceStatus( self, serviceName, statusType, status, reason, dateCreated, 
                         dateEffective, dateEnd, lastCheckTime, tokenOwner, 
                         tokenExpiration )
    '''
    
    ins = inspect.getargspec( self.rsDB.updateServiceStatus.f )   
    self.assertEqual( ins.args, [ 'self', 'serviceName', 'statusType', 'status', 'reason', 'dateCreated', 
                                  'dateEffective', 'dateEnd', 'lastCheckTime', 'tokenOwner', 
                                  'tokenExpiration' ] )
    self.assertEqual( ins.varargs,  None)
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )
    
    dNow = datetime.now()
    
    initArgs = [ None, None, None, None, None, None, None, None, None, None ]
    
    dSol= datetime( 9999, 12, 11, 10, 9, 8 )
    
    res = self.rsDB.updateServiceStatus( *tuple( initArgs ) )
    self.assertNotEqual( res.has_key( 'Value' ), True )
    
    '''
      Param tests
    '''    
    #Test first parameter ( siteName )
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'Computing@LCG.CERN.ch'
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'Computing@LCG.CERN.ch' ]
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = 'eGGs'
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'eGGs' ]
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'Computing@LCG.CERN.ch', 'eGGs' ]
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ None ]
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 0 ] = [ 'Computing@LCG.CERN.ch', 'eGGs', None ]
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    #Test second parameter ( statusType )
    modArgs[ 1 ] = ''
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ '' ]
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ 'eGGs' ]
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ '', 'eGGs' ]
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ None ]
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 1 ] = [ '', 'eGGs', None ]
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )

    #Test third parameter ( status )
    modArgs[ 2 ] = 'Active'
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ 'Active' ]
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = 'eGGs'
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ 'eGGs' ]
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ 'Active', 'eGGs' ]
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ None ]
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 2 ] = [ 'Active', 'eGGs', None ]
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    #Test forth parameter ( reason )
    modArgs = initArgs[:]
    modArgs[ 3 ] = 'Init'
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 3 ] = [ 'Init' ]    
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 3 ] = 'eGGs'
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 3 ] = [ 'eGGs' ]
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 3 ] = [ 'Init', 'eGGs' ]
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 3 ] = [ None ]
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )         
    modArgs[ 3 ] = [ 'Init', 'eGGs', None ]
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )  

    #Test fifth parameter ( dateCreated )
    modArgs = initArgs[:]
    modArgs[ 4 ] = dSol
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 4 ] = [ dSol ]    
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )     
    modArgs[ 4 ] = dNow
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 4 ] = [ dNow ]
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 4 ] = [ dSol, dNow ]
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 4 ] = [ None ]
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 4 ] = [ dSol, dNow, None ]
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
            
    #Test sixth parameter ( dateEffective ) 
    modArgs = initArgs[:]
    modArgs[ 5 ] = dSol
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )    
    modArgs[ 5 ] = [ dSol ]    
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )         
    modArgs[ 5 ] = dNow
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )  
    modArgs[ 5 ] = [ dNow ]
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 5 ] = [ dSol, dNow ]
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )     
    modArgs[ 5 ] = [ None ]
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 5 ] = [ dSol, dNow, None ]
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )     

    #Test seventh parameter ( dateEnd )
    modArgs = initArgs[:]
    modArgs[ 6 ] = dSol
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )   
    modArgs[ 6 ] = [ dSol ]    
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )        
    modArgs[ 6 ] = dNow
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 6 ] = [ dNow ]
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 6 ] = [ dSol, dNow ]
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 6 ] = [ None ]
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 6 ] = [ dSol, dNow, None ]
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
            
    #Test eighth parameter ( lastCheckTime )
    modArgs = initArgs[:]
    modArgs[ 7 ] = dSol
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )   
    modArgs[ 7 ] = [ dSol ]    
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )        
    modArgs[ 7 ] = dNow
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False ) 
    modArgs[ 7 ] = [ dNow ]
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 7 ] = [ dSol, dNow ]
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 7 ] = [ None ]
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 7 ] = [ dSol, dNow, None ]
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    #Test ninth parameter ( tokenOwner )
    modArgs = initArgs[:]
    modArgs[ 8 ] = 'RS_SVC'
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 8 ] = [ 'RS_SVC' ]   
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )  
    modArgs[ 8 ] = 'eGGs'   
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 8 ] = [ 'eGGs' ]   
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs' ]   
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 8 ] = [ None ]   
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs', None ]   
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    
    #Test tenth parameter ( tokenExpiration )
    modArgs = initArgs[:]
    modArgs[ 9 ] = dSol
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )   
    modArgs[ 9 ] = [ dSol ]    
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )        
    modArgs[ 9 ] = dNow
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 9 ] = [ dNow ]
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 9 ] = [ dSol, dNow ]
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
    modArgs[ 9 ] = [ None ]
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )    
    modArgs[ 9 ] = [ dSol, dNow, None ]
    res = self.rsDB.updateServiceStatus( *tuple( modArgs ) )
    self.assertEqual( res[ 'OK' ], False )
       
    
    res = self.rsDB.updateServiceStatus( 'Computing@LCG.CERN.ch', '', 'Active', None, None,
                                       None,None,None,None,None )
    self.assertEqual( res[ 'OK' ], False )
    
    res = self.rsDB.updateServiceStatus( ['Computing@LCG.CERN.ch'], '', 'Active', None, None,
                                            None, None, None, None, None )
    self.assertEqual( res[ 'OK' ], False )
    res = self.rsDB.updateServiceStatus( 'Computing@LCG.CERN.ch', [''], 'Active', None, None,
                                            None, None, None, None, None )
    self.assertEqual( res[ 'OK' ], False )
    res = self.rsDB.updateServiceStatus( 'Computing@LCG.CERN.ch', '', ['Active'], None, None,
                                            None, None, None, None, None )
    self.assertEqual( res[ 'OK' ], False )
    res = self.rsDB.updateServiceStatus( 'Computing@LCG.CERN.ch', '', 'Active', None, 'eGGs',
                                            None, None, None, None, None )
    self.assertEqual( res[ 'OK' ], False )
    res = self.rsDB.updateServiceStatus( 'Computing@LCG.CERN.ch', '', 'Active', None, None,
                                            'eGGs', None, None, None, None )
    self.assertEqual( res[ 'OK' ], False )
    res = self.rsDB.updateServiceStatus( 'Computing@LCG.CERN.ch', '', 'Active', None, None,
                                            None, 'eGGs', None, None, None )
    self.assertEqual( res[ 'OK' ], False )
    res = self.rsDB.updateServiceStatus( 'Computing@LCG.CERN.ch', '', 'Active', None, None,
                                            None, None, 'eGGs', None, None )
    self.assertEqual( res[ 'OK' ], False )
    res = self.rsDB.updateServiceStatus( 'Computing@LCG.CERN.ch', '', 'Active', None, None,
                                            None, None, None, None, 'eGGs' )
    self.assertEqual( res[ 'OK' ], False )
    
    now = datetime.now()
    
    #Our DB is empty, some validations must break !
    res = self.rsDB.updateServiceStatus( 'Computing@LCG.CERN.ch', '', 'Active', None, now,
                                            now, now, now, None, now )
    self.assertEqual( res[ 'OK' ], False )
    res = self.rsDB.updateServiceStatus( 'eGGs', '', 'Active', None, now,
                                            now, now, now, None, now )
    self.assertEqual( res[ 'OK' ], False )
    res = self.rsDB.updateServiceStatus( 'Computing@LCG.CERN.ch', 'eGGs', 'Active', None, now,
                                            now, now, now, None, now )
    self.assertEqual( res[ 'OK' ], False )
    res = self.rsDB.updateServiceStatus( 'Computing@LCG.CERN.ch', '', 'eGGs', None, now,
                                       now, now, now, None, now )
    self.assertEqual( res[ 'OK' ], False )
        
  def test_05_getServices( self ):
    '''
    getServices( self, serviceName, serviceType, siteName, **kwargs )
    '''
    ins = inspect.getargspec( self.rsDB.getServices.f )   
    self.assertEqual( ins.args, [ 'self', 'serviceName', 'serviceType', 'siteName' ] )
    self.assertEqual( ins.varargs,  None)
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, None )
    
    initArgs = [ None, None, None ]
    
    res = self.rsDB.getServices( *tuple( initArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    
    '''
      Param tests
    '''    
    #Test first param ( serviceName )
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'Computing@LCG.CERN.ch'
    res = self.rsDB.getServices( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 0 ] = [ 'Computing@LCG.CNAF.it' ]
    res = self.rsDB.getServices( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = [ 'Computing@LCG.CERN.ch', 'Computing@LCG.CNAF.it' ]
    res = self.rsDB.getServices( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = 'eGGs'
    res = self.rsDB.getServices( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'eGGs' ]
    res = self.rsDB.getServices( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'Computing@LCG.CERN.ch', 'Computing@LCG.CNAF.it', 'eGGs' ]
    res = self.rsDB.getServices( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ None ]
    res = self.rsDB.getServices( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'Computing@LCG.CERN.ch', 'Computing@LCG.CNAF.it', 'eGGs', None ]
    res = self.rsDB.getServices( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 

    #Test second param ( serviceType )
    modArgs = initArgs[:]
    modArgs[ 1 ] = 'Computing'
    res = self.rsDB.getServices( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ 'Computing' ]
    res = self.rsDB.getServices( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ 'Storage', 'Computing' ]
    res = self.rsDB.getServices( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.getServices( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ 'eGGs' ]
    res = self.rsDB.getServices( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ 'Storage', 'Computing', 'eGGs' ]
    res = self.rsDB.getServices( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ None ]
    res = self.rsDB.getServices( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ 'Storage', 'Computing', 'eGGs', None ]
    res = self.rsDB.getServices( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )

    #Test third param ( siteName )
    modArgs = initArgs[:]
    modArgs[ 2 ] = 'LCG.CERN.ch'
    res = self.rsDB.getServices( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = [ 'LCG.CERN.ch' ]
    res = self.rsDB.getServices( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = [ 'LCG.CERN.ch', 'LCG.CNAF.it' ]
    res = self.rsDB.getServices( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = 'eGGs'
    res = self.rsDB.getServices( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = [ 'eGGs' ]
    res = self.rsDB.getServices( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = [ 'LCG.CERN.ch', 'LCG.CNAF.it', 'eGGs' ]
    res = self.rsDB.getServices( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = [ None ]
    res = self.rsDB.getServices( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = [ 'LCG.CERN.ch', 'LCG.CNAF.it', 'eGGs', None ]
    res = self.rsDB.getServices( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    
  def test_06_getServicesStatus( self ):
    '''
    getServicesStatus( self, serviceName, statusType, status, reason, dateCreated, 
                       dateEffective, dateEnd, lastCheckTime, tokenOwner, 
                       tokenExpiration, **kwargs )
    '''
    dNow = datetime.now()
    
    ins = inspect.getargspec( self.rsDB.getServicesStatus.f )   
    self.assertEqual( ins.args, [ 'self', 'serviceName', 'statusType', 'status', 
                                  'reason', 'dateCreated', 'dateEffective', 'dateEnd', 
                                  'lastCheckTime', 'tokenOwner', 'tokenExpiration' ] )
    self.assertEqual( ins.varargs,  None)
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, None )
    
    initArgs = [ None,None,None,None,None,None,None,None,None,None ]
    dSol     = datetime(9999, 12, 11, 10, 9, 8)
        
    res = self.rsDB.getServicesStatus( *tuple( initArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    
    '''
      Param tests
    '''    
    #Test first param ( siteName )
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'Computing@LCG.CERN.ch'
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'Computing@LCG.CERN.ch' ]    
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 0 ] = [ 'Computing@LCG.CERN.ch', 'Computing@LCG.CNAF.it' ]
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = 'LCG.eGGs.xy'
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'LCG.eGGs.xy' ]
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'Computing@LCG.CERN.ch', 'LCG.eGGs.xy' ]
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ None ]
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'Computing@LCG.CERN.ch', 'LCG.eGGs.xy', None ]
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )

    #Test second param ( statusType ) 
    modArgs = initArgs[:]
    modArgs[ 1 ] = ''
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ '' ]    
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ 'eGGs' ]
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ '', 'eGGs' ]
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ None ]
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ '', 'eGGs', None ]
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )

    #Test third parameter ( status )
    modArgs = initArgs[:]
    modArgs[ 2 ] = 'Active'
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = [ 'Active' ]    
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    modArgs[ 2 ] = 'Banned'
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 2 ] = [ 'Banned' ]
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = [ 'Active', 'Banned' ]
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = 'eGGs'
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = [ 'eGGs' ]
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )    
    modArgs[ 2 ] = [ 'eGGs', 'Banned' ]
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 2 ] = [ None ]
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )   
    modArgs[ 2 ] = [ 'eGGs', 'Banned', None ]
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )

    #Test forth parameter ( reason )
    modArgs = initArgs[:]
    modArgs[ 3 ] = 'Init'
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 3 ] = [ 'Init' ]    
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 3 ] = 'eGGs'
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 3 ] = [ 'eGGs' ]
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 3 ] = [ 'Init', 'eGGs' ]
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )         
    modArgs[ 3 ] = [ None ]
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 3 ] = [ 'Init', 'eGGs', None ]
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )

    #Test fifth parameter ( dateCreated )
    modArgs = initArgs[:]
    modArgs[ 4 ] = dSol
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 4 ] = [ dSol ]    
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 4 ] = dNow
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 4 ] = [ dNow ]
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 4 ] = [ dSol, dNow ]
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )    
    modArgs[ 4 ] = [ None ]
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 4 ] = [ dSol, dNow, None ]
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
            
    #Test sixth parameter ( dateEffective ) 
    modArgs = initArgs[:]
    modArgs[ 5 ] = dSol
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 5 ] = [ dSol ]    
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    modArgs[ 5 ] = dNow
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 5 ] = [ dNow ]
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 5 ] = [ dSol, dNow ]
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )   
    modArgs[ 5 ] = [ None ]
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 5 ] = [ dSol, dNow, None ]
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )

    #Test seventh parameter ( dateEnd )
    modArgs = initArgs[:]
    modArgs[ 6 ] = dSol
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 6 ] = [ dSol ]    
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )      
    modArgs[ 6 ] = dNow
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 6 ] = [ dNow ]
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 6 ] = [ dSol, dNow ]
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )   
    modArgs[ 6 ] = [ None ]
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 6 ] = [ dSol, dNow, None ]
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
                
    #Test eighth parameter ( lastCheckTime )
    modArgs = initArgs[:]
    modArgs[ 7 ] = dSol
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 7 ] = [ dSol ]    
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    modArgs[ 7 ] = dNow
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 7 ] = [ dNow ]
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 7 ] = [ dSol, dNow ]
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )    
    modArgs[ 7 ] = [ None ]
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 7 ] = [ dSol, dNow, None ]
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
        
    #Test ninth parameter ( tokenOwner )
    modArgs = initArgs[:]
    modArgs[ 8 ] = 'RS_SVC'
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'RS_SVC' ]   
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 8 ] = 'eGGs'   
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'eGGs' ]   
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs' ]   
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ None ]   
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs', None ]   
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    
    #Test tenth parameter ( tokenExpiration )
    modArgs = initArgs[:]
    modArgs[ 9 ] = dSol
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 9 ] = [ dSol ]    
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )      
    modArgs[ 9 ] = dNow
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 9 ] = [ dNow ]
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 9 ] = [ dSol, dNow ]
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 9 ] = [ None ]
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 9 ] = [ dSol, dNow, None ]
    res = self.rsDB.getServicesStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
            
             
  def test_07_getServicesHistory( self ):
    '''
    getServicesHistory( self, serviceName, statusType, status, reason, dateCreated, 
                        dateEffective, dateEnd, lastCheckTime, tokenOwner, 
                        tokenExpiration, **kwargs )
    '''
    dNow = datetime.now()
    
    ins = inspect.getargspec( self.rsDB.getServicesHistory.f )   
    self.assertEqual( ins.args, [ 'self', 'serviceName', 'statusType', 'status', 'reason', 'dateCreated', 
                                  'dateEffective', 'dateEnd', 'lastCheckTime', 'tokenOwner', 
                                  'tokenExpiration' ] )
    self.assertEqual( ins.varargs,  None)
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, None )
    
    initArgs = [ None,None,None,None,None,None,None,None,None,None ]
    dSol     = datetime(9999, 12, 11, 10, 9, 8)
        
    res = self.rsDB.getServicesHistory( *tuple( initArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    
    '''
      Param tests
    '''    
    #Test first param ( siteName )
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'Computing@LCG.CERN.ch'
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = [ 'Computing@LCG.CERN.ch' ]    
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )          
    modArgs[ 0 ] = [ 'Computing@LCG.CERN.ch', 'Computing@LCG.CNAF.it' ]
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = 'LCG.eGGs.xy'
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = [ 'LCG.eGGs.xy' ]
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = [ 'Computing@LCG.CERN.ch', 'LCG.eGGs.xy' ]
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = [ None ]
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = [ 'Computing@LCG.CERN.ch', 'LCG.eGGs.xy', None ]
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  

    #Test second param ( statusType ) 
    modArgs = initArgs[:]
    modArgs[ 1 ] = ''
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 1 ] = [ '' ]    
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 1 ] = [ 'eGGs' ]
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 1 ] = [ '', 'eGGs' ]
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 1 ] = [ None ]
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 1 ] = [ '', 'eGGs', None ]
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  

    #Test third parameter ( status )
    modArgs = initArgs[:]
    modArgs[ 2 ] = 'Active'
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 2 ] = [ 'Active' ]    
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    modArgs[ 2 ] = 'Banned'
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 2 ] = [ 'Banned' ]
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 2 ] = [ 'Active', 'Banned' ]
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 2 ] = 'eGGs'
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 2 ] = [ 'eGGs' ]
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )     
    modArgs[ 2 ] = [ 'eGGs', 'Banned' ]
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )         
    modArgs[ 2 ] = [ None ]
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    modArgs[ 2 ] = [ 'eGGs', 'Banned', None ]
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  

    #Test forth parameter ( reason )
    modArgs = initArgs[:]
    modArgs[ 3 ] = 'Init'
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 3 ] = [ 'Init' ]    
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )         
    modArgs[ 3 ] = 'eGGs'
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 3 ] = [ 'eGGs' ]
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 3 ] = [ 'Init', 'eGGs' ]
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )            
    modArgs[ 3 ] = [ None ]
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 3 ] = [ 'Init', 'eGGs', None ]
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  

    #Test fifth parameter ( dateCreated )
    modArgs = initArgs[:]
    modArgs[ 4 ] = dSol
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )     
    modArgs[ 4 ] = [ dSol ]    
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 4 ] = dNow
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 4 ] = [ dNow ]
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 4 ] = [ dSol, dNow ]
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )    
    modArgs[ 4 ] = [ None ]
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )  
    modArgs[ 4 ] = [ dSol, dNow, None ]
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )  
            
    #Test sixth parameter ( dateEffective ) 
    modArgs = initArgs[:]
    modArgs[ 5 ] = dSol
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 5 ] = [ dSol ]    
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )      
    modArgs[ 5 ] = dNow
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 5 ] = [ dNow ]
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 5 ] = [ dSol, dNow ]
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )    
    modArgs[ 5 ] = [ None ]
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 5 ] = [ dSol, dNow, None ]
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )

    #Test seventh parameter ( dateEnd )
    modArgs = initArgs[:]
    modArgs[ 6 ] = dSol
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 6 ] = [ dSol ]    
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 6 ] = dNow
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 6 ] = [ dNow ]
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 6 ] = [ dSol, dNow ]
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )     
    modArgs[ 6 ] = [ None ]
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False ) 
    modArgs[ 6 ] = [ dSol, dNow, None ]
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False ) 
                
    #Test eighth parameter ( lastCheckTime )
    modArgs = initArgs[:]
    modArgs[ 7 ] = dSol
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )   
    modArgs[ 7 ] = [ dSol ]    
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    modArgs[ 7 ] = dNow
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 7 ] = [ dNow ]
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 7 ] = [ dSol, dNow ]
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 7 ] = [ None ]
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 7 ] = [ dSol, dNow, None ]
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
        
    #Test ninth parameter ( tokenOwner )
    modArgs = initArgs[:]
    modArgs[ 8 ] = 'RS_SVC'
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'RS_SVC' ]   
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = 'eGGs'   
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'eGGs' ]   
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs' ]   
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ None ]   
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs', None ]   
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    
    #Test tenth parameter ( tokenExpiration )
    modArgs = initArgs[:]
    modArgs[ 9 ] = dSol
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 9 ] = [ dSol ]    
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )      
    modArgs[ 9 ] = dNow
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 9 ] = [ dNow ]
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 9 ] = [ dSol, dNow ]
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )    
    modArgs[ 9 ] = [ None ]
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 9 ] = [ dSol, dNow, None ]
    res = self.rsDB.getServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )

  def test_08_getServicesScheduledStatus( self ):
    '''
    getServicesScheduledStatus( self, serviceName, statusType, status, reason, dateCreated, 
                                dateEffective, dateEnd, lastCheckTime, tokenOwner, 
                                tokenExpiration, **kwargs )
    '''
    dNow = datetime.now()
    
    ins = inspect.getargspec( self.rsDB.getServicesScheduledStatus.f )   
    self.assertEqual( ins.args, [ 'self', 'serviceName', 'statusType', 'status', 'reason', 'dateCreated', 
                                  'dateEffective', 'dateEnd', 'lastCheckTime', 'tokenOwner', 
                                  'tokenExpiration' ] )
    self.assertEqual( ins.varargs,  None)
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, None )
    
    initArgs = [ None,None,None,None,None,None,None,None,None,None ]
    dSol     = datetime(9999, 12, 11, 10, 9, 8)
        
    res = self.rsDB.getServicesScheduledStatus( *tuple( initArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    
    '''
      Param tests
    '''    
    #Test first param ( siteName )
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'Computing@LCG.CERN.ch'
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = [ 'Computing@LCG.CERN.ch' ]    
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )          
    modArgs[ 0 ] = [ 'Computing@LCG.CERN.ch', 'Computing@LCG.CNAF.it' ]
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = 'LCG.eGGs.xy'
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = [ 'LCG.eGGs.xy' ]
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = [ 'Computing@LCG.CERN.ch', 'LCG.eGGs.xy' ]
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = [ None ]
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = [ 'Computing@LCG.CERN.ch', 'LCG.eGGs.xy', None ]
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  

    #Test second param ( statusType ) 
    modArgs = initArgs[:]
    modArgs[ 1 ] = ''
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 1 ] = [ '' ]    
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 1 ] = [ 'eGGs' ]
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 1 ] = [ '', 'eGGs' ]
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 1 ] = [ None ]
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 1 ] = [ '', 'eGGs', None ]
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  

    #Test third parameter ( status )
    modArgs = initArgs[:]
    modArgs[ 2 ] = 'Active'
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 2 ] = [ 'Active' ]    
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    modArgs[ 2 ] = 'Banned'
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 2 ] = [ 'Banned' ]
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 2 ] = [ 'Active', 'Banned' ]
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 2 ] = 'eGGs'
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 2 ] = [ 'eGGs' ]
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )     
    modArgs[ 2 ] = [ 'eGGs', 'Banned' ]
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )         
    modArgs[ 2 ] = [ None ]
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    modArgs[ 2 ] = [ 'eGGs', 'Banned', None ]
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  

    #Test forth parameter ( reason )
    modArgs = initArgs[:]
    modArgs[ 3 ] = 'Init'
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 3 ] = [ 'Init' ]    
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )         
    modArgs[ 3 ] = 'eGGs'
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 3 ] = [ 'eGGs' ]
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 3 ] = [ 'Init', 'eGGs' ]
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )            
    modArgs[ 3 ] = [ None ]
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 3 ] = [ 'Init', 'eGGs', None ]
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  

    #Test fifth parameter ( dateCreated )
    modArgs = initArgs[:]
    modArgs[ 4 ] = dSol
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )     
    modArgs[ 4 ] = [ dSol ]    
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 4 ] = dNow
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 4 ] = [ dNow ]
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 4 ] = [ dSol, dNow ]
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )    
    modArgs[ 4 ] = [ None ]
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )  
    modArgs[ 4 ] = [ dSol, dNow, None ]
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )  
            
    #Test sixth parameter ( dateEffective ) 
    modArgs = initArgs[:]
    modArgs[ 5 ] = dSol
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 5 ] = [ dSol ]    
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )      
    modArgs[ 5 ] = dNow
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 5 ] = [ dNow ]
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 5 ] = [ dSol, dNow ]
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )    
    modArgs[ 5 ] = [ None ]
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 5 ] = [ dSol, dNow, None ]
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )

    #Test seventh parameter ( dateEnd )
    modArgs = initArgs[:]
    modArgs[ 6 ] = dSol
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 6 ] = [ dSol ]    
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 6 ] = dNow
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 6 ] = [ dNow ]
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 6 ] = [ dSol, dNow ]
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )     
    modArgs[ 6 ] = [ None ]
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False ) 
    modArgs[ 6 ] = [ dSol, dNow, None ]
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False ) 
                
    #Test eighth parameter ( lastCheckTime )
    modArgs = initArgs[:]
    modArgs[ 7 ] = dSol
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )   
    modArgs[ 7 ] = [ dSol ]    
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    modArgs[ 7 ] = dNow
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 7 ] = [ dNow ]
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 7 ] = [ dSol, dNow ]
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 7 ] = [ None ]
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 7 ] = [ dSol, dNow, None ]
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
        
    #Test ninth parameter ( tokenOwner )
    modArgs = initArgs[:]
    modArgs[ 8 ] = 'RS_SVC'
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'RS_SVC' ]   
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = 'eGGs'   
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'eGGs' ]   
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs' ]   
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ None ]   
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs', None ]   
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    
    #Test tenth parameter ( tokenExpiration )
    modArgs = initArgs[:]
    modArgs[ 9 ] = dSol
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 9 ] = [ dSol ]    
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )      
    modArgs[ 9 ] = dNow
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 9 ] = [ dNow ]
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 9 ] = [ dSol, dNow ]
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )    
    modArgs[ 9 ] = [ None ]
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 9 ] = [ dSol, dNow, None ]
    res = self.rsDB.getServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )

  def test_09_getServicesPresent( self ):
    '''
    def getServicesPresent( self, serviceName, siteName, siteType, serviceType, 
                          statusType, status, dateEffective, reason, lastCheckTime, 
                          tokenOwner, tokenExpiration, formerStatus, **kwargs )                 
    '''
    dNow = datetime.now()
    
    ins = inspect.getargspec( self.rsDB.getServicesPresent.f )   
    self.assertEqual( ins.args, [ 'self', 'serviceName', 'siteName', 'siteType', 'serviceType', 
                     'statusType', 'status', 'dateEffective', 'reason', 'lastCheckTime', 
                     'tokenOwner', 'tokenExpiration', 'formerStatus' ] )
    self.assertEqual( ins.varargs,  None)
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, None )

    
    initArgs = [ None,None,None,None,None,None,None,None,None,None, None, None ]
    dSol     = datetime(9999, 12, 11, 10, 9, 8)

    res = self.rsDB.getServicesPresent( *tuple( initArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    
    '''
      Param tests
    '''    
    #Test first param ( siteName )
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'Computing@LCG.CERN.ch'
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = [ 'Computing@LCG.CERN.ch' ]    
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )          
    modArgs[ 0 ] = [ 'Computing@LCG.CERN.ch', 'Computing@LCG.CNAF.it' ]
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = 'LCG.eGGs.xy'
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = [ 'LCG.eGGs.xy' ]
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = [ 'Computing@LCG.CERN.ch', 'LCG.eGGs.xy' ]
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = [ None ]
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 0 ] = [ 'Computing@LCG.CERN.ch', 'LCG.eGGs.xy', None ]
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  

    #Test second param ( siteType )
    modArgs = initArgs[:]
    modArgs[ 1 ] = 'LCG.CERN.ch'
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 1 ] = [ 'LCG.CERN.ch' ]    
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )          
    modArgs[ 1 ] = [ 'LCG.CERN.ch', 'LCG.CNAF.it' ]
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 1 ] = 'LCG.eGGs.xy'
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 1 ] = [ 'LCG.eGGs.xy' ]
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 1 ] = [ 'LCG.CERN.ch', 'LCG.eGGs.xy' ]
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 1 ] = [ None ]
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 1 ] = [ 'LCG.CERN.ch', 'LCG.eGGs.xy', None ]
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  

    #Test third param ( gridSiteName )
    modArgs = initArgs[:]
    modArgs[ 2 ] = 'T0'
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 2 ] = [ 'T0' ]    
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )          
    modArgs[ 2 ] = [ 'T0', 'T1' ]
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 2 ] = 'LCG.eGGs.xy'
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 2 ] = [ 'LCG.eGGs.xy' ]
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 2 ] = [ 'T0', 'LCG.eGGs.xy' ]
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 2 ] = [ None ]
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 2 ] = [ 'T0', 'LCG.eGGs.xy', None ]
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  

    #Test forth param ( gridTier )
    modArgs = initArgs[:]
    modArgs[ 3 ] = 'Computing'
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 3 ] = [ 'Computing' ]    
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )          
    modArgs[ 3 ] = [ 'Computing', 'Storage' ]
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 3 ] = 'LCG.eGGs.xy'
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 3 ] = [ 'LCG.eGGs.xy' ]
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 3 ] = [ 'Computing', 'LCG.eGGs.xy' ]
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 3 ] = [ None ]
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 3 ] = [ 'Computing', 'LCG.eGGs.xy', None ]
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  

    #Test fifth param ( statusType ) 
    modArgs = initArgs[:]
    modArgs[ 4 ] = ''
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 4 ] = [ '' ]    
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 4 ] = 'eGGs'
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 4 ] = [ 'eGGs' ]
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'Value' ], [] )
    modArgs[ 4 ] = [ '', 'eGGs' ]
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 4 ] = [ None ]
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 4 ] = [ '', 'eGGs', None ]
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  

    #Test sixth parameter ( status )
    modArgs = initArgs[:]
    modArgs[ 5 ] = 'Active'
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 5 ] = [ 'Active' ]    
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    modArgs[ 5 ] = 'Banned'
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 5 ] = [ 'Banned' ]
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 5 ] = [ 'Active', 'Banned' ]
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 5 ] = 'eGGs'
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 5 ] = [ 'eGGs' ]
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )     
    modArgs[ 5 ] = [ 'eGGs', 'Banned' ]
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )         
    modArgs[ 5 ] = [ None ]
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    modArgs[ 5 ] = [ 'eGGs', 'Banned', None ]
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  

    #Test seventh parameter ( dateEffective ) 
    modArgs = initArgs[:]
    modArgs[ 6 ] = dSol
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 6 ] = [ dSol ]    
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )      
    modArgs[ 6 ] = dNow
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 6 ] = [ dNow ]
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 6 ] = [ dSol, dNow ]
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )    
    modArgs[ 6 ] = [ None ]
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 6 ] = [ dSol, dNow, None ]
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )

    #Test eighth parameter ( reason )
    modArgs = initArgs[:]
    modArgs[ 7 ] = 'Init'
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 7 ] = [ 'Init' ]    
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )         
    modArgs[ 7 ] = 'eGGs'
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 7 ] = [ 'eGGs' ]
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 7 ] = [ 'Init', 'eGGs' ]
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )            
    modArgs[ 7 ] = [ None ]
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 7 ] = [ 'Init', 'eGGs', None ]
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
                
    #Test ninth parameter ( lastCheckTime )
    modArgs = initArgs[:]
    modArgs[ 8 ] = dSol
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )   
    modArgs[ 8 ] = [ dSol ]    
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    modArgs[ 8 ] = dNow
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 8 ] = [ dNow ]
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ dSol, dNow ]
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ None ]
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 8 ] = [ dSol, dNow, None ]
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
        
    #Test tenth parameter ( tokenOwner )
    modArgs = initArgs[:]
    modArgs[ 9 ] = 'RS_SVC'
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 9 ] = [ 'RS_SVC' ]   
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 9 ] = 'eGGs'   
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 9 ] = [ 'eGGs' ]   
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 9 ] = [ 'RS_SVC','eGGs' ]   
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 9 ] = [ None ]   
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 9 ] = [ 'RS_SVC','eGGs', None ]   
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    
    #Test eleventh parameter ( tokenExpiration )
    modArgs = initArgs[:]
    modArgs[ 10 ] = dSol
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 10 ] = [ dSol ]    
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )      
    modArgs[ 10 ] = dNow
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 10 ] = [ dNow ]
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 10 ] = [ dSol, dNow ]
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )    
    modArgs[ 10 ] = [ None ]
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 10 ] = [ dSol, dNow, None ]
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )

    #Test 12th parameter ( former status )
    modArgs = initArgs[:]
    modArgs[ 11 ] = 'Active'
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 11 ] = [ 'Active' ]    
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    modArgs[ 11 ] = 'Banned'
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 11 ] = [ 'Banned' ]
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 11 ] = [ 'Active', 'Banned' ]
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 11 ] = 'eGGs'
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 11 ] = [ 'eGGs' ]
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )     
    modArgs[ 11 ] = [ 'eGGs', 'Banned' ]
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )         
    modArgs[ 11 ] = [ None ]
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    modArgs[ 11 ] = [ 'eGGs', 'Banned', None ]
    res = self.rsDB.getServicesPresent( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  

  def test_10_deleteServicesScheduledStatus( self ):
    '''
    deleteServicesScheduledStatus( self, serviceName, statusType, status, reason, 
                                   dateCreated, dateEffective, dateEnd, 
                                   lastCheckTime, tokenOwner, tokenExpiration, **kwargs)
    '''
    dNow = datetime.now()
    
    ins = inspect.getargspec( self.rsDB.deleteServicesScheduledStatus.f )   
    self.assertEqual( ins.args, [ 'self', 'serviceName', 'statusType', 'status', 'reason', 
                                  'dateCreated', 'dateEffective', 'dateEnd', 
                                  'lastCheckTime', 'tokenOwner', 'tokenExpiration' ] )
    self.assertEqual( ins.varargs,  None)
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, None )        
       
    dSol     = datetime(9999, 12, 11, 10, 9, 8)      
    initArgs = [ None,None,None,None,None,None,None,None,None,None ]
   
       
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( initArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    
    '''
      Param tests
    '''    
    #Test first param ( siteName )
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'Computing@LCG.CERN.ch'
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'Computing@LCG.CERN.ch' ]    
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 0 ] = [ 'Computing@LCG.CERN.ch', 'Computing@LCG.CNAF.it' ]
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = 'LCG.eGGs.xy'
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'LCG.eGGs.xy' ]
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'Computing@LCG.CERN.ch', 'LCG.eGGs.xy' ]
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ None ]
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'Computing@LCG.CERN.ch', 'LCG.eGGs.xy', None ]
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )

    #Test second param ( statusType ) 
    modArgs = initArgs[:]
    modArgs[ 1 ] = ''
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ '' ]    
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 1 ] = [ 'eGGs' ]
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ '', 'eGGs' ]
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 1 ] = [ None ]
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ '', 'eGGs', None ]
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )

    #Test third parameter ( status )
    modArgs = initArgs[:]
    modArgs[ 2 ] = 'Active'
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = [ 'Active' ]    
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )         
    modArgs[ 2 ] = 'Banned'
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 2 ] = [ 'Banned' ]
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = [ 'Active', 'Banned' ]
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 2 ] = 'eGGs'
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = [ 'eGGs' ]
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )     
    modArgs[ 2 ] = [ 'eGGs', 'Banned' ]
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )      
    modArgs[ 2 ] = [ None ]
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 2 ] = [ 'eGGs', 'Banned', None ]
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )

    #Test forth parameter ( reason )
    modArgs = initArgs[:]
    modArgs[ 3 ] = 'Init'
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 3 ] = [ 'Init' ]    
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )     
    modArgs[ 3 ] = 'eGGs'
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 3 ] = [ 'eGGs' ]
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 3 ] = [ 'Init', 'eGGs' ]
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 3 ] = [ None ]
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 3 ] = [ 'Init', 'eGGs', None ]
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )

    #Test fifth parameter ( dateCreated )
    modArgs = initArgs[:]
    modArgs[ 4 ] = dSol
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 4 ] = [ dSol ]    
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    modArgs[ 4 ] = dNow
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 4 ] = [ dNow ]
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 4 ] = [ dSol, dNow ]
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )     
    modArgs[ 4 ] = [ None ]
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 4 ] = [ dSol, dNow, None ]
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
            
    #Test sixth parameter ( dateEffective ) 
    modArgs = initArgs[:]
    modArgs[ 5 ] = dSol
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 5 ] = [ dSol ]    
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 5 ] = dNow
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 5 ] = [ dNow ]
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 5 ] = [ dSol, dNow ]
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )   
    modArgs[ 5 ] = [ None ]
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 5 ] = [ dSol, dNow, None ]
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False ) 

    #Test seventh parameter ( dateEnd )
    modArgs = initArgs[:]
    modArgs[ 6 ] = dSol
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )    
    modArgs[ 6 ] = [ dSol ]    
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )      
    modArgs[ 6 ] = dNow
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 6 ] = [ dNow ]
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 6 ] = [ dSol, dNow ]
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )     
    modArgs[ 6 ] = [ None ]
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 6 ] = [ dSol, dNow, None ]
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
                
    #Test eighth parameter ( lastCheckTime )
    modArgs = initArgs[:]
    modArgs[ 7 ] = dSol
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )   
    modArgs[ 7 ] = [ dSol ]    
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 7 ] = dNow
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 7 ] = [ dNow ]
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 7 ] = [ dSol, dNow ]
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )   
    modArgs[ 7 ] = [ None ]
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 7 ] = [ dSol, dNow, None ]
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
        
    #Test ninth parameter ( tokenOwner )
    modArgs = initArgs[:]
    modArgs[ 8 ] = 'RS_SVC'
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'RS_SVC' ]   
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 8 ] = 'eGGs'   
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 8 ] = [ 'eGGs' ]   
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs' ]   
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ None ]   
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs', None ]   
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    
    #Test tenth parameter ( tokenExpiration )
    modArgs = initArgs[:]
    modArgs[ 9 ] = dSol
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )   
    modArgs[ 9 ] = [ dSol ]    
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )      
    modArgs[ 9 ] = dNow
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 9 ] = [ dNow ]
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 9 ] = [ dSol, dNow ]
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )   
    modArgs[ 9 ] = [ None ]
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 9 ] = [ dSol, dNow, None ]
    res = self.rsDB.deleteServicesScheduledStatus( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )

  def test_11_deleteServicesHistory( self ):
    '''
    deleteServicesHistory( self, serviceName, statusType, status, reason, 
                           dateCreated, dateEffective, dateEnd, 
                           lastCheckTime, tokenOwner, tokenExpiration, kwargs )
    '''
    dNow = datetime.now()
    
    ins = inspect.getargspec( self.rsDB.deleteServicesHistory.f )   
    self.assertEqual( ins.args, [ 'self', 'serviceName', 'statusType', 'status', 'reason', 
                                  'dateCreated', 'dateEffective', 'dateEnd', 
                                  'lastCheckTime', 'tokenOwner', 'tokenExpiration' ] )
    self.assertEqual( ins.varargs,  None)
    self.assertEqual( ins.keywords, 'kwargs' )
    self.assertEqual( ins.defaults, None )        
       
    dSol     = datetime(9999, 12, 11, 10, 9, 8)      
    initArgs = [ None,None,None,None,None,None,None,None,None,None ]
   
       
    res = self.rsDB.deleteServicesHistory( *tuple( initArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    
    '''
      Param tests
    '''    
    #Test first param ( siteName )
    modArgs = initArgs[:]
    modArgs[ 0 ] = 'Computing@LCG.CERN.ch'
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'Computing@LCG.CERN.ch' ]    
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 0 ] = [ 'Computing@LCG.CERN.ch', 'Computing@LCG.CNAF.it' ]
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = 'LCG.eGGs.xy'
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'LCG.eGGs.xy' ]
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'Computing@LCG.CERN.ch', 'LCG.eGGs.xy' ]
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ None ]
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 0 ] = [ 'Computing@LCG.CERN.ch', 'LCG.eGGs.xy', None ]
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )

    #Test second param ( statusType ) 
    modArgs = initArgs[:]
    modArgs[ 1 ] = ''
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ '' ]    
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 1 ] = 'eGGs'
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 1 ] = [ 'eGGs' ]
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ '', 'eGGs' ]
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 1 ] = [ None ]
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 1 ] = [ '', 'eGGs', None ]
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )

    #Test third parameter ( status )
    modArgs = initArgs[:]
    modArgs[ 2 ] = 'Active'
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = [ 'Active' ]    
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )         
    modArgs[ 2 ] = 'Banned'
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 2 ] = [ 'Banned' ]
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = [ 'Active', 'Banned' ]
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 2 ] = 'eGGs'
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 2 ] = [ 'eGGs' ]
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )     
    modArgs[ 2 ] = [ 'eGGs', 'Banned' ]
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )      
    modArgs[ 2 ] = [ None ]
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 2 ] = [ 'eGGs', 'Banned', None ]
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )

    #Test forth parameter ( reason )
    modArgs = initArgs[:]
    modArgs[ 3 ] = 'Init'
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 3 ] = [ 'Init' ]    
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )     
    modArgs[ 3 ] = 'eGGs'
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 3 ] = [ 'eGGs' ]
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 3 ] = [ 'Init', 'eGGs' ]
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 3 ] = [ None ]
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 3 ] = [ 'Init', 'eGGs', None ]
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )

    #Test fifth parameter ( dateCreated )
    modArgs = initArgs[:]
    modArgs[ 4 ] = dSol
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 4 ] = [ dSol ]    
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )       
    modArgs[ 4 ] = dNow
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 4 ] = [ dNow ]
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 4 ] = [ dSol, dNow ]
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )     
    modArgs[ 4 ] = [ None ]
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 4 ] = [ dSol, dNow, None ]
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
            
    #Test sixth parameter ( dateEffective ) 
    modArgs = initArgs[:]
    modArgs[ 5 ] = dSol
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 5 ] = [ dSol ]    
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 5 ] = dNow
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 5 ] = [ dNow ]
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 5 ] = [ dSol, dNow ]
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )   
    modArgs[ 5 ] = [ None ]
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 5 ] = [ dSol, dNow, None ]
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False ) 

    #Test seventh parameter ( dateEnd )
    modArgs = initArgs[:]
    modArgs[ 6 ] = dSol
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )    
    modArgs[ 6 ] = [ dSol ]    
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )      
    modArgs[ 6 ] = dNow
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 6 ] = [ dNow ]
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 6 ] = [ dSol, dNow ]
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )     
    modArgs[ 6 ] = [ None ]
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 6 ] = [ dSol, dNow, None ]
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
                
    #Test eighth parameter ( lastCheckTime )
    modArgs = initArgs[:]
    modArgs[ 7 ] = dSol
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )   
    modArgs[ 7 ] = [ dSol ]    
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )        
    modArgs[ 7 ] = dNow
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 7 ] = [ dNow ]
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 7 ] = [ dSol, dNow ]
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )   
    modArgs[ 7 ] = [ None ]
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 7 ] = [ dSol, dNow, None ]
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
        
    #Test ninth parameter ( tokenOwner )
    modArgs = initArgs[:]
    modArgs[ 8 ] = 'RS_SVC'
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'RS_SVC' ]   
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )  
    modArgs[ 8 ] = 'eGGs'   
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True ) 
    modArgs[ 8 ] = [ 'eGGs' ]   
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs' ]   
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ None ]   
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 8 ] = [ 'RS_SVC','eGGs', None ]   
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    
    #Test tenth parameter ( tokenExpiration )
    modArgs = initArgs[:]
    modArgs[ 9 ] = dSol
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )   
    modArgs[ 9 ] = [ dSol ]    
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )      
    modArgs[ 9 ] = dNow
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 9 ] = [ dNow ]
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )
    modArgs[ 9 ] = [ dSol, dNow ]
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], True )   
    modArgs[ 9 ] = [ None ]
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )
    modArgs[ 9 ] = [ dSol, dNow, None ]
    res = self.rsDB.deleteServicesHistory( *tuple( modArgs ) )
    self.assertEquals( res[ 'OK' ], False )

  def test_12_deleteServices( self ):
    '''
    deleteServices( self, serviceName )
    '''

    ins = inspect.getargspec( self.rsDB.deleteServices.f )   
    self.assertEqual( ins.args, [ 'self', 'serviceName' ] )
    self.assertEqual( ins.varargs,  None)
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )       

    res = self.rsDB.deleteServices()
    self.assertNotEqual( res.has_key( 'Value' ), True )
    res = self.rsDB.deleteServices( 'eGGs', None )
    self.assertNotEqual( res.has_key( 'Value' ), True )

    #Test first param
    res = self.rsDB.deleteServices( 'Computing@LCG.CERN.ch' )
    self.assertEquals( res[ 'OK' ], True )
    res = self.rsDB.deleteServices( [ 'Computing@LCG.CERN.ch' ] )
    self.assertEquals( res[ 'OK' ], True )
    res = self.rsDB.deleteServices( 'eGGs' )
    self.assertEquals( res[ 'OK' ], True )
    res = self.rsDB.deleteServices( [ 'eGGs' ] )
    self.assertEquals( res[ 'OK' ], True )
    res = self.rsDB.deleteServices( None )
    self.assertEquals( res[ 'OK' ], False )
    res = self.rsDB.deleteServices( [ None ] )
    self.assertEquals( res[ 'OK' ], False )
    res = self.rsDB.deleteServices( [ 'Computing@LCG.CERN.ch', None ] )
    self.assertEquals( res[ 'OK' ], False )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF