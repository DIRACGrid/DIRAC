import unittest
import inspect

class TestCase_Description_TransferQualityByDestSplitted_Command( unittest.TestCase ):

  me = 'TransferQualityByDestSplitted_Command'

  def test_doCommand_definition( self ):
    
    ins = inspect.getargspec( self.clients[ self.me ].doCommand )   
    self.assertEqual( ins.args, [ 'self', 'sources', 'SEs' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, ( None, None ) )

  def test_APIs_description( self ):
    
    __APIs__ = [ 'ResourceStatusClient', 'ReportsClient', 'ReportGenerator' ]
    self.assertEquals( self.clients[ self.me ].__APIs__, __APIs__ )

################################################################################

class TestCase_Description_TransferQualityByDestSplittedSite_Command( unittest.TestCase ):

  me = 'TransferQualityByDestSplittedSite_Command'

  def test_doCommand_definition( self ):
    
    ins = inspect.getargspec( self.clients[ self.me ].doCommand )   
    self.assertEqual( ins.args, [ 'self', 'sources', 'SEs' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, ( None, None ) )

  def test_APIs_description( self ):
    
    __APIs__ = [ 'ResourceStatusClient', 'ReportsClient', 'ReportGenerator' ]
    self.assertEquals( self.clients[ self.me ].__APIs__, __APIs__ )

################################################################################

class TestCase_Description_TransferQualityBySourceSplittedSite_Command( unittest.TestCase ):

  me = 'TransferQualityBySourceSplittedSite_Command'

  def test_doCommand_definition( self ):
    
    ins = inspect.getargspec( self.clients[ self.me ].doCommand )   
    self.assertEqual( ins.args, [ 'self', 'sources', 'SEs' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, ( None, None ) )

  def test_APIs_description( self ):
    
    __APIs__ = [ 'ResourceStatusClient', 'ReportsClient', 'ReportGenerator' ]
    self.assertEquals( self.clients[ self.me ].__APIs__, __APIs__ )

################################################################################

class TestCase_Description_FailedTransfersBySourceSplitted_Command( unittest.TestCase ):

  me = 'FailedTransfersBySourceSplitted_Command'

  def test_doCommand_definition( self ):
    
    ins = inspect.getargspec( self.clients[ self.me ].doCommand )   
    self.assertEqual( ins.args, [ 'self', 'sources', 'SEs' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, ( None, None ) )

  def test_APIs_description( self ):
    
    __APIs__ = [ 'ResourceStatusClient', 'ReportsClient', 'ReportGenerator' ]
    self.assertEquals( self.clients[ self.me ].__APIs__, __APIs__ )

################################################################################

class TestCase_Description_SuccessfullJobsBySiteSplitted_Command( unittest.TestCase ):

  me = 'SuccessfullJobsBySiteSplitted_Command'

  def test_doCommand_definition( self ):
    
    ins = inspect.getargspec( self.clients[ self.me ].doCommand )   
    self.assertEqual( ins.args, [ 'self', 'sites' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, ( None, ) )

  def test_APIs_description( self ):
    
    __APIs__ = [ 'ResourceStatusClient', 'ReportsClient', 'ReportGenerator' ]
    self.assertEquals( self.clients[ self.me ].__APIs__, __APIs__ )

################################################################################

class TestCase_Description_FailedJobsBySiteSplitted_Command( unittest.TestCase ):

  me = 'FailedJobsBySiteSplitted_Command'

  def test_doCommand_definition( self ):
    
    ins = inspect.getargspec( self.clients[ self.me ].doCommand )   
    self.assertEqual( ins.args, [ 'self', 'sites' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, ( None, ) )

  def test_APIs_description( self ):
    
    __APIs__ = [ 'ResourceStatusClient', 'ReportsClient', 'ReportGenerator' ]
    self.assertEquals( self.clients[ self.me ].__APIs__, __APIs__ )        

################################################################################

class TestCase_Description_SuccessfullPilotsBySiteSplitted_Command( unittest.TestCase ):

  me = 'SuccessfullPilotsBySiteSplitted_Command'

  def test_doCommand_definition( self ):
    
    ins = inspect.getargspec( self.clients[ self.me ].doCommand )   
    self.assertEqual( ins.args, [ 'self', 'sites' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, ( None, ) )

  def test_APIs_description( self ):
    
    __APIs__ = [ 'ResourceStatusClient', 'ReportsClient', 'ReportGenerator' ]
    self.assertEquals( self.clients[ self.me ].__APIs__, __APIs__ )    

################################################################################

class TestCase_Description_FailedPilotsBySiteSplitted_Command( unittest.TestCase ):

  me = 'FailedPilotsBySiteSplitted_Command'

  def test_doCommand_definition( self ):
    
    ins = inspect.getargspec( self.clients[ self.me ].doCommand )   
    self.assertEqual( ins.args, [ 'self', 'sites' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, ( None, ) )

  def test_APIs_description( self ):
    
    __APIs__ = [ 'ResourceStatusClient', 'ReportsClient', 'ReportGenerator' ]
    self.assertEquals( self.clients[ self.me ].__APIs__, __APIs__ )    

################################################################################

class TestCase_Description_SuccessfullPilotsByCESplitted_Command( unittest.TestCase ):

  me = 'SuccessfullPilotsByCESplitted_Command'

  def test_doCommand_definition( self ):
    
    ins = inspect.getargspec( self.clients[ self.me ].doCommand )   
    self.assertEqual( ins.args, [ 'self', 'CEs' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, ( None, ) )

  def test_APIs_description( self ):
    
    __APIs__ = [ 'ResourceStatusClient', 'ReportsClient', 'ReportGenerator' ]
    self.assertEquals( self.clients[ self.me ].__APIs__, __APIs__ )   

################################################################################

class TestCase_Description_FailedPilotsByCESplitted_Command( unittest.TestCase ):

  me = 'FailedPilotsByCESplitted_Command'

  def test_doCommand_definition( self ):
    
    ins = inspect.getargspec( self.clients[ self.me ].doCommand )   
    self.assertEqual( ins.args, [ 'self', 'CEs' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, ( None, ) )

  def test_APIs_description( self ):
    
    __APIs__ = [ 'ResourceStatusClient', 'ReportsClient', 'ReportGenerator' ]
    self.assertEquals( self.clients[ self.me ].__APIs__, __APIs__ ) 

################################################################################

class TestCase_Description_RunningJobsBySiteSplitted_Command( unittest.TestCase ):

  me = 'RunningJobsBySiteSplitted_Command'

  def test_doCommand_definition( self ):
    
    ins = inspect.getargspec( self.clients[ self.me ].doCommand )   
    self.assertEqual( ins.args, [ 'self', 'sites' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, ( None, ) )

  def test_APIs_description( self ):
    
    __APIs__ = [ 'ResourceStatusClient', 'ReportsClient', 'ReportGenerator' ]
    self.assertEquals( self.clients[ self.me ].__APIs__, __APIs__ ) 
            
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF    