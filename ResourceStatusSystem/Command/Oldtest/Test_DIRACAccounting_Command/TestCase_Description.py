import unittest
import inspect

class TestCase_Description_DIRACAccounting_Command( unittest.TestCase ):

  me = 'DIRACAccounting_Command'

  def test_doCommand_definition( self ):
    
    ins = inspect.getargspec( self.clients[ self.me ].doCommand )   
    self.assertEqual( ins.args, [ 'self' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )

  def test_APIs_description( self ):
    
    __APIs__ = [ 'ReportGenerator', 'ReportsClient' ]
    self.assertEquals( self.clients[ self.me ].__APIs__, __APIs__ )

################################################################################

class TestCase_Description_TransferQuality_Command( unittest.TestCase ):

  me = 'TransferQuality_Command'

  def test_doCommand_definition( self ):
    
    ins = inspect.getargspec( self.clients[ self.me ].doCommand )   
    self.assertEqual( ins.args, [ 'self' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )

  def test_APIs_description( self ):
    
    __APIs__ = [ 'ReportGenerator', 'ReportsClient' ]
    self.assertEquals( self.clients[ self.me ].__APIs__, __APIs__ )

################################################################################

class TestCase_Description_TransferQualityCached_Command( unittest.TestCase ):

  me = 'TransferQualityCached_Command'

  def test_doCommand_definition( self ):
    
    ins = inspect.getargspec( self.clients[ self.me ].doCommand )   
    self.assertEqual( ins.args, [ 'self' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )

  def test_APIs_description( self ):
    
    __APIs__ = [ 'ResourceManagementClient' ]
    self.assertEquals( self.clients[ self.me ].__APIs__, __APIs__ )

################################################################################

class TestCase_Description_CachedPlot_Command( unittest.TestCase ):

  me = 'CachedPlot_Command'

  def test_doCommand_definition( self ):
    
    ins = inspect.getargspec( self.clients[ self.me ].doCommand )   
    self.assertEqual( ins.args, [ 'self' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )

  def test_APIs_description( self ):
    
    __APIs__ = [ 'ResourceManagementClient' ]
    self.assertEquals( self.clients[ self.me ].__APIs__, __APIs__ )

################################################################################

class TestCase_Description_TransferQualityFromCachedPlot_Command( unittest.TestCase ):

  me = 'TransferQualityFromCachedPlot_Command'

  def test_doCommand_definition( self ):
    
    ins = inspect.getargspec( self.clients[ self.me ].doCommand )   
    self.assertEqual( ins.args, [ 'self' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )

  def test_APIs_description( self ):
    
    __APIs__ = [ 'ResourceManagementClient' ]
    self.assertEquals( self.clients[ self.me ].__APIs__, __APIs__ )
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF    