import unittest
import inspect

class TestCase_Description_RSPeriods_Command( unittest.TestCase ):

  me = 'RSPeriods_Command'

  def test_doCommand_definition( self ):
    
    ins = inspect.getargspec( self.clients[ self.me ].doCommand )   
    self.assertEqual( ins.args, [ 'self' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )

  def test_APIs_description( self ):
    
    __APIs__ = [ 'ResourceStatusClient' ]
    self.assertEquals( self.clients[ self.me ].__APIs__, __APIs__ )

################################################################################

class TestCase_Description_ServiceStats_Command( unittest.TestCase ):

  me = 'ServiceStats_Command'

  def test_doCommand_definition( self ):
    
    ins = inspect.getargspec( self.clients[ self.me ].doCommand )   
    self.assertEqual( ins.args, [ 'self' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )

  def test_APIs_description( self ):
    
    __APIs__ = [ 'ResourceStatusClient' ]
    self.assertEquals( self.clients[ self.me ].__APIs__, __APIs__ )

################################################################################

class TestCase_Description_ResourceStats_Command( unittest.TestCase ):

  me = 'ResourceStats_Command'

  def test_doCommand_definition( self ):
    
    ins = inspect.getargspec( self.clients[ self.me ].doCommand )   
    self.assertEqual( ins.args, [ 'self' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )

  def test_APIs_description( self ):
    
    __APIs__ = [ 'ResourceStatusClient' ]
    self.assertEquals( self.clients[ self.me ].__APIs__, __APIs__ )

################################################################################

class TestCase_Description_StorageElementsStats_Command( unittest.TestCase ):

  me = 'StorageElementsStats_Command'

  def test_doCommand_definition( self ):
    
    ins = inspect.getargspec( self.clients[ self.me ].doCommand )   
    self.assertEqual( ins.args, [ 'self' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )

  def test_APIs_description( self ):
    
    __APIs__ = [ 'ResourceStatusClient' ]
    self.assertEquals( self.clients[ self.me ].__APIs__, __APIs__ )

################################################################################

class TestCase_Description_MonitoredStatus_Command( unittest.TestCase ):

  me = 'MonitoredStatus_Command'

  def test_doCommand_definition( self ):
    
    ins = inspect.getargspec( self.clients[ self.me ].doCommand )   
    self.assertEqual( ins.args, [ 'self' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, None )

  def test_APIs_description( self ):
    
    __APIs__ = [ 'ResourceStatusClient' ]
    self.assertEquals( self.clients[ self.me ].__APIs__, __APIs__ )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF    