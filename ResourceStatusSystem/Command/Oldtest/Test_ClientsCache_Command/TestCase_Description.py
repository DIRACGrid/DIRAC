import unittest
import inspect

class TestCase_Description_JobsEffSimpleEveryOne_Command( unittest.TestCase ):

  me = 'JobsEffSimpleEveryOne_Command'

  def test_doCommand_definition( self ):
    
    ins = inspect.getargspec( self.clients[ self.me ].doCommand )   
    self.assertEqual( ins.args, [ 'self', 'sites' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, ( None, ) )

  def test_APIs_description( self ):
    
    __APIs__ = [ 'ResourceStatusClient', 'JobsClient', 'WMSAdministrator' ]
    self.assertEquals( self.clients[ self.me ].__APIs__, __APIs__ )

################################################################################

class TestCase_Description_PilotsEffSimpleEverySites_Command( unittest.TestCase ):

  me = 'PilotsEffSimpleEverySites_Command'

  def test_doCommand_definition( self ):
    
    ins = inspect.getargspec( self.clients[ self.me ].doCommand )   
    self.assertEqual( ins.args, [ 'self', 'sites' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, ( None, ) )

  def test_APIs_description( self ):
    
    __APIs__ = [ 'ResourceStatusClient', 'PilotsClient', 'WMSAdministrator' ]
    self.assertEquals( self.clients[ self.me ].__APIs__, __APIs__ )

################################################################################

class TestCase_Description_TransferQualityEverySEs_Command( unittest.TestCase ):

  me = 'TransferQualityEverySEs_Command'

  def test_doCommand_definition( self ):
    
    ins = inspect.getargspec( self.clients[ self.me ].doCommand )   
    self.assertEqual( ins.args, [ 'self', 'SEs' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, ( None, ) )

  def test_APIs_description( self ):
    
    __APIs__ = [ 'ResourceStatusClient', 'ReportsClient' ]
    self.assertEquals( self.clients[ self.me ].__APIs__, __APIs__ )

################################################################################

class TestCase_Description_DTEverySites_Command( unittest.TestCase ):

  me = 'DTEverySites_Command'

  def test_doCommand_definition( self ):
    
    ins = inspect.getargspec( self.clients[ self.me ].doCommand )   
    self.assertEqual( ins.args, [ 'self', 'sites' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, ( None, ) )

  def test_APIs_description( self ):
    
    __APIs__ = [ 'ResourceStatusClient', 'GOCDBClient' ]
    self.assertEquals( self.clients[ self.me ].__APIs__, __APIs__ )

################################################################################

class TestCase_Description_DTEveryResources_Command( unittest.TestCase ):

  me = 'DTEveryResources_Command'

  def test_doCommand_definition( self ):
    
    ins = inspect.getargspec( self.clients[ self.me ].doCommand )   
    self.assertEqual( ins.args, [ 'self', 'resources' ] )
    self.assertEqual( ins.varargs,  None )
    self.assertEqual( ins.keywords, None )
    self.assertEqual( ins.defaults, ( None, ) )

  def test_APIs_description( self ):
    
    __APIs__ = [ 'ResourceStatusClient', 'GOCDBClient' ]
    self.assertEquals( self.clients[ self.me ].__APIs__, __APIs__ )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF    