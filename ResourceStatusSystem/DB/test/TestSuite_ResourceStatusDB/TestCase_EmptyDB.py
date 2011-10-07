import unittest

from mock import Mock
from DIRAC.ResourceStatusSystem.DB.ResourceStatusDB import ResourceStatusDB

class TestCase_EmptyDB( unittest.TestCase ):
  
  def setUp( self ):   
    
    self.mockDB = Mock()
    self.mockDB._query.return_value  = { 'OK': True, 'Value' : '' }
    self.mockDB._update.return_value = { 'OK': True, 'Value' : '' }             
                       
    self.rsDB   = ResourceStatusDB( DBin = self.mockDB )