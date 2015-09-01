'''
Created on May 18, 2015

@author: Corentin Berger
'''

from DIRAC.DataManagementSystem.private.DLSerializable import DLSerializable

class DLMethodName( DLSerializable ):

  attrNames = ['name']

  def __init__( self, name ):
    super( DLMethodName, self ).__init__()
    self.name = name
    self.attrNames = ['name']
