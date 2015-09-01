'''
Created on May 18, 2015

@author: Corentin Berger
'''

from DIRAC.DataManagementSystem.private.DLSerializable import DLSerializable

class DLStorageElement( DLSerializable ):

  attrNames = ['name']

  def __init__( self, name ):
    super( DLStorageElement, self ).__init__()
    self.name = name

