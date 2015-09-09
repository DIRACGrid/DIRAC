'''
Created on May 4, 2015

@author: Corentin Berger
'''

from DIRAC.DataManagementSystem.private.DLSerializable import DLSerializable

class DLFile( DLSerializable ):
  """ this is the class for data logging system which is like lfn"""
  attrNames = ['name']

  def __init__( self, name ):
    super( DLFile, self ).__init__()
    self.name = name
