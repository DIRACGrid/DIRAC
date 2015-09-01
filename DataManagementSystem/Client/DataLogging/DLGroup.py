'''
Created on Jul 10, 2015

@author: Corentin Berger
'''
from DIRAC.DataManagementSystem.private.DLSerializable import DLSerializable


class DLGroup( DLSerializable ):
  """
    this is the DLGroup class for data logging system
    DLGroup is the DIRAC group
  """

  attrNames = ['groupID', 'name']

  def __init__( self, name ):
    super( DLGroup, self ).__init__()
    self.name = name
