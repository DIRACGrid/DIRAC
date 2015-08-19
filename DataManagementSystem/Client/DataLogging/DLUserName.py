'''
Created on Jul 10, 2015

@author: Corentin Berger
'''

from DIRAC.DataManagementSystem.private.DLSerializable import DLSerializable


class DLUserName( DLSerializable ):
  """
    this is the DLUserName class for data logging system
    DLUserName is the dirac username
  """

  attrNames = ['userNameID', 'name']

  def __init__( self, name ):
    super( DLUserName, self ).__init__()
    self.name = name
