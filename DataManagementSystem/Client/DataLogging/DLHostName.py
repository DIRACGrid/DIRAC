'''
Created on Jul 10, 2015

@author: Corentin Berger
'''
from DIRAC.DataManagementSystem.private.DLSerializable import DLSerializable


class DLHostName( DLSerializable ):
  """
    this is the DLHostName class for data logging system
    DLHostName is the dirac HostName
  """

  attrNames = ['callerID', 'name']

  def __init__( self, name ):
    super( DLHostName, self ).__init__()
    self.name = name
