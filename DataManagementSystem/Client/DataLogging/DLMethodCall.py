'''
Created on May 4, 2015

@author: Corentin Berger
'''

import datetime
from types import StringTypes
from DIRAC.DataManagementSystem.private.DLSerializable import DLSerializable

class DLMethodCall( DLSerializable ):
  """
  describe a method call
  """

  attrNames = ['methodCallID', 'creationTime', 'name', 'caller', 'parentID', 'sequenceID', 'rank', 'children', 'actions']

  def __init__( self, fromDict = None ):
    """
    :param self: self reference
    :param dict fromDict: attributes dictionary
    """
    super( DLMethodCall, self ).__init__()
    now = datetime.datetime.utcnow().replace( microsecond = 0 )
    self.creationTime = now
    self.children = []
    self.actions = []
    self.name = None
    self.rank = 0
    self.sequence = None
    self.parentID = None
    self.methodCallID = None
    # set the different attribute from dictionary 'fromDict'
    for key, value in fromDict.items():
      if type( value ) in StringTypes:
        value = value.encode()
      if value is not None:
        setattr( self, key, value )



  def addChild( self, child ):
    """
    Add a child into the children list
    """
    self.children.append( child )


  def addAction( self, action ):
    """
    Add an action into the actions list
    """
    self.actions.append( action )
