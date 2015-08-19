'''
Created on May 4, 2015

@author: Corentin Berger
'''
from DIRAC import S_ERROR, S_OK

from DIRAC.DataManagementSystem.private.DLSerializable import DLSerializable
from DIRAC.DataManagementSystem.Client.DataLogging.DLMethodCall import DLMethodCall
from DIRAC.DataManagementSystem.Client.DataLogging.DLCaller import DLCaller


class DLSequence( DLSerializable ) :
  """ Describe a sequence, used to know sequence of MethodCall"""
  attrNames = ['sequenceID', 'caller', 'methodCalls', 'userName', 'hostName', 'group', 'extra']

  def __init__( self, methodCalls = None, caller = None, sequenceID = None, userName = None, group = None, hostName = None, extra = {} ):
    """
    :param self: self reference
    :param methodCalls: a list of method call
    :param caller : the caller of the sequence
    :param sequenceID: id of the sequence
    :param userName: DLUserName object, can be none
    :param group: DLGroup object, can be none
    :param hostName: DLHostName object, can be none
    :param extra: dictionary with extra specific to the sequence
    """
    super( DLSequence, self ).__init__()
    self.sequenceID = sequenceID
    self.caller = caller
    self.userName = userName
    self.hostName = hostName
    self.group = group
    self.stack = []
    self.extra = extra
    self.attributesValues = []
    self.methodCalls = []

    if methodCalls :
      # we have to do this because when objects are deserialize from JSON
      # references about same objects are not saved and two objects are created instead of one
      self.stack.append( methodCalls[0] )
      while len( self.stack ) != 0 :
        mc = self.stack.pop()
        mc.sequence = self
        self.methodCalls.append( mc )
        for child in mc.children :
          self.stack.append( child )

  def appendMethodCall( self, args ):
    """
    append an operation into the stack
    :param self: self reference
    :param args: dict with the args to create an methodCall
    """
    methodCall = DLMethodCall( args )
    methodCall.sequence = self

    self.methodCalls.append( methodCall )
    self.stack.append( methodCall )

    return methodCall

  def popMethodCall( self ):
    """
    :param self: self reference
    Pop an operation from the stack
    """
    # if it's not  the first method call, we add the element that we need to pop into the parent
    if len( self.stack ) != 1 :
      self.stack[len( self.stack ) - 2].addChild( self.stack[len( self.stack ) - 1] )

    mc = self.stack.pop()
    # we set the rank of children
    cpt = 0
    for child in mc.children :
      child.rank = cpt
      cpt += 1

    return mc


  def isComplete( self ):
    return not self.stack

  def setCaller( self, caller ):
    self.caller = DLCaller( caller )


  def getCaller( self ) :
    return self.caller


  def isCallerSet( self ):
    return self.caller

  def addExtraArg( self, name, value ):
    self.extra[name] = value
