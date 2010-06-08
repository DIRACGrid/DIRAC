""" The Command class is a simple base class for all the commands
    for interacting with the clients
"""

from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Utilities.Utils import *

class Command(object):
  
  def __init__(self):
    self.args = None
    self.client = None
    self.RPC = None
    self.timeout = 10
  
  def setArgs(self, argsIn):
    """
    Set the command arguments, as a tuple. The tuple has to contain at least 2 values.
    
    :params:

      :attr:`args`: a tuple 
        - `args[0]` should be a ValidRes

        - `args[1]` should be the name of the ValidRes
    """
    if not isinstance(argsIn, tuple):
      raise TypeError, "`Args` of commands should be in a tuple."
    
    self.args = argsIn
    if not isinstance(self.args, tuple):
      raise TypeError, where(self, self.setArgs)
    if self.args[0] not in ValidRes:
      raise InvalidRes, where(self, self.setArgs)

  def setClient(self, clientIn = None):
    """
    set `self.client`. If not set, a standard client will be instantiated.
    
    :params:
      :attr:`clientIn`: a client object 
    """
    self.client = clientIn
  
  def setRPC(self, RPCIn = None):
    """
    set `self.RPC`. If not set, a standard RPC will be instantiated.
    
    :params:
      :attr:`RPCIn`: a client object 
    """
    self.RPC = RPCIn
  
  def setTimeOut(self, timoeut = None):
    """
    set `self.timeout`. If not set, a standard RPC will be instantiated.
    
    :params:
      :attr:`timeout`: a client object 
    """
    self.timeout = timeout
  
  #to be extended by real commands
  def doCommand(self):
    """ 
    Before use, call at least `setArgs`.  
    """
    if self.args is None:
      raise RSSException, "Before, set `self.args` with `self.setArgs(a)` function."
    