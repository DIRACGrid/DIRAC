""" The Command class is a simple base class for all the commands
    for interacting with the clients
"""

from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Utilities.Utils import *

class Command(object):
  
  def __init__(self):
    self.args = None
    self.client = None
  
  def setArgs(self, argsIn):
    """
    :params:

      :attr:`args`: a tuple 
        - `args[0]` should be a ValidRes

        - `args[1]` should be the name of the ValidRes

        - `args[2]` should be the present status
    """    
    self.args = argsIn
    if not isinstance(self.args, tuple):
      raise TypeError, where(self, self.setArgs)
    if self.args[0] not in ValidRes:
      raise InvalidRes, where(self, self.setArgs)

  def setClient(self, clientIn = None):
    """
    set `self.client`. If not set, a standard client will be instantiated.
    
    :params:
      :attr:`commandIn`: a command object 
    """
    self.client = clientIn
  
  #to be extended by real commands
  def doCommand(self):
    """ 
    Before use, call at least `setArgs`.  
    """
    pass