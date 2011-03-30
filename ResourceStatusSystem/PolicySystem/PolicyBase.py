""" The Policy class is a simple base class for all the policies
"""

from DIRAC.ResourceStatusSystem.Utilities.Exceptions import InvalidRes, InvalidStatus, RSSException
from DIRAC.ResourceStatusSystem.Utilities.Utils import where, ValidRes, ValidStatus

from DIRAC.ResourceStatusSystem.Command.ClientsInvoker import ClientsInvoker

#############################################################################

class PolicyBase(object):
  """
  Base class for all the policies. Do not instantiate directly.
  To use, you should call at least `setArgs` and, alternatively, 
  `setCommand` or `setCommandName` on the real policy instance.    
    
  It will set `self.oldStatus` as the status of the ValidRes. 
  """      
        

  def __init__(self):
    self.args = None
    self.command = None
    self.commandName = None
    self.knownInfo = None
    self.infoName = None
    self.result = {}
  
  def setArgs(self, argsIn):
    """
    Set `self.args`.
    
    :params:

      :attr:`argsIn`: a tuple of a list of tuples. Each tuple contains at least: 
        - `args[0]` should be a ValidRes

        - `args[1]` should be the name of the ValidRes

        - `args[2]` should be the present status
    """    
    self.args = argsIn
    
    if not isinstance(self.args, tuple):
      if not isinstance(self.args, list):
        raise TypeError, where(self, self.setArgs)
      for arg in self.args:
        if arg[0] not in ValidRes:
          raise InvalidRes, where(self, self.setArgs)
        if arg[2] not in ValidStatus:
          raise InvalidStatus, where(self, self.setArgs)
      self.oldStatus = self.args[0][2]
    else:
      if self.args[0] not in ValidRes:
        raise InvalidRes, where(self, self.setArgs)
      if self.args[2] not in ValidStatus:
        raise InvalidStatus, where(self, self.setArgs)
      self.oldStatus = self.args[2]

  def setCommand(self, commandIn = None):
    """
    Set `self.command`. 
    
    :params:
      :attr:`commandIn`: a command object 
    """
    self.command = commandIn
  
  def setCommandName(self, commandNameIn = None):
    """
    Set `self.commandName`, necessary when a command object is not provided with setCommand. 
    
    :params:
      :attr:`commandNameIn`: a tuple containing the command module and class (as strings) 
    """
    self.commandName = commandNameIn
  
  def setKnownInfo(self, knownInfoIn = None):
    """
    Set `self.knownInfo`. No command will be then invoked. 
    
    :params:

      :attr:`knownInfoIn`: a dictionary
    """
    self.knownInfo = knownInfoIn
  
  def setInfoName(self, infoNameIn = None):
    """
    Set `self.infoName`. 
    
    :params:

      :attr:`infoNameIn`: a string
    """
    self.infoName = infoNameIn
  
  # method to be extended by sub(real) policies
  def evaluate(self):
    """
    Before use, call at least `setArgs` and, alternatively, 
    `setCommand` or `setCommandName`.    
    Then, use `self.oldStatus` as the status of the ValidRes. 
    
    Invoking `super(PolicyCLASS, self).evaluate` will invoke 
    the command (if necessary) as it is provided and returns the results.  
    """

    if self.knownInfo is not None:
      result = self.knownInfo
    else:
      if self.command is None:
        # use standard Command
        from DIRAC.ResourceStatusSystem.Command.CommandCaller import CommandCaller
        cc = CommandCaller()
        self.command = cc.setCommandObject(self.commandName)

      clientsInvoker = ClientsInvoker()
      clientsInvoker.setCommand(self.command)

      if isinstance(self.args, tuple):
        if len(self.args) == 3:
          self.command.setArgs((self.args[0], self.args[1]))
        elif len(self.args) == 4:
          self.command.setArgs((self.args[0], self.args[1], self.args[3]))
        elif len(self.args) == 5:
          self.command.setArgs((self.args[0], self.args[1], self.args[3], self.args[4]))
        else:
          raise RSSException, where(self, self.evaluate)
      elif isinstance(self.args, list):
        argsList = []
        for arg in self.args:
          if len(arg) == 3:
            argsList.append((arg[0], arg[1]))
          elif len(arg) == 4:
            argsList.append((arg[0], arg[1], arg[3]))
          elif len(arg) == 5:
            argsList.append((arg[0], arg[1], arg[3], arg[4]))
          else:
            raise RSSException, where(self, self.evaluate)
        self.command.setArgs(argsList)

      result = clientsInvoker.doCommand()

    if self.infoName is None:
      result = result['Result']
    else:
      if self.infoName in result.keys():
        result = result[self.infoName]
      else:
        raise RSSException, "missing \'infoName\' in result"

    return result
  
#############################################################################
