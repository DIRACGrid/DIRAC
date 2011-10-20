################################################################################
# $HeadURL $
################################################################################
__RCSID__  = "$Id$"

""" 
  The Policy class is a simple base class for all the policies
"""

from DIRAC.ResourceStatusSystem.Utilities.Exceptions        import InvalidRes, RSSException
from DIRAC.ResourceStatusSystem.Utilities.Utils             import where
from DIRAC.ResourceStatusSystem                             import ValidRes

from DIRAC.ResourceStatusSystem.Command.ClientsInvoker      import ClientsInvoker

class PolicyBase(object):
  """
  Base class for all the policies. Do not instantiate directly.
  To use, you should call at least `setArgs` and, alternatively,
  `setCommand` or `setCommandName` on the real policy instance.
  """

  def __init__(self):
    self.args        = None
    self.command     = None
    self.commandName = None
    self.knownInfo   = None
    self.infoName    = None
    self.result      = {}

################################################################################

  def setArgs(self, argsIn):
    """
    Set `self.args`.

    :params:

      :attr:`argsIn`: a tuple: (Module Command doesn't handle lists for now.)
        - `args[0]` should be a ValidRes

        - `args[1]` should be the name of the ValidRes
    """
    self.args = argsIn

    if self.args[0] not in ValidRes:
      raise InvalidRes, where(self, self.setArgs)

################################################################################

  def setCommand(self, commandIn = None):
    """
    Set `self.command`.

    :params:
      :attr:`commandIn`: a command object
    """
    self.command = commandIn

################################################################################

  def setCommandName(self, commandNameIn = None):
    """
    Set `self.commandName`, necessary when a command object is not provided with setCommand.

    :params:
      :attr:`commandNameIn`: a tuple containing the command module and class (as strings)
    """
    self.commandName = commandNameIn

################################################################################

  def setKnownInfo(self, knownInfoIn = None):
    """
    Set `self.knownInfo`. No command will be then invoked.

    :params:

      :attr:`knownInfoIn`: a dictionary
    """
    self.knownInfo = knownInfoIn

################################################################################

  def setInfoName(self, infoNameIn = None):
    """
    Set `self.infoName`.

    :params:

      :attr:`infoNameIn`: a string
    """
    self.infoName = infoNameIn

################################################################################

  # method to be extended by sub(real) policies
  def evaluate(self):
    """
    Before use, call at least `setArgs` and, alternatively,
    `setCommand` or `setCommandName`.

    Invoking `super(PolicyCLASS, self).evaluate` will invoke
    the command (if necessary) as it is provided and returns the results.
    """

    if self.knownInfo:
      result = self.knownInfo
    else:
      if not self.command:
        # use standard Command
        from DIRAC.ResourceStatusSystem.Command.CommandCaller import CommandCaller
        cc = CommandCaller()
        self.command = cc.setCommandObject(self.commandName)

      clientsInvoker = ClientsInvoker()
      clientsInvoker.setCommand(self.command)

      self.command.setArgs(self.args)

      result = clientsInvoker.doCommand()

    if not self.infoName:
      result = result['Result']
    else:
      if self.infoName in result.keys():
        result = result[self.infoName]
      else:
        raise RSSException, "missing 'infoName' in result"

    return result

################################################################################
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #  
################################################################################

'''
  HOW DOES THIS WORK.
    
    will come soon...
'''
            
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF