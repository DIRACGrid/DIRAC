# $HeadURL $
''' PolicyBase
  
  The Policy class is a simple base class for all the policies.
  
'''

from DIRAC.ResourceStatusSystem.Command.Command import Command

__RCSID__  = '$Id: $'

class PolicyBase( object ):
  '''
  Base class for all the policies. Do not instantiate directly.
  To use, you should call at least `setArgs` and, alternatively,
  `setCommand` or `setCommandName` on the real policy instance.
  '''

  def __init__( self ):
    '''
    Constructor
    '''
    
    self.command = Command()
    self.result  = {}

  def setCommand( self, policyCommand ):
    '''
    Set `self.command`.

    :params:
      :attr:`commandIn`: a command object
    '''
    if policyCommand is not None:
      self.command = policyCommand

  # method to be extended by sub(real) policies
  def evaluate( self ):
    '''
    Before use, call at least `setArgs` and, alternatively,
    `setCommand` or `setCommandName`.

    Invoking `super(PolicyCLASS, self).evaluate` will invoke
    the command (if necessary) as it is provided and returns the results.
    '''

    result = self.command.doCommand()       
    return result

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF