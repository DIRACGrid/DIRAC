# $HeadURL $
''' PolicyBase
  
  The Policy class is a simple base class for all the policies.
  
'''

#from DIRAC                                                  import gLogger 
#from DIRAC.ResourceStatusSystem.Command.ClientsInvoker      import ClientsInvoker
#from DIRAC.ResourceStatusSystem.Command.CommandCaller       import CommandCaller
#from DIRAC.ResourceStatusSystem.Utilities                   import RssConfiguration

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
    
#    self.args            = None
#    self.decissionParams = None 
    self.command         = Command()
#    self.commandName     = None
#    self.knownInfo       = None
#    self.infoName        = None
    self.result          = {}

#  def setArgs( self, policyArguments ):
#    '''
#    Set `self.args`.
#
#    :params:
#
#      :attr:`argsIn`: a tuple: (Module Command doesn't handle lists for now.)
#        - `args[0]` should be a ValidElement
#
#        - `args[1]` should be the name of the ValidElement
#    '''
#    self.args = policyArguments
#
##    validElements = RssConfiguration.getValidElements()
##
##    if self.args[0] not in validElements:
##      gLogger.error( 'PolicyBase.setArgs got wrong ValidElement' )
#
#  def setDecissionParams( self, decissionParams ):
#    
#    self.decissionParams = decissionParams

  def setCommand( self, policyCommand ):
    '''
    Set `self.command`.

    :params:
      :attr:`commandIn`: a command object
    '''
    if policyCommand is not None:
      self.command = policyCommand

#  def setCommandName( self, commandNameIn = None ):
#    '''
#    Set `self.commandName`, necessary when a command object is not provided with setCommand.
#
#    :params:
#      :attr:`commandNameIn`: a tuple containing the command module and class (as strings)
#    '''
#    self.commandName = commandNameIn

#  def setKnownInfo( self, knownInfoIn = None ):
#    '''
#    Set `self.knownInfo`. No command will be then invoked.
#
#    :params:
#
#      :attr:`knownInfoIn`: a dictionary
#    '''
#    self.knownInfo = knownInfoIn
#
#  def setInfoName( self, infoNameIn = None ):
#    '''
#    Set `self.infoName`.
#
#    :params:
#
#      :attr:`infoNameIn`: a string
#    '''
#    self.infoName = infoNameIn

  # method to be extended by sub(real) policies
  def evaluate( self ):
    '''
    Before use, call at least `setArgs` and, alternatively,
    `setCommand` or `setCommandName`.

    Invoking `super(PolicyCLASS, self).evaluate` will invoke
    the command (if necessary) as it is provided and returns the results.
    '''

#    if self.knownInfo:
#      result = self.knownInfo
#    else:
#    if not self.command:
#      # use standard Command
#      cc = CommandCaller()
#      self.command = cc.setCommandObject( self.commandName )

    #clientsInvoker = ClientsInvoker()
    #clientsInvoker.setCommand( self.command )

#    self.command.setArgs( self.args )
#    self.command.setDecissionParams( self.decissionParams )

    result = self.command.doCommand()       

    return result

#    if not self.infoName:
#      result = result[ 'Result' ]
#    else:
#      if self.infoName in result.keys():
#        result = result[ self.infoName ]
#      else:
#        gLogger.error( 'missing "infoName" in result' )
#        return None
#
#    return result

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF