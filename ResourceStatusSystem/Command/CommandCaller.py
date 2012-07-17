# $HeadURL $
''' CommandCaller

  Module that loads commands and executes them.

'''

import copy

from DIRAC                                import S_ERROR, S_OK
from DIRAC.ResourceStatusSystem.Utilities import Utils

__RCSID__ = '$Id: $'

class CommandCaller:
  """
    Module used for calling policies. Its class is used for invoking
    real policies, based on the policy name
  """

  def __init__( self ):
    pass

#  def commandInvocation( self, granularity = None, name = None, command = None,
#                         comm = None, extraArgs = None):
#
#    c = command if command else self.setCommandObject(comm)
#    a = (granularity, name) if not extraArgs else (granularity, name) + extraArgs
#
#    res = self._innerCall(c, a)
#    return res

################################################################################

  def commandInvocation( self, commandTuple, pArgs = None, 
                         decissionParams = None, clients = None ):
    """
    Returns a command object, given comm

    :params:
      `comm`: a tuple, where comm[0] is a module name and comm[1] is a class name (inside the module)
    """
    
    if commandTuple is None:
      return S_OK( None )
    
    try:
      cModule = commandTuple[ 0 ]
      cClass  = commandTuple[ 1 ]
      commandModule = Utils.voimport( 'DIRAC.ResourceStatusSystem.Command.' + cModule )
    except ImportError:
      #gLogger.warn( "Command %s/%s not found, using dummy command DoNothing_Command." % ( cModule, cClass ) )
      return S_ERROR( "Import error for command %s." % ( cModule ) )
      #cClass = "DoNothing_Command"
      #commandModule = __import__( "DIRAC.ResourceStatusSystem.Command.DoNothing_Command", globals(), locals(), ['*'] )

    if not hasattr( commandModule, cClass ):
      return S_ERROR( '%s has no %s' % ( cModule, cClass ) )
    
    # We merge decision parameters and policy arguments.
    newArgs = copy.deepcopy( decissionParams )
    newArgs.update( pArgs )
      
    commandObject = getattr( commandModule, cClass )( newArgs, clients ) 

    return S_OK( commandObject ) 

#################################################################################
#
#  def _innerCall(self, command, args):#, clientIn = None):
#    """ command call
#    """
#    #clientsInvoker = ClientsInvoker()
#
#    command.setArgs(args)
#    #clientsInvoker.setCommand(c)
#
#    res = command.doCommand()
#
#    return res

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF