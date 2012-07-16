# $HeadURL $
''' CommandCaller

  Module that loads commands and executes them.

'''

from DIRAC import S_ERROR, S_OK#, gLogger
from DIRAC.ResourceStatusSystem.Utilities              import Utils
#from DIRAC.ResourceStatusSystem.Command.ClientsInvoker import ClientsInvoker

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

  def commandInvocation( self, commandTuple, pArgs, decissionParams, clients ):
    """
    Returns a command object, given comm

    :params:
      `comm`: a tuple, where comm[0] is a module name and comm[1] is a class name (inside the module)
    """
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
      
    commandObject = getattr( commandModule, cClass )( pArgs, decissionParams, clients ) 

    return S_OK( commandObject ) 

################################################################################
#
#  def setClient( self, commandObj, clientName, clientInstance ):
#    commandObj.setAPI( clientName, clientInstance )
#
#################################################################################
#  
#  def setDecissionParams( self, commandObj, decissionParams ):
#    commandObj.setDecissionParams( decissionParams )
#
#################################################################################
#
#  def setArgs( self, commandObj, args ):
#    commandObj.setArgs( args )
#
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