from DIRAC.ResourceStatusSystem.Command.mock.Command import Command 

class CommandCaller( object ):
  
  def setCommandObject( self, comm ):
    return Command()
  
  def commandInvocation( self, granularity = None, name = None, command = None,  
                         args = None, comm = None, extraArgs = None ):
    return {}
  
    