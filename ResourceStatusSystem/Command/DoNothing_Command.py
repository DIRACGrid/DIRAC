from DIRAC.ResourceStatusSystem.Command.Command import Command

class DoNothing_Command(Command):
  
  def doCommand(self):
    """ 
    """
    super(DoNothing_Command, self).doCommand()

    return {'Result':None}

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__
    
