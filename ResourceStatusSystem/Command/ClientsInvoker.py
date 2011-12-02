################################################################################
# $HeadURL $
################################################################################
__RCSID__  = "$Id$"

class ClientsInvoker:
  """ 
    Clients Invoker is the invoker for commands to be executed on clients
  """
  
  def setCommand(self, c):
    """ Set command to c
    """
    self.command = c
    
  def doCommand(self):
    """ Call command.doCommand 
    """
    return self.command.doCommand()
  
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF   