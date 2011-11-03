class ClientsInvoker( object ):
      
  def setCommand( self, c ):
    self.command = c
    
  def doCommand( self ):
    return self.command.doCommand()