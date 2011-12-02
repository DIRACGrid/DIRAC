class PolicyBase(object):
  
  def __init__(self):
    pass
  
  def setArgs(self, argsIn):
    pass
  
  def setCommand(self, commandIn = None):
    pass
  
  def setCommandName(self, commandNameIn = None):
    pass
  
  def setKnownInfo(self, knownInfoIn = None):
    pass
  
  def setInfoName(self, infoNameIn = None):
    pass
  
  def evaluate( self ):
    return { 'Status' : '', 'Reason' : '' }    