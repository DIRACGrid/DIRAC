def getExt():
  return ""

def getSetup():
  return {'OK': True, 'Value': ''}

def getMailForUser(users):
  return ""

class Logger:
  
  def dummy(self, *args, **kwargs):
    pass
  def __getattr__( self, name ):
    return self.dummy
    
gLogger = Logger()    