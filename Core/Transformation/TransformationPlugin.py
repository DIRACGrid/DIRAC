class TransformationPlugin:
  
  def __init__(self,plugin):
    self.valid = True
    self.plugin = plugin

  def setInputData(self,data):
    self.data = data

  def _LoadBalancePlugin(self):
    print self.data

  def _AutomaticPlugin(self):
    return S_OK('sdfsdfsdfsdf')

  def isOK(self):
    return self.valid
