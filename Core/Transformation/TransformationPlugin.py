"""  TransformationPlugin is a class wrapping the supported transformation plugins
"""

from DIRAC import gLogger, S_OK, S_ERROR

class TransformationPlugin:

  def __init__(self,plugin):
    self.valid = True
    self.params = False
    self.data = False
    supportedPlugins = ['LoadBalance','Automatic']
    if not plugin in supportedPlugins:
      self.valid = False
    else:
      self.plugin = plugin

  def isOK(self):
    return self.valid

  def setInputData(self,data):
    self.data = data

  def setParameters(self,params):
    self.params = params

  def generateTask(self):
    evalString = "self._%s()" % self.plugin
    return eval(evalString)

  def _LoadBalance(self):
    """ This plug-in will load balances the input files across the selected target SEs.
    """
    if not self.params:
      return S_ERROR("TransformationPlugin._LoadBalance: The 'LoadBalance' plugin requires additional parameters.")

    targetSEs = {}
    totalRatio = 0
    ses = self.params['TargetSE'].split(',')
    for targetSE in ses:
      targetSEs[targetSE] = int(self.params[targetSE])
      totalRatio += int(self.params[targetSE])

    sourceSE = False
    if self.params.has_key('SourceSE'):
      sourceSE = self.params['SourceSE']

    selectedFiles = []
    for lfn,se in self.data:
      useFile = False
      if not sourceSE:
        useFile = True
      elif sourceSE == se:
        useFile = True
      if useFile:
        selectedFiles.append((lfn,se))

    seFiles = {}
    multiplier = int(len(selectedFiles)/float(totalRatio))
    if multiplier > 0:
      currIndex = 0
      for targetSE,load in targetSEs.items():
        offset = (load*multiplier)
        seFiles[targetSE] = selectedFiles[currIndex:currIndex+offset]
        currIndex += offset
    return S_OK(seFiles)

  def _Automatic(self):
    return S_ERROR()
