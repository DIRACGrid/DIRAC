"""  TransformationPlugin is a class wrapping the supported transformation plugins
"""

from DIRAC import gLogger, S_OK, S_ERROR

class TransformationPlugin:

  def __init__(self,plugin):
    self.valid = True
    self.params = False
    self.data = False
    supportedPlugins = ['LoadBalance','Automatic','Broadcast']
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

  def _Broadcast(self):
    """ This plug-in takes files found at the sourceSE and broadcasts to all targetSEs.
    """
    if not self.params:
      return S_ERROR("TransformationPlugin._Broadcast: The 'Broadcast' plugin requires additional parameters.")

    sourceSEs = self.params['SourceSE'].split(',')
    targetSEs = self.params['TargetSE'].split(',')

    seFiles = {}
    for lfn,se in self.data:
      if se in sourceSEs:
        sourceSite = se.split('_')[0].split('-')[0]
        targets = []
        for targetSE in targetSEs:
          if not targetSE.startswith(sourceSite):
            targets.append(targetSE)
        strTargetSE = ','.join(targets)
        if not seFiles.has_key(se):
          seFiles[se] = {}
        if not seFiles[se].has_key(strTargetSE):
          seFiles[se][strListToString(targets)] = []
        seFiles[se][strTargetSE].append(lfn)        
    return S_OK(seFiles) 

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

    sourceSE = ''
    if self.params.has_key('SourceSE'):
      sourceSE = self.params['SourceSE']
    seFiles = {}
    
    selectedFiles = []
    for lfn,se in self.data:
      useFile = False
      if not sourceSE:
        useFile = True
      elif sourceSE == se:
        useFile = True
      if useFile:
        selectedFiles.append(lfn)

    multiplier = int(len(selectedFiles)/float(totalRatio))
    if multiplier > 0:
      currIndex = 0
      seFiles[sourceSE] = {}
      for targetSE,load in targetSEs.items():
        offset = (load*multiplier)
        seFiles[sourceSE][targetSE] = selectedFiles[currIndex:currIndex+offset]
        currIndex += offset
    return S_OK(seFiles)

  def _Automatic(self):
    return S_ERROR()
