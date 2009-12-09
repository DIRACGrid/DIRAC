"""  TransformationPlugin is a class wrapping the supported transformation plugins
"""
from DIRAC                                                  import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.SiteSEMapping                     import getSitesForSE
from DIRAC.Core.Utilities.List                              import breakListIntoChunks, sortList, uniqueElements
from DIRAC.DataManagementSystem.Client.ReplicaManager       import ReplicaManager
import random

class TransformationPlugin:

  def __init__(self,plugin):
    self.params = False
    self.data = False
    self.plugin = plugin

  def isOK(self):
    self.valid = True
    if (not self.data) or (not self.params):
      self.valid = False
    return self.valid

  def setInputData(self,data):
    self.data = data

  def setParameters(self,params):
    self.params = params

  def generateTasks(self):
    try:
      evalString = "self._%s()" % self.plugin
      return eval(evalString)
    except AttributeError,x:
      return S_ERROR("Plugin not found")
    except Exception,x:
      return S_ERROR(x)

  def _Broadcast(self):
    """ This plug-in takes files found at the sourceSE and broadcasts to all targetSEs.
    """
    if not self.params:
      return S_ERROR("TransformationPlugin._Broadcast: The 'Broadcast' plugin requires additional parameters.")
    sourceSEs = self.params['SourceSE'].split(',')
    targetSEs = self.params['TargetSE'].split(',')

    fileGroups = self.__getFileGroups(self.data)
    targetSELfns = {}
    for replicaSE,lfns in fileGroups.items():
      ses = replicaSE.split(',')
      sourceSites = self.__getSitesForSEs(ses)
      atSource = False
      for se in ses:
        if se in sourceSEs:
          atSource = True
      if not atSource:
        continue
      targets = []
      for targetSE in targetSEs:
        site = self.__getSiteForSE(targetSE)['Value']
        if not site in sourceSites:
          targets.append(targetSE)
          sourceSites.append(site)
      strTargetSEs = str.join(',',targets)
      if not targetSELfns.has_key(strTargetSEs):
        targetSELfns[strTargetSEs] = []
      targetSELfns[strTargetSEs].extend(lfns)
    tasks = []
    for ses,lfns in targetSELfns.items():
      tasks.append((ses,lfns))
    return S_OK(tasks)

  def _Standard(self):
    return self.__groupByReplicas()

  def _BySize(self):
    return self.__groupBySize()

  def __groupByReplicas(self):
    """ Generates a job based on the location of the input data """
    if not self.params:
      return S_ERROR("TransformationPlugin._Standard: The 'Standard' plug-in requires parameters.")
    status = self.params['Status']
    groupSize = self.params['GroupSize']
    # Group files by SE
    fileGroups = self.__getFileGroups(self.data)
    # Create tasks based on the group size
    tasks = []
    for replicaSE,lfns in fileGroups.items():
      tasksLfns = breakListIntoChunks(lfns,groupSize)
      for taskLfns in tasksLfns:
        if (status == 'Flush') or (len(taskLfns) >= int(groupSize)):
          tasks.append((replicaSE,taskLfns))
    return S_OK(tasks)
  
  def __groupBySize(self):
    """ Generate a task for a given amount of data """
    if not self.params:
      return S_ERROR("TransformationPlugin._BySize: The 'BySize' plug-in requires parameters.")
    status = self.params['Status']
    requestedSize = float(self.params['GroupSize'])*1000*1000*1000 # input size in GB converted to bytes
    # Group files by SE
    fileGroups = self.__getFileGroups(self.data)
    # Get the file sizes
    rm = ReplicaManager()
    res = rm.getCatalogFileSize(self.data.keys())
    if not res['OK']:
      return S_ERROR("Failed to get sizes for files")
    if res['Value']['Failed']:
      return S_ERROR("Failed to get sizes for all files")
    fileSizes = res['Value']['Successful']
    tasks = []
    for replicaSE,lfns in fileGroups.items():
      taskLfns = []
      taskSize = 0
      for lfn in lfns:
        taskSize += fileSizes[lfn]
        taskLfns.append(lfn)
        if taskSize > requestedSize:
          tasks.append(replicaSE,taskLfns)
          taskLfns = []
          taskSize = 0
      if (status == 'Flush') and taskLfns:
        tasks.append((replicaSE,taskLfns))
    return S_OK(tasks)

  def __getFileGroups(self,fileReplicas):
    fileGroups = {}
    for lfn,replicas in fileReplicas.items():
      replicaSEs = str.join(',',sortList(uniqueElements(replicas.keys())))
      if not fileGroups.has_key(replicaSEs):
        fileGroups[replicaSEs] = []
      fileGroups[replicaSEs].append(lfn)
    return fileGroups
  
  def __getSiteForSE(self,se):
    """ Get site name for the given SE
    """
    result = getSitesForSE(se,gridName='LCG')
    if not result['OK']:
      return result
    if result['Value']:
      return S_OK(result['Value'][0])
    return S_OK('')

  def __getSitesForSEs(self, seList):
    """ Get all the sites for the given SE list
    """
    sites = []
    for se in seList:
      result = getSitesForSE(se,gridName='LCG')
      if result['OK']:
        sites += result['Value']
    return sites
