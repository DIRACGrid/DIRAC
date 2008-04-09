import random,types

class NameSpaceBrowser:

  def __init__(self,baseDir):
    if type(baseDir) == types.ListType:
      self.activeDirs = baseDir
    else:
      self.activeDirs = [baseDir]
    self.activeDir = self.activeDirs[0]
    self.baseDir = baseDir

  def isActive(self):
    if self.activeDirs:
      return True
    else:
      return False

  def getBaseDir(self):
    return self.baseDir

  def getActiveDir(self):
    random.shuffle(self.activeDirs)
    self.activeDir = self.activeDirs[0]
    return self.activeDir

  def updateDirs(self,subDirs):
    self.activeDirs.extend(subDirs)
    self.activeDirs.remove(self.activeDir)
