import random

class NameSpaceBrowser:

  def __init__(self,baseDir):
    self.activeDirs = [baseDir]
    self.activeDir = baseDir
    self.baseDir = baseDir

  def getBaseDir(self):
    return self.baseDir

  def getActiveDir(self):
    return self.activeDir

  def updateDirs(self,subDirs):
    self.activeDirs.extend(subDirs)
    self.activeDirs.remove(self.activeDir)
    if len(self.activeDirs) > 0:
      random.shuffle(self.activeDirs)
    else:
      self.activeDirs = [self.baseDir]
    self.activeDir = self.activeDirs[0]
