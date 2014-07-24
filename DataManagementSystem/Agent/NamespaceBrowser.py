# ABANDONWARE ### ABANDONWARE ### ABANDONWARE ###  ABANDONWARE ### ABANDONWARE ### ABANDONWARE ###
# ABANDONWARE ### ABANDONWARE ### ABANDONWARE ###  ABANDONWARE ### ABANDONWARE ### ABANDONWARE ###
#
# import random,types
# from DIRAC.Core.Utilities.List import sortList
#
# class NamespaceBrowser:
#
#   def __init__(self,baseDir,sort=False):
#     if type(baseDir) == types.ListType:
#       self.activeDirs = baseDir
#     else:
#       self.activeDirs = [baseDir]
#     self.sort = False
#     if sort:
#       self.sort = True
#     self.activeDirs = sortList(self.activeDirs)
#     self.activeDir = self.activeDirs[0]
#     self.baseDir = baseDir
#
#   def isActive(self):
#     if self.activeDirs:
#       return True
#     else:
#       return False
#
#   def getNumberActiveDirs(self):
#     return len(self.activeDirs)
#
#   def getBaseDir(self):
#     return self.baseDir
#
#   def getActiveDir(self):
#     #random.shuffle(self.activeDirs)
#     if self.sort:
#       self.activeDirs = sortList(self.activeDirs)
#     self.activeDir = self.activeDirs[0]
#     return self.activeDir
#
#   def updateDirs(self,subDirs):
#     self.activeDirs.extend(subDirs)
#     self.activeDirs.remove(self.activeDir)
