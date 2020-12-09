from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
__RCSID__ = "$Id$"


class DirectoryExplorer(object):

  def __init__(self, sort=False, reverse=False):
    self.__toExplore = []
    self.__explored = set()
    self.__sort = sort
    self.__reverse = reverse

  def isActive(self):
    return len(self.__toExplore) > 0

  def getNumRemainingDirs(self):
    return len(self.__toExplore)

  def __popNextDir(self):
    if self.__reverse:
      return self.__toExplore.pop()[1]
    else:
      return self.__toExplore.pop(0)[1]

  def getNextDir(self):
    if self.__sort:
      self.__toExplore = sorted(self.__toExplore)
    try:
      nextDir = self.__popNextDir()
      while nextDir in self.__explored:
        nextDir = self.__popNextDir()
    except IndexError:
      return False
    self.__explored.add(nextDir)
    return nextDir

  def addDir(self, dirName, weight=None):
    if weight is None and self.__sort:
      weight = dirName.count("/")
    if dirName not in self.__explored:
      self.__toExplore.append((weight, dirName))

  def addDirList(self, dirList, weight=None):
    for dirName in dirList:
      self.addDir(dirName, weight)
