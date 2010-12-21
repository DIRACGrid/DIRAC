
class DirectoryBrowser:

  def __init__( self, sort = False ):
    self.__toExplore = []
    self.__explored = set()
    self.__sort = sort

  def isActive( self ):
    return len( self.__toExplore ) > 0

  def getRemainingDirs( self ):
    return len( self.__toExplore )

  def getNextDir( self ):
    if self.__sort:
      sort( self.__toExplore )
    try:
      nextDir = self.__toExplore.pop( 0 )[1]
      while nextDir in self.__explored:
        nextDir = self.__toExplore.pop( 0 )[1]
    except IndexError:
      return False
    self.__explored.add( nextDir )
    return nextDir

  def addDir( self, dirName, weight = 0 ):
    if dirName not in self.__explored:
      self.__toExplore.append( ( weight, dirName ) )

  def addDirList( self, dirList, weight = 0 ):
    for d in dirList:
      self.addDir( d, weight )
