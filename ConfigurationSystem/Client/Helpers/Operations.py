import threading
import types
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities import CFG, LockRing
from DIRAC.ConfigurationSystem.Client.Helpers import Registry, CSGlobals
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData

class Operations( object ):

  __cache = {}
  __cacheVersion = 0
  __cacheLock = LockRing.LockRing().getLock( "CSOperations.cache" )

  def __init__( self, vo = False, group = False, setup = False ):
    self.__threadData = threading.local()
    self.__threadData.uVO = vo
    self.__threadData.uGroup = group
    self.__threadData.uSetup = setup
    self.__threadData.vo = False
    self.__threadData.setup = False
    self.__discoverSettings()

  def __discoverSettings( self ):
    #Set the VO
    self.__threadData.vo = False
    if self.__threadData.uVO:
      self.__threadData.vo = self.__threadData.uVO
    else:
      self.__threadData.vo = Registry.getVOForGroup( self.__threadData.uGroup )
      if not self.__threadData.vo:
        raise RuntimeError( "Don't know how to discover VO. Please check your VO and groups configuration" )
    #Set the setup
    self.__threadData.setup = False
    if self.__threadData.uSetup:
      self.__threadData.setup = self.__threadData.uSetup
    else:
      self.__threadData.setup = CSGlobals.getSetup()

  def __getCache( self ):
    Operations.__cacheLock.acquire()
    try:
      currentVersion = gConfigurationData.getVersion()
      if currentVersion != Operations.__cacheVersion:
        Operations.__cache = {}
        Operations.__cacheVersion = currentVersion

      cacheKey = ( self.__threadData.vo, self.__threadData.setup )
      if cacheKey in Operations.__cache:
        return Operations.__cache[ cacheKey ]

      mergedCFG = CFG.CFG()

      for path in ( self.__getDefaultPath(), self.__getSetupPath() ):
        pathCFG = gConfigurationData.mergedCFG[ path ]
        if pathCFG:
          mergedCFG = mergedCFG.mergeWith( pathCFG )

      Operations.__cache[ cacheKey ] = mergedCFG

      return Operations.__cache[ cacheKey ]
    finally:
      Operations.__cacheLock.release()

  def setVO( self, vo ):
    """ False to auto detect VO
    """
    self.__threadData.uVO = vo
    self.__discoverSettings()

  def setGroup( self, group ):
    """ False to auto detect VO
    """
    self.__threadData.uGroup = group
    self.__discoverSettings()

  def setSetup( self, setup ):
    """ False to auto detect
    """
    self.__threadData.uSetup = setup
    self.__discoverSettings()


  def __getVOPath( self ):
    if CSGlobals.getVO():
      return "/Operations"
    return "/Operations/%s" % self.__threadData.vo

  def __getDefaultPath( self ):
    return "%s/Defaults/" % self.__getVOPath()

  def __getSetupPath( self ):
    return "%s/%s" % ( self.__getVOPath(), self.__threadData.setup )


  def getValue( self, optionPath, defaultValue = None ):
    return self.__getCache().getOption( optionPath, defaultValue )

  def __getCFG( self, sectionPath ):
    cacheCFG = self.__getCache()
    section = cacheCFG.getRecursive( sectionPath )
    if not section:
      return S_ERROR( "%s in Operations does not exist" % sectionPath )
    sectionCFG = section[ 'value' ]
    if type( sectionCFG ) in ( types.StringType, types.UnicodeType ):
      return S_ERROR( "%s in Operations is not a section" % sectionPath )
    return S_OK( sectionCFG )

  def getSections( self, sectionPath, listOrdered = False ):
    result = self.__getCFG( sectionPath )
    if not result[ 'OK' ]:
      return result
    sectionCFG = result[ 'Value' ]
    return S_OK( sectionCFG.listSections( listOrdered ) )

  def getOptions( self, sectionPath, listOrdered = False ):
    result = self.__getCFG( sectionPath )
    if not result[ 'OK' ]:
      return result
    sectionCFG = result[ 'Value' ]
    return S_OK( sectionCFG.listOptions( listOrdered ) )

  def getOptionsDict( self, sectionPath ):
    result = self.__getCFG( sectionPath )
    if not result[ 'OK' ]:
      return result
    sectionCFG = result[ 'Value' ]
    data = {}
    for opName in sectionCFG.listOptions():
      data[ opName ] = sectionCFG[ opName ]
    return S_OK( data )
