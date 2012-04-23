import threading, thread
import types
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities import CFG, LockRing
from DIRAC.ConfigurationSystem.Client.Helpers import Registry, CSGlobals
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData

class Operations( object ):

  __cache = {}
  __cacheVersion = 0
  __cacheLock = LockRing.LockRing().getLock()

  def __init__( self, vo = False, group = False, setup = False ):
    self.__uVO = vo
    self.__uGroup = group
    self.__uSetup = setup
    self.__vo = False
    self.__setup = False
    self.__discoverSettings()

  def __discoverSettings( self ):
    #Set the VO
    self.__vo = False
    if self.__uVO:
      self.__vo = self.__uVO
    else:
      self.__vo = Registry.getVOForGroup( self.__uGroup )
      if not self.__vo:
        self.__vo = False
    #Set the setup
    self.__setup = False
    if self.__uSetup:
      self.__setup = self.__uSetup
    else:
      self.__setup = CSGlobals.getSetup()

  def __getCache( self ):
    Operations.__cacheLock.acquire()
    try:
      currentVersion = gConfigurationData.getVersion()
      if currentVersion != Operations.__cacheVersion:
        Operations.__cache = {}
        Operations.__cacheVersion = currentVersion

      cacheKey = ( self.__vo, self.__setup )
      if cacheKey in Operations.__cache:
        return Operations.__cache[ cacheKey ]

      mergedCFG = CFG.CFG()

      #Only for the /Operations transition
      paths = [ "/Operations" ]
      paths.append( "/Operations/%s" % self.__setup )
      if self.__vo:
        paths.append( "/Operations/%s" % self.__vo )
        paths.append( "/Operations/%s/%s" % ( self.__vo, self.__setup ) )

      for path in paths:
        pathCFG = gConfigurationData.mergedCFG[ path ]
        if pathCFG:
          mergedCFG = mergedCFG.mergeWith( pathCFG )
      #End of migration

      for path in ( self.__getDefaultPath(), self.__getSetupPath() ):
        pathCFG = gConfigurationData.mergedCFG[ path ]
        if pathCFG:
          mergedCFG = mergedCFG.mergeWith( pathCFG )

      Operations.__cache[ cacheKey ] = mergedCFG

      return Operations.__cache[ cacheKey ]
    finally:
      try:
        Operations.__cacheLock.release()
      except thread.error:
        pass

  def setVO( self, vo ):
    """ False to auto detect VO
    """
    self.__uVO = vo
    self.__discoverSettings()

  def setGroup( self, group ):
    """ False to auto detect VO
    """
    self.__uGroup = group
    self.__discoverSettings()

  def setSetup( self, setup ):
    """ False to auto detect
    """
    self.__uSetup = setup
    self.__discoverSettings()


  def __getVOPath( self ):
    if CSGlobals.getVO() or not self.__vo:
      return "/Operations"
    return "/Operations/%s" % self.__vo

  def __getDefaultPath( self ):
    return "%s/Defaults/" % self.__getVOPath()

  def __getSetupPath( self ):
    return "%s/%s" % ( self.__getVOPath(), self.__setup )


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
